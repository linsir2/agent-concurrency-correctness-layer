"""CheckpointStore + EffectJournal —— 文档 §4.2 / §4.4 / §5.8。

v1.1（S5 第一波，§3/§4）：
- `EffectJournal` 事件**落盘到同一个 `EventLog`**（单一数据源，§3.1）：
  `begin` 写 `EFFECT_INTENT`、`confirm` 写 `EFFECT_CONFIRMED`、
  `mark_unknown` 写 `EFFECT_UNKNOWN`、`probe_one` 写 `EFFECT_PROBED`（三态，只写一次）；
- 事件 `subject` 统一为 `acb.id`，`idem_key` 入 payload（修 P0-4）；
- `pending(acb_id)` 返回 `INTENT + UNKNOWN`（修 P0-1）；
- `rebuild_from(events)` 从传入快照重建（调用方按 `producer` 过滤，§4.3）；
- `CheckpointStore.save` 增 `reservations` / `budget_path` / `event_cursor` /
  `failure` / `spec` / `deadline` / `no_progress_steps`。
"""

from __future__ import annotations

import copy
import hashlib
import itertools
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .event_log import EventLog
from .storage.atomic import atomic_write, fsync_dir, write_all
from .storage.codec import CodecError, decode, dumps, encode, loads
from .types import Action, Event, EventKind


_FORMAT_VERSION = 1
_SCHEMA_VERSION = 2

_HEADER_FIELDS = (
    "format_version", "schema_version", "ref", "acb_id", "run_id", "task_id",
    "generation", "save_seq", "created_at_ms", "event_cursor",
)

_REF_RE = re.compile(r"^CP-[0-9a-f]{8}-\d{20}-[0-9a-f]{12}$")
_REF_FILE_RE = re.compile(
    r"^CP-[0-9a-f]{8}-(\d{20})-[0-9a-f]{12}\.json(?:\.corrupt)?$")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha16(text: str) -> str:
    """指针文件名 hash：`sha256[:16]`（64bit）。

    P2：原 `[:8]`（32bit）在 ~7 万 ACB 后碰撞概率 ~50%，两个 id 可能落到同一指针
    文件；加长到 16 hex 使碰撞概率可忽略。
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class CheckpointCorrupt(Exception):
    """checkpoint 损坏（无法通过双 checksum 校验）。

    `header` 保留信封头部字段：即使 payload 坏掉，`recover` 仍可用它
    构造 `QUARANTINED` + `needs_readmit` 的 ACB。
    """

    def __init__(self, ref: str = "", header: dict | None = None,
                 reason: str = "") -> None:
        self.ref = ref
        self.header = dict(header or {})
        self.reason = reason
        super().__init__(f"checkpoint corrupt: {ref} ({reason})")


class _FileLock:
    """跨进程 `flock`；同线程可重入（`flock` 以 open file description 计）。"""

    def __init__(self, path: str | Path) -> None:
        self._path = os.fspath(path)
        self._local = threading.local()

    @contextmanager
    def __call__(self):
        depth = getattr(self._local, "depth", 0)
        if depth:
            self._local.depth = depth + 1
            try:
                yield
            finally:
                self._local.depth -= 1
            return

        parent = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(parent, exist_ok=True)
        try:
            import fcntl
        except ImportError:  # pragma: no cover - 非 POSIX 平台降级为仅线程互斥
            fcntl = None

        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_EX)
        self._local.depth = 1
        try:
            yield
        finally:
            self._local.depth = 0
            try:
                if fcntl is not None:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


@dataclass
class EffectEntry:
    """外部副作用条目（三态必处理，§3.3）。"""

    idem_key: str
    effect_id: str
    tool: str
    args: dict
    state: str = "INTENT"        # INTENT | CONFIRMED | UNKNOWN
    probe: str = ""              # "" | "True" | "False" | "None"
    updated_at: float = 0.0


def effect_idem_key(acb, action) -> str:
    """稳定幂等键：`sha256(run_id|logical_subgoal_id|tool|args)[:16]`（不含 task_id/generation）。"""
    payload = f"{acb.run_id}|{acb.logical_subgoal_id}|{action.tool}|{action.args}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class CheckpointStore:
    """内存版 checkpoint（默认实现，行为保持不变）。"""

    def __init__(self, root=None) -> None:
        self.root = root             # 兼容签名；内存版不使用
        self._store: dict[str, dict] = {}
        self._counter = itertools.count(1)
        self.slots = None            # 由 Harness 注入，用于快照活跃槽位
        self.event_log = None        # 必存属性（Harness 无条件注入）
        self.applied_event_ids = None  # 由 Harness 注入，用于恢复去重持久化

    def save(self, acb) -> str:
        ref = f"CP{next(self._counter)}"
        self._store[ref] = self._build_payload(acb)
        acb.result_ref = ref
        return ref

    def load(self, ref: str) -> dict:
        return copy.deepcopy(self._store[ref])

    def load_strict(self, ref: str) -> dict:
        """内存版无磁盘校验；与 `load` 等价（`recover` 统一调用入口）。"""
        return self.load(ref)

    def latest_ref_for_acb(self, acb_id: str) -> str | None:
        return None

    def latest_ref_for_run(self, run_id: str) -> str | None:
        return None

    def latest_ref_for_task(self, run_id: str, task_id: str,
                            logical_subgoal_id: str) -> str | None:
        return None

    def _build_payload(self, acb) -> dict:
        """构造扁平 payload（内存 / 文件两版共用）。"""
        node = acb.budget_node
        return {
            "acb_id": acb.id,
            "run_id": acb.run_id,
            "task_id": acb.task_id,
            "logical_subgoal_id": acb.logical_subgoal_id,
            "generation": acb.generation,
            "state": acb.state.value,
            "turn_seq": acb.turn_seq,
            "staged": copy.deepcopy(acb.staged),
            "read_set": dict(acb.read_set),
            "base_version": dict(acb.base_version),
            "cursor": acb.cursor,
            "materialized_cursor": acb.cursor,       # §2.4：per-ACB 物化游标
            # P1-8：recover 必须恢复 subscriptions / materialized（及其水位），
            # 否则恢复后 context_view 为空（事件全被订阅过滤掉）。
            "subscriptions": sorted(acb.subscriptions),
            "materialized": copy.deepcopy(acb.materialized),
            "materialized_watermark": int(getattr(acb, "materialized_watermark", 0) or 0),
            "pre_image": copy.deepcopy(acb.pre_image),   # §2.4：补偿 PIVOT 判定依赖
            "last_committed_seq": int(getattr(acb, "last_committed_seq", 0) or 0),
            "aging_deadline": acb.aging_deadline,
            "effect_journal_offset": acb.step,
            "hitl_result": copy.deepcopy(acb.hitl_result),
            "rebase_attempts": acb.rebase_attempts,
            "retry_budget": acb.retry_budget,
            "reservation_ids": list(acb.reservation_ids),   # legacy：新格式用 `reservations` 逐笔账本
            "needs_readmit": bool(getattr(acb, "needs_readmit", False)),
            "slots_active": (sorted(self.slots.active_keys())
                             if self.slots is not None else []),
            "budget_node_id": node.node_id if node is not None else None,
            "budget_parent_id": (node.parent.node_id
                                 if node is not None and node.parent is not None
                                 else None),
            "budget_limits": node.limits if node is not None else None,
            "budget_self": node.self_reserved if node is not None else 0,
            "budget_subtree": node.subtree_reserved if node is not None else 0,
            "budget_consumed": node.consumed if node is not None else 0,
            "budget_folded_consumed": (node.folded_consumed
                                       if node is not None else 0),
            # ---- S5 第一波新增（§5.1/§5.2/§4.3）----
            "reservations": self._reservations_snapshot(node),
            "budget_path": self._budget_path(node),
            "budget_path_limits": self._budget_path_limits(node),
            "event_cursor": self._event_cursor(acb),
            "applied_event_ids": (sorted(self.applied_event_ids)
                                  if self.applied_event_ids is not None else []),
            "failure": acb.failure,
            "spec": copy.deepcopy(acb.spec),
            "deadline": acb.deadline,
            "no_progress_steps": acb.no_progress_steps,
        }

    # ---- S5 快照辅助 ------------------------------------------------
    @staticmethod
    def _reservations_snapshot(node) -> list[dict]:
        """逐笔账本（§5.1）：只以逐笔为准，聚合快照仅作交叉校验。"""
        if node is None:
            return []
        return [
            {"rid": rid, "node_id": r.node_id, "amount": r.amount,
             "consumed": r.consumed, "state": r.state}
            for rid, r in node.reservations.items()
        ]

    @staticmethod
    def _budget_path(node) -> list[str]:
        """`[root ... node]`（§5.2）：供 `_restore_path` 自顶向下重建。"""
        if node is None:
            return []
        path: list[str] = []
        cur = node
        while cur is not None:
            path.append(cur.node_id)
            cur = cur.parent
        path.reverse()
        return path

    @staticmethod
    def _budget_path_limits(node) -> dict[str, int]:
        """路径上每个节点的 limits（供任意深度恢复）。"""
        if node is None:
            return {}
        limits: dict[str, int] = {}
        cur = node
        while cur is not None:
            limits[cur.node_id] = cur.limits
            cur = cur.parent
        return limits

    def _event_cursor(self, acb) -> int:
        """已重放到的事件序号（§4.3）。未注入 `event_log` 时退回 `acb.cursor`。"""
        if self.event_log is not None:
            return self.event_log.tail()
        return int(getattr(acb, "cursor", 0) or 0)


class FileCheckpointStore(CheckpointStore):
    """文件版 checkpoint（S9 §2.4）：信封 + 双 checksum + 双层锁 + 唯一 ref。

    目录结构：`store.json` / `store.lock` / `refs/<xx>/<yy>/<ref>.json` /
    `latest/<sha8(acb_id)>.json` / `latest_run/<sha8>.json` /
    `latest_task/<sha8>.json`。

    **先盘后缓存**：ref 文件成功落盘后才填 `_store`；`load_strict` 绕过缓存。
    """

    def __init__(self, root: str | Path) -> None:
        super().__init__(root=root)
        self.root = Path(root)
        self._thread_lock = threading.RLock()
        self._flock = _FileLock(self.root / "store.lock")
        self._meta: dict = {}
        self._init_meta()

    # ---- 元数据 / 启动扫描 ------------------------------------------
    @property
    def _meta_path(self) -> Path:
        return self.root / "store.json"

    @property
    def store_uuid(self) -> str:
        return str(self._meta["store_uuid"])

    def _init_meta(self) -> None:
        with self._thread_lock, self._flock():
            max_seq = self._scan_max_seq()
            meta_path = self._meta_path
            if meta_path.exists():
                try:
                    meta = loads(meta_path.read_bytes().decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError("store.json 损坏（无法解析）") from exc
                if not isinstance(meta, dict) or "store_uuid" not in meta:
                    raise RuntimeError("store.json 缺少 store_uuid")
                orig_next_seq = int(meta.get("next_seq", 0) or 0)
                next_seq = max(orig_next_seq, max_seq + 1)
                meta["next_seq"] = next_seq
                added_format = "format_version" not in meta
                meta.setdefault("format_version", _FORMAT_VERSION)
                # P2：修正后的 next_seq **立即落盘**，否则 `save` 重读磁盘的 stale
                # 值 → 已用 seq 被复用（stale meta 场景下 ref 槽位串号）。
                if next_seq != orig_next_seq or added_format:
                    atomic_write(meta_path, dumps(meta).encode("utf-8"))
            else:
                if self._has_ref_files():
                    # refs 非空但 store.json 缺失：绝不生成新 uuid 覆盖旧 ref 空间
                    raise RuntimeError(
                        "checkpoint refs 存在但 store.json 缺失（拒绝静默重建）")
                self._ensure_dir(self.root)
                meta = {
                    "format_version": _FORMAT_VERSION,
                    "store_uuid": uuid.uuid4().hex,
                    "next_seq": max_seq + 1,
                }
                atomic_write(meta_path, dumps(meta).encode("utf-8"))
            self._meta = meta
            self._quarantine_bad_refs()

    def _scan_max_seq(self) -> int:
        """seq 从**文件名**取 max（内容坏不影响计数）；`*.part` 忽略。"""
        refs_root = self.root / "refs"
        if not refs_root.exists():
            return -1
        best = -1
        for path in refs_root.rglob("*"):
            if not path.is_file():
                continue
            name = path.name
            if name.endswith(".part"):
                continue
            match = _REF_FILE_RE.match(name)
            if match:
                best = max(best, int(match.group(1)))
        return best

    def _has_ref_files(self) -> bool:
        """refs 目录是否存在**真实** ref 文件。

        P3-E：`.tmp`（`_write_new` 中断残留）与 `.part` 都不是合法 ref，
        不得计入 —— 否则「只剩一个残留 tmp 但 store.json 缺失」会被误判为
        「refs 非空」而抛 RuntimeError（启动失败）。
        """
        refs_root = self.root / "refs"
        if not refs_root.exists():
            return False
        return any(path.is_file()
                   and not path.name.endswith(".part")
                   and not path.name.endswith(".tmp")
                   for path in refs_root.rglob("*"))

    def _quarantine_bad_refs(self) -> None:
        """坏 ref 文件改名 `.corrupt`（槽位仍被文件名占位，绝不复用）。"""
        refs_root = self.root / "refs"
        if not refs_root.exists():
            return
        for path in list(refs_root.rglob("*.json")):
            if path.name.endswith(".part"):
                continue
            try:
                doc = loads(path.read_bytes().decode("utf-8"))
                if not isinstance(doc, dict):
                    raise ValueError("ref 文件顶层不是 dict")
            except Exception:  # noqa: BLE001
                target = path.with_name(path.name + ".corrupt")
                os.replace(path, target)
                fsync_dir(path.parent)
        # P2：隔离残留的半写 tmp（`_write_new` 中断/SIGKILL）→ 改名 `.corrupt`，
        # 防止被误当合法 ref；`.tmp` 不进 ref 命名空间（`_scan_max_seq` 只认文件名）。
        for path in list(refs_root.rglob("*.tmp")):
            try:
                os.replace(path, path.with_name(path.name + ".corrupt"))
                fsync_dir(path.parent)
            except OSError:  # noqa: S110  # 隔离失败不阻断启动
                pass
        self._rebuild_latest_pointers()

    def _rebuild_latest_pointers(self) -> None:
        """P1-6：quarantine 后重建 latest 指针（扫描 refs 取匹配 header 的最大 seq）。

        **只更新已存在的指针文件**（不新建）：P2 阶段「latest 未写」的原子性语义
        —— ref 存在但指针未写时，latest 必须仍为 `None`，不得由扫描凭空补出指针。
        无合法 ref 的键**保留**原指针（可能悬空），以便 `recover(ref)` 仍能从指针
        文件取到 `acb_id/run_id/task_id/generation` 兜底（`_read_pointer` 对悬空
        指针返回 `None`，不会误报 latest）。
        """
        refs_root = self.root / "refs"
        if not refs_root.exists():
            return
        index: dict[str, tuple[int, str, dict]] = {}
        for path in refs_root.rglob("*.json"):
            if path.name.endswith(".part") or path.name.endswith(".corrupt"):
                continue
            try:
                doc = loads(path.read_bytes().decode("utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(doc, dict):
                continue
            ref = doc.get("ref")
            if not isinstance(ref, str) or not _REF_RE.match(ref):
                continue
            match = _REF_FILE_RE.match(path.name)
            seq = int(match.group(1)) if match else 0
            acb_id = doc.get("acb_id") or ""
            run_id = doc.get("run_id") or ""
            task_id = doc.get("task_id") or ""
            subgoal = doc.get("logical_subgoal_id") or ""
            for kind, key in (("latest", acb_id),
                              ("latest_run", run_id),
                              ("latest_task", f"{run_id}\x00{task_id}\x00{subgoal}")):
                if not key:
                    continue
                composite = f"{kind}:{key}"
                cur = index.get(composite)
                if cur is None or seq > cur[0]:
                    index[composite] = (seq, ref, doc)
        for kind in ("latest", "latest_run", "latest_task"):
            directory = self.root / kind
            if not directory.exists():
                continue
            for path in directory.glob("*.json"):
                try:
                    data = loads(path.read_bytes().decode("utf-8"))
                except Exception:  # noqa: BLE001
                    continue
                if not isinstance(data, dict):
                    continue
                run_id = data.get("run_id") or ""
                task_id = data.get("task_id") or ""
                subgoal = data.get("logical_subgoal_id") or ""
                if kind == "latest":
                    key = data.get("acb_id") or ""
                elif kind == "latest_run":
                    key = run_id
                else:
                    key = f"{run_id}\x00{task_id}\x00{subgoal}"
                if not key:
                    continue
                best = index.get(f"{kind}:{key}")
                if best is None:
                    continue                    # 无合法 ref → 保留原指针（供 recover 兜底）
                seq, ref, doc = best
                if data.get("ref") == ref:
                    continue                    # 已是最优
                pointer = {
                    "ref": ref,
                    "acb_id": doc.get("acb_id", ""),
                    "run_id": doc.get("run_id", ""),
                    "task_id": doc.get("task_id", ""),
                    "logical_subgoal_id": doc.get("logical_subgoal_id", ""),
                    "generation": int(doc.get("generation", 1) or 1),
                    "save_seq": int(doc.get("save_seq", 0) or 0),
                    "updated_at_ms": int(doc.get("created_at_ms", 0) or 0),
                }
                try:
                    atomic_write(path, dumps(pointer).encode("utf-8"))
                except Exception:  # noqa: BLE001
                    pass

    def _read_meta_locked(self) -> dict:
        try:
            meta = loads(self._meta_path.read_bytes().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("store.json 缺失或损坏") from exc
        if not isinstance(meta, dict) or "store_uuid" not in meta:
            raise RuntimeError("store.json 缺少 store_uuid")
        return meta

    # ---- 路径 -------------------------------------------------------
    def _ref_path(self, ref: str) -> Path:
        if not _REF_RE.match(ref):
            raise ValueError(f"非法 checkpoint ref：{ref!r}")
        return (self.root / "refs" / ref[:2] / ref[2:4] / f"{ref}.json")

    def _latest_path(self, kind: str, key: str) -> Path:
        return self.root / kind / f"{_sha16(key)}.json"

    def _task_pointer_path(self, acb) -> Path:
        key = f"{acb.run_id}\x00{acb.task_id}\x00{acb.logical_subgoal_id}"
        return self._latest_path("latest_task", key)

    @staticmethod
    def _ensure_dir(path: str | Path) -> None:
        """逐级创建目录，并对每个**新建**目录的父目录 fsync（掉电持久性）。"""
        target = Path(path)
        missing: list[Path] = []
        current = target
        while not current.exists():
            missing.append(current)
            if current.parent == current:
                break
            current = current.parent
        for directory in reversed(missing):
            try:
                directory.mkdir()
            except FileExistsError:  # pragma: no cover - 并发创建
                pass
            fsync_dir(directory.parent)

    @staticmethod
    def _write_new(path: Path, data: bytes) -> None:
        """同目录 `ref.json.tmp` + fsync → `os.link` 原子占位 → unlink tmp → fsync 目录。

        P2 原子性：绝不直接写最终路径（SIGKILL/短写会留下半截 `ref.json`）。
        `os.link(tmp, path)` 是原子操作且 `path` 已存在时抛 `FileExistsError`
        —— 原子且**不覆盖**；tmp 已 fsync，link 成功后 ref 必是完整内容。

        P3-E：`os.link` 抛**非** `FileExistsError`（ENOSPC / EIO / EXDEV）时，
        旧实现不清理 tmp → 残留 `.tmp` 会被 `_has_ref_files` 误判为「有 ref」。
        改为 `try/finally` 兜底 unlink（`FileExistsError` 也走同一兜底）。
        """
        tmp = path.with_name(path.name + ".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.link(tmp, path)
        finally:
            try:
                os.unlink(tmp)                             # P3-E：任何路径都兜底清理
            except FileNotFoundError:
                pass
        fsync_dir(path)

    def _write_pointer(self, path: Path, ref: str, acb, seq: int, now_ms: int) -> None:
        """单调 CAS 写指针：仅在「权威键」更大时替换（不回退）。

        P2：滞后写者（读到旧 head 后更晚落盘）不得把 latest 回退到旧 ref；
        `_rebuild_latest_pointers` 选最大 seq 也依赖此单调性。

        P3-F 权威键取舍（`seq` 与 `generation` 谁优先）：
        - `latest`（per ACB）：**`generation` 权威**，`save_seq` 仅 tie-break。
          generation 是 ACB 级、随每次 recover 递增，天然 fence 旧代际的滞后写者；
        - `latest_run` / `latest_task`：**`save_seq` 权威**，`generation` 仅 tie-break。
          generation 是 **ACB 级**、跨 ACB **非单调**：同一 run/task 内
          「seq 更大但 generation 更小」的合法保存（另一 ACB 更晚落盘）会被
          generation 优先的 CAS 错误挡住 → latest 指向陈旧 ref。save_seq 是
          store 级全局单调（`next_seq`），对 run/task 维度的「最新」才是正确权威。
        """
        kind = path.parent.name                       # latest / latest_run / latest_task
        key = self._pointer_key(kind, acb)
        with self._thread_lock, self._flock():       # 线程锁 + flock 内 CAS（原子）
            old = self._read_pointer_doc(path)
            if old is not None and self._pointer_matches(old, kind, key):
                old_gen = int(old.get("generation", 0) or 0)
                old_seq = int(old.get("save_seq", 0) or 0)
                new_gen = int(acb.generation)
                new_seq = int(seq)
                if kind == "latest":                 # generation 权威
                    old_key, new_key = (old_gen, old_seq), (new_gen, new_seq)
                else:                                # save_seq 权威
                    old_key, new_key = (old_seq, old_gen), (new_seq, new_gen)
                if old_key >= new_key:
                    return                               # 不回退
            pointer = {
                "ref": ref,
                "acb_id": acb.id,
                "run_id": acb.run_id,
                "task_id": acb.task_id,
                "logical_subgoal_id": acb.logical_subgoal_id,
                "generation": int(acb.generation),
                "save_seq": seq,
                "updated_at_ms": now_ms,
            }
            self._ensure_dir(path.parent)
            atomic_write(path, dumps(pointer).encode("utf-8"))

    @staticmethod
    def _pointer_key(kind: str, acb) -> str:
        if kind == "latest":
            return acb.id
        if kind == "latest_run":
            return acb.run_id
        return f"{acb.run_id}\x00{acb.task_id}\x00{acb.logical_subgoal_id}"

    @staticmethod
    def _pointer_matches(data: dict, kind: str, key: str) -> bool:
        """校验指针内容与键匹配（P2：防 hash 碰撞串号）。"""
        if kind == "latest":
            return data.get("acb_id") == key
        if kind == "latest_run":
            return data.get("run_id") == key
        run_id, task_id, subgoal = key.split("\x00")
        return (data.get("run_id") == run_id
                and data.get("task_id") == task_id
                and data.get("logical_subgoal_id") == subgoal)

    def _read_pointer_doc(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            data = loads(path.read_bytes().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return None
        return data if isinstance(data, dict) else None

    # ---- 写入 -------------------------------------------------------
    def save(self, acb) -> str:
        payload = self._build_payload(acb)
        with self._thread_lock, self._flock():
            meta = self._read_meta_locked()
            seq = int(meta.get("next_seq", 0) or 0)
            meta["next_seq"] = seq + 1
            atomic_write(self._meta_path, dumps(meta).encode("utf-8"))
            ref = f"CP-{str(meta['store_uuid'])[:8]}-{seq:020d}-{uuid.uuid4().hex[:12]}"
            now_ms = int(time.time() * 1000)
            header = {
                "format_version": _FORMAT_VERSION,
                "schema_version": _SCHEMA_VERSION,
                "ref": ref,
                "acb_id": acb.id,
                "run_id": acb.run_id,
                "task_id": acb.task_id,
                "generation": int(acb.generation),
                "save_seq": seq,
                "created_at_ms": now_ms,
                "event_cursor": int(payload.get("event_cursor") or 0),
            }
            encoded_payload = encode(payload)
            payload_bytes = dumps(encoded_payload).encode("utf-8")
            envelope = dict(header)
            envelope["header_checksum"] = _sha256(dumps(header).encode("utf-8"))
            envelope["payload_checksum"] = _sha256(payload_bytes)
            envelope["payload"] = encoded_payload

            ref_path = self._ref_path(ref)
            self._ensure_dir(ref_path.parent)
            self._write_new(ref_path, dumps(envelope).encode("utf-8"))

            self._write_pointer(self._latest_path("latest", acb.id),
                                ref, acb, seq, now_ms)
            self._write_pointer(self._latest_path("latest_run", acb.run_id),
                                ref, acb, seq, now_ms)
            self._write_pointer(self._task_pointer_path(acb),
                                ref, acb, seq, now_ms)

            # 先盘后缓存：全部落盘成功后才填只读镜像
            self._store[ref] = copy.deepcopy(payload)
            self._meta = meta
        acb.result_ref = ref
        return ref

    # ---- 读取 -------------------------------------------------------
    def load(self, ref: str) -> dict:
        with self._thread_lock:
            cached = self._store.get(ref)
            if cached is not None:
                return copy.deepcopy(cached)
        doc = self._read_document(ref)
        payload = self._payload_of(doc)
        return self._decode_payload(payload, ref, {})

    def load_strict(self, ref: str) -> dict:
        """绕过缓存，校验 header + payload 双 checksum。"""
        doc = self._read_document(ref)
        if not self._is_envelope(doc):
            return self._decode_payload(doc, ref, {})
        header = {name: doc.get(name) for name in _HEADER_FIELDS}
        try:
            expected_header = _sha256(dumps(header).encode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise CheckpointCorrupt(ref, header, "header_checksum") from exc
        if expected_header != doc.get("header_checksum"):
            raise CheckpointCorrupt(ref, header, "header_checksum")
        payload = doc.get("payload")
        try:
            actual = _sha256(dumps(payload).encode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise CheckpointCorrupt(ref, header, "payload_checksum") from exc
        if actual != doc.get("payload_checksum"):
            raise CheckpointCorrupt(ref, header, "payload_checksum")
        return self._decode_payload(payload, ref, header)

    def _read_document(self, ref: str) -> Any:
        """读 ref 文件；非法 ref → `CheckpointCorrupt(bad_ref)`；缺失 → 头部兜底。"""
        try:
            path = self._ref_path(ref)
        except ValueError as exc:
            raise CheckpointCorrupt(ref, {}, "bad_ref") from exc
        if not path.exists():
            raise CheckpointCorrupt(ref, self._header_from_pointers(ref), "missing")
        try:
            return loads(path.read_bytes().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise CheckpointCorrupt(ref, {}, "unreadable") from exc

    def _header_from_pointers(self, ref: str) -> dict:
        """P1-6：ref 缺失/不可读时，从 latest 指针文件兜底取头部元数据。"""
        for kind in ("latest", "latest_run", "latest_task"):
            directory = self.root / kind
            if not directory.exists():
                continue
            for path in directory.glob("*.json"):
                try:
                    data = loads(path.read_bytes().decode("utf-8"))
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(data, dict) and data.get("ref") == ref:
                    return {
                        "acb_id": data.get("acb_id", ""),
                        "run_id": data.get("run_id", ""),
                        "task_id": data.get("task_id", ""),
                        "logical_subgoal_id": data.get("logical_subgoal_id", ""),
                        "generation": data.get("generation", 1),
                    }
        return {}

    @staticmethod
    def _is_envelope(doc) -> bool:
        return (isinstance(doc, dict) and (
            ("header_checksum" in doc and "payload_checksum" in doc)
            or ("format_version" in doc and "payload" in doc)))

    @classmethod
    def _payload_of(cls, doc) -> Any:
        """信封 → `payload`；旧格式（扁平 payload）→ 原样返回（兼容路径）。"""
        return doc["payload"] if cls._is_envelope(doc) else doc

    @staticmethod
    def _decode_payload(payload: Any, ref: str, header: dict) -> dict:
        try:
            return copy.deepcopy(decode(payload))
        except (CodecError, ValueError, TypeError) as exc:
            raise CheckpointCorrupt(ref, header, "payload_decode") from exc

    # ---- latest 指针 ------------------------------------------------
    def _read_pointer(self, path: Path, kind: str, key: str) -> str | None:
        """读 latest 指针并校验：键不匹配（hash 碰撞）/ 非法 ref / 悬空 → `None`。

        P2：校验 `acb_id`（task 指针校验三元组），避免两个 id 撞同一指针文件时
        `latest_ref_for_acb(A)` 误返回 B 的 ref（P1-6 悬空语义）。
        """
        with self._thread_lock:
            data = self._read_pointer_doc(path)
            if data is None:
                return None
            if not self._pointer_matches(data, kind, key):
                return None
            ref = data.get("ref")
            if not isinstance(ref, str) or not _REF_RE.match(ref):
                return None
            try:
                exists = self._ref_path(ref).exists()
            except ValueError:
                return None
            return ref if exists else None

    def latest_ref_for_acb(self, acb_id: str) -> str | None:
        return self._read_pointer(self._latest_path("latest", acb_id),
                                  "latest", acb_id)

    def latest_ref_for_run(self, run_id: str) -> str | None:
        return self._read_pointer(self._latest_path("latest_run", run_id),
                                  "latest_run", run_id)

    def latest_ref_for_task(self, run_id: str, task_id: str,
                            logical_subgoal_id: str) -> str | None:
        key = f"{run_id}\x00{task_id}\x00{logical_subgoal_id}"
        return self._read_pointer(self._latest_path("latest_task", key),
                                  "latest_task", key)


class EffectJournal:
    """外部副作用账本。`idem_key` 是幂等键（I7）。

    事件写**同一个** `EventLog`（§3.1 单一数据源）；恢复走 `rebuild_from(events)`。
    """

    def __init__(self, event_log: EventLog, probe=None) -> None:
        self._entries: dict[str, EffectEntry] = {}
        self._acb_of: dict[str, str] = {}
        self._run_of: dict[str, str] = {}
        self._probes: dict[str, str] = {}
        self._event_log = event_log
        self._probe = probe
        self._counter = itertools.count(1)
        # 单写者临界区：begin/confirm/mark_unknown/probe_one 必须原子
        self._lock = threading.RLock()

    # ---- 写入（§3.2：subject = acb.id，idem_key 入 payload）----------
    def begin(self, acb, action, *legacy) -> str:
        """幂等：已存在（含 CONFIRMED/UNKNOWN）则不覆盖（I7）。

        **兼容垫片**：旧签名 `begin(acb, tool, args, key)` 仍可用
        （新签名 `begin(acb, action)` 是生产路径）。
        """
        if isinstance(action, str):
            action = Action(kind="tool", tool=action,
                            args=(legacy[0] if legacy else {}) or {})
        with self._lock:
            idem_key = effect_idem_key(acb, action)
            existing = self._entries.get(idem_key)
            if existing:
                return existing.effect_id
            eid = f"E{next(self._counter)}"
            args = dict(action.args) if action.args is not None else {}
            self._entries[idem_key] = EffectEntry(
                idem_key=idem_key, effect_id=eid, tool=action.tool,
                args=args, state="INTENT")
            self._acb_of[idem_key] = acb.id
            self._run_of[idem_key] = acb.run_id
            self._event_log.append(
                EventKind.EFFECT_INTENT, acb.id,
                {"idem_key": idem_key, "tool": action.tool, "args": action.args,
                 "effect_id": eid},
                producer=acb.id, idempotency_key=idem_key, run_id=acb.run_id)
            return eid

    def confirm(self, idem_key: str, result: Any = None) -> bool:
        with self._lock:
            entry = self._entries.get(idem_key)
            if not entry:
                return False
            entry.state = "CONFIRMED"
            entry.updated_at = time.time()
            acb_id = self._acb_of.get(idem_key, "")
            self._event_log.append(
                EventKind.EFFECT_CONFIRMED, acb_id,
                {"idem_key": idem_key, "effect_id": entry.effect_id,
                 "tool": entry.tool, "result": result},
                producer=acb_id, idempotency_key=idem_key,
                run_id=self._run_of.get(idem_key, ""))
            return True

    def mark_unknown(self, idem_key: str, reason: str = "") -> bool:
        with self._lock:
            entry = self._entries.get(idem_key)
            if not entry:
                return False
            entry.state = "UNKNOWN"
            entry.updated_at = time.time()
            acb_id = self._acb_of.get(idem_key, "")
            self._event_log.append(
                EventKind.EFFECT_UNKNOWN, acb_id,
                {"idem_key": idem_key, "tool": entry.tool, "reason": reason},
                producer=acb_id, idempotency_key=idem_key,
                run_id=self._run_of.get(idem_key, ""))
            return True

    # ---- 读 --------------------------------------------------------
    def pending(self, acb_id: str) -> list[EffectEntry]:
        """返回 `state ∈ {INTENT, UNKNOWN}` 的条目（含 INTENT，修 P0-1）。"""
        with self._lock:
            return [e for k, e in self._entries.items()
                    if self._acb_of.get(k) == acb_id
                    and e.state in ("INTENT", "UNKNOWN")]

    def state(self, idem_key: str) -> str | None:
        with self._lock:
            entry = self._entries.get(idem_key)
            return entry.state if entry else None

    def get(self, idem_key: str) -> EffectEntry | None:
        with self._lock:
            return self._entries.get(idem_key)

    # ---- probe（§3.5 / §4.1）--------------------------------------
    def probe_one(self, idem_key: str) -> str:
        """三态 probe：`"True"` / `"False"` / `"None"`。

        - double-checked locking：先无锁查缓存，加锁后复查，再调外部 probe
          （保证同一 idem_key 的外部探针只调用一次、`EFFECT_PROBED` 只写一次）；
        - 内部 `try/except BaseException → "None"`（异常不得冒泡终止 run，修 P1-2）；
        - **`CrashInjected` 例外：必须穿透**（S9 §2.1 / K-25）—— 否则注入的
          崩溃被降级成 `"None"`，故障注入测试全部假绿；
        - 未配置 probe → `"None"`（fail-closed，Q2）。
        """
        # 局部 import：`runtime` 依赖本模块所在包的其他组件，局部导入可确保
        # 模块加载顺序无关、且不会有 checkpoint ↔ runtime 的循环导入。
        from .runtime import CrashInjected
        cached = self._probes.get(idem_key)
        if cached is not None:
            return cached
        with self._lock:
            cached = self._probes.get(idem_key)
            if cached is not None:
                return cached
            if self._probe is None:
                probe = "None"
            else:
                try:
                    result = self._probe(idem_key)
                except CrashInjected:                 # S9 §2.1：注入崩溃必须穿透
                    raise
                except BaseException:  # noqa: BLE001  # 契约：probe 异常一律降级为 "None"
                    result = None
                probe = "None" if result is None else ("True" if result else "False")
            self._probes[idem_key] = probe
            entry = self._entries.get(idem_key)
            if entry is not None:
                entry.probe = probe
            acb_id = self._acb_of.get(idem_key, "")
            self._event_log.append(
                EventKind.EFFECT_PROBED, acb_id,
                {"idem_key": idem_key, "probe": probe},
                producer=acb_id, idempotency_key=idem_key,
                run_id=self._run_of.get(idem_key, ""))
            return probe

    # ---- 重建（§4.3）----------------------------------------------
    def rebuild_from(self, events: list[Event]) -> int:
        """从传入快照重建（调用方已按 `producer` 过滤）。返回条目数。"""
        count = 0
        with self._lock:
            for ev in events:
                payload = ev.payload or {}
                if ev.kind == EventKind.EFFECT_INTENT:
                    idem_key = payload.get("idem_key", "")
                    if not idem_key:
                        continue
                    self._entries[idem_key] = EffectEntry(
                        idem_key=idem_key,
                        effect_id=payload.get("effect_id", ""),
                        tool=payload.get("tool", ""),
                        args=payload.get("args") or {},
                        state="INTENT",
                        updated_at=ev.ts / 1000.0)
                    self._acb_of[idem_key] = ev.subject
                    self._run_of[idem_key] = ev.run_id
                    count += 1
                elif ev.kind == EventKind.EFFECT_CONFIRMED:
                    idem_key = payload.get("idem_key", "")
                    entry = self._entries.get(idem_key)
                    if entry is not None:
                        entry.state = "CONFIRMED"
                        if payload.get("effect_id"):
                            entry.effect_id = payload["effect_id"]
                        entry.updated_at = ev.ts / 1000.0
                        self._acb_of.setdefault(idem_key, ev.subject)
                elif ev.kind == EventKind.EFFECT_UNKNOWN:
                    idem_key = payload.get("idem_key", "")
                    entry = self._entries.get(idem_key)
                    if entry is not None:
                        entry.state = "UNKNOWN"
                        entry.updated_at = ev.ts / 1000.0
                        self._acb_of.setdefault(idem_key, ev.subject)
                elif ev.kind == EventKind.EFFECT_PROBED:
                    idem_key = payload.get("idem_key", "")
                    probe = payload.get("probe", "")
                    self._probes[idem_key] = probe
                    entry = self._entries.get(idem_key)
                    if entry is not None:
                        entry.probe = probe
        return count
