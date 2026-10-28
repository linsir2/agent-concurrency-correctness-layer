"""S22：Z3 私有工作区（per-agent worktree）生命周期。

对应设计规范 §3：**Z3 = per-agent worktree（agent 直接写本地文件，无锁）**，
Z2 = 提交层仲裁的共享态。本模块只负责 Z3 的**创建 / 读回 / 销毁**，
不碰宿主的 cwd、不 monkeypatch `open`、不回写用户仓库。

两种后端（`mode="auto"` 自动选）：

- **git**：`git worktree add --detach <path> <ref>`（共享 .git，最省空间）；
- **copy**：`shutil.copytree`（非 git 目录兜底；用 mtime+size 检测改动）。

**安全**：所有相对路径都经 `realpath` 校验必须落在 worktree 内（防 `../` 逃逸）。
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import threading
from dataclasses import dataclass

# 小文件直接算内容哈希（同尺寸+同 mtime 的改写也能检出）；大文件退化为
# (mtime, ctime, size) —— ctime 在写入时必变，足以覆盖 `os.utime` 恢复 mtime 的情况。
_HASH_LIMIT = 256 * 1024


class WorktreeError(RuntimeError):
    """worktree 创建/销毁/读取失败（fail-closed，不静默降级）。"""


@dataclass(frozen=True)
class _CopyState:
    # rel -> (mtime_ns, ctime_ns, size, sha256|None)
    files: dict[str, tuple[int, int, int, str | None]]


class WorktreeManager:
    """每个 agent 一个独立目录；同 agent 重复 `create` 幂等。"""

    def __init__(self, base: str, root: str, *, ref: str = "HEAD",
                 mode: str = "auto") -> None:
        if mode not in ("auto", "git", "copy"):
            raise ValueError(f"未知 mode：{mode!r}（auto|git|copy）")
        self.base = os.path.abspath(base)
        self.root = os.path.abspath(root)
        self.ref = ref
        self.mode = self._detect(mode)
        os.makedirs(self.root, exist_ok=True)
        self._paths: dict[str, str] = {}
        self._copy_state: dict[str, _CopyState] = {}
        # 同 agent 并发 create/destroy 会互删目录 → 用一把进程内锁串行化。
        self._lock = threading.RLock()

    # ---- 选择后端 -------------------------------------------------
    def _detect(self, mode: str) -> str:
        if mode == "git":
            return "git"
        if mode == "copy":
            return "copy"
        if (os.path.isdir(os.path.join(self.base, ".git"))
                and shutil.which("git") is not None):
            return "git"
        return "copy"

    # ---- 路径安全 -------------------------------------------------
    @staticmethod
    def _safe_name(agent_id: str) -> str:
        raw = str(agent_id or "")
        clean = "".join(c if (c.isalnum() or c in "._-") else "_" for c in raw)
        if clean != raw or not clean:
            clean = f"{clean or 'agent'}-{hashlib.sha1(raw.encode()).hexdigest()[:8]}"
        return clean

    def path_for(self, agent_id: str) -> str:
        return os.path.join(self.root, self._safe_name(agent_id))

    def _checked(self, path: str, rel: str) -> str:
        """`rel` 必须在 `path` 之内（防目录逃逸）。"""
        base = os.path.realpath(path)
        target = os.path.realpath(os.path.join(base, rel))
        if target != base and not target.startswith(base + os.sep):
            raise ValueError(f"路径逃逸：{rel!r} 不在 worktree 内")
        return target

    # ---- 生命周期 -------------------------------------------------
    def create(self, agent_id: str) -> str:
        """创建（或复用）该 agent 的 worktree，返回绝对路径。"""
        with self._lock:
            path = self.path_for(agent_id)
            if self._paths.get(agent_id) == path and os.path.isdir(path):
                return path
            if os.path.exists(path):
                self._remove_dir(path)
            if self.mode == "git":
                try:
                    self._git(["worktree", "add", "--detach", path, self.ref])
                except WorktreeError:
                    # 空仓库/无 HEAD/ref 不存在 → 退回 copy（不静默：仍可工作）
                    self.mode = "copy"
            if self.mode == "copy":
                try:
                    shutil.copytree(self.base, path, symlinks=True,
                                    ignore=shutil.ignore_patterns(".git"))
                except OSError as exc:
                    raise WorktreeError(
                        f"复制 worktree 失败（base={self.base!r}）：{exc}") from exc
                self._copy_state[agent_id] = self._snapshot(path)
            self._paths[agent_id] = path
            return path

    def destroy(self, path: str) -> None:
        """销毁 worktree（幂等）。git 模式先 `worktree remove`，失败再删目录。"""
        if not path:
            return
        with self._lock:
            if self.mode == "git":
                try:
                    self._git(["worktree", "remove", "--force", path])
                except WorktreeError:
                    pass
            self._remove_dir(path)
            if self.mode == "git":
                # `remove` 失败后 rmtree 会留下 prunable 登记 → 清掉，避免下次
                # 同路径 create 被降级为 copy。
                try:
                    self._git(["worktree", "prune"])
                except WorktreeError:
                    pass
            for aid, p in list(self._paths.items()):
                if p == path:
                    self._paths.pop(aid, None)
                    self._copy_state.pop(aid, None)

    def destroy_all(self) -> None:
        for path in list(self._paths.values()):
            self.destroy(path)

    @staticmethod
    def _remove_dir(path: str) -> None:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)

    # ---- 读回 -----------------------------------------------------
    def changed(self, path: str) -> list[str]:
        """返回相对路径列表（新增/修改/删除）。"""
        if self.mode == "git":
            return self._changed_git(path)
        return self._changed_copy(path)

    def read(self, path: str, rel: str) -> bytes | None:
        """读 worktree 内的文件；不存在返回 None（调用方据此判 DELETE）。"""
        target = self._checked(path, rel)
        if not os.path.isfile(target):
            return None
        with open(target, "rb") as fh:
            return fh.read()

    # ---- git 后端 -------------------------------------------------
    def _git(self, args: list[str]) -> str:
        try:
            proc = subprocess.run(
                ["git", "-C", self.base, *args],
                capture_output=True, text=True, timeout=60, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            raise WorktreeError(f"git {' '.join(args)} 失败：{exc}") from exc
        if proc.returncode != 0:
            raise WorktreeError(
                f"git {' '.join(args)} 退出码 {proc.returncode}：{proc.stderr.strip()}")
        return proc.stdout

    def _changed_git(self, path: str) -> list[str]:
        try:
            proc = subprocess.run(
                ["git", "-C", path, "status", "--porcelain", "-z"],
                capture_output=True, timeout=60, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            raise WorktreeError(f"git status 失败：{exc}") from exc
        if proc.returncode != 0:
            raise WorktreeError(
                f"git status 退出码 {proc.returncode}：{proc.stderr.decode(errors='replace')}")
        parts = [p for p in proc.stdout.decode("utf-8", errors="replace").split("\0") if p]
        out: list[str] = []
        i = 0
        while i < len(parts):
            entry = parts[i]
            status, _, rel = entry[:2], entry[2:3], entry[3:]
            if status[:1] in ("R", "C") and i + 1 < len(parts):
                rel = parts[i + 1]          # 重命名：下一段是新路径
                i += 1
            if rel:
                out.append(rel)
            i += 1
        return sorted(set(out))

    # ---- copy 后端 ------------------------------------------------
    @staticmethod
    def _snapshot(path: str) -> _CopyState:
        files: dict[str, tuple[int, int, int, str | None]] = {}
        for dirpath, _dirs, names in os.walk(path):
            for name in names:
                full = os.path.join(dirpath, name)
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                digest = None
                if st.st_size <= _HASH_LIMIT:
                    try:
                        with open(full, "rb") as fh:
                            digest = hashlib.sha256(fh.read()).hexdigest()
                    except OSError:
                        digest = None
                rel = os.path.relpath(full, path)
                files[rel] = (st.st_mtime_ns, st.st_ctime_ns, st.st_size, digest)
        return _CopyState(files=files)

    def _changed_copy(self, path: str) -> list[str]:
        agent = next((a for a, p in self._paths.items() if p == path), None)
        state = self._copy_state.get(agent) if agent else None
        # git→copy 降级后可能没有基线 → 视为「全部可疑改动」（保守，不丢写）
        before = state.files if state is not None else {}
        after = self._snapshot(path).files
        changed = {r for r, sig in after.items() if before.get(r) != sig}
        changed |= {r for r in before if r not in after}       # 删除
        return sorted(changed)
