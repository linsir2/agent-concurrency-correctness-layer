"""S22 C6：Z3 私有工作区（`harness/worktree.py`）的 RIPR 用例。"""

from __future__ import annotations

import os
import shutil
import subprocess
import threading

import pytest

from harness.worktree import WorktreeError, WorktreeManager

needs_git = pytest.mark.skipif(
    shutil.which("git") is None, reason="git 不可用（worktree 后端不可验证）")


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args],
                   check=True, capture_output=True, text=True)


@pytest.fixture()
def repo(tmp_path):
    """最小 git 仓库：a.txt + keep.txt。"""
    base = tmp_path / "repo"
    base.mkdir()
    _git(base, "init", "-q")
    _git(base, "config", "user.email", "t@example.com")
    _git(base, "config", "user.name", "t")
    (base / "a.txt").write_text("hello", encoding="utf-8")
    (base / "keep.txt").write_text("keep", encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


@needs_git
def test_c6_isolation(repo, tmp_path):
    """R: 两个 agent 各建 worktree 改同一文件；I: 共用目录；P: 两边内容 + base。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    p1 = mgr.create("agent-1")
    p2 = mgr.create("agent-2")
    assert p1 != p2

    with open(os.path.join(p1, "a.txt"), "w", encoding="utf-8") as fh:
        fh.write("from-1")
    with open(os.path.join(p2, "a.txt"), "w", encoding="utf-8") as fh:
        fh.write("from-2")

    assert mgr.read(p1, "a.txt") == b"from-1"
    assert mgr.read(p2, "a.txt") == b"from-2"
    assert (repo / "a.txt").read_text(encoding="utf-8") == "hello", (
        "base 仓库不得被 agent 的私有编辑污染")
    mgr.destroy_all()


@needs_git
def test_c6_changed_and_read(repo, tmp_path):
    """R: 改一个、删一个、加一个；I: 漏检/误检；P: changed 集合 + read 返回值。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    p = mgr.create("a")
    with open(os.path.join(p, "a.txt"), "w", encoding="utf-8") as fh:
        fh.write("changed")
    os.remove(os.path.join(p, "keep.txt"))
    with open(os.path.join(p, "new.txt"), "w", encoding="utf-8") as fh:
        fh.write("new")

    assert mgr.changed(p) == ["a.txt", "keep.txt", "new.txt"]
    assert mgr.read(p, "keep.txt") is None, "已删除文件读回 None（调用方判 DELETE）"
    assert mgr.read(p, "new.txt") == b"new"
    mgr.destroy_all()


@needs_git
def test_c6_path_escape_rejected(repo, tmp_path):
    """R: 用 ../ 逃逸；I: 未校验；P: 抛 ValueError 而非读到外部文件。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    p = mgr.create("a")
    with pytest.raises(ValueError):
        mgr.read(p, "../../etc/passwd")
    mgr.destroy_all()


@needs_git
def test_c6_cleanup(repo, tmp_path):
    """R: destroy 两次；I: 目录残留/二次抛错；P: 目录存在性。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    p = mgr.create("a")
    assert os.path.isdir(p)
    mgr.destroy(p)
    assert not os.path.exists(p)
    mgr.destroy(p)                              # 幂等：不抛
    # git 的 worktree 登记也应清理（list 里不应再有该路径）
    out = subprocess.run(["git", "-C", str(repo), "worktree", "list"],
                         capture_output=True, text=True, check=True).stdout
    assert p not in out


def test_c6_copy_fallback(tmp_path):
    """R: 非 git 目录；I: 强制 git 模式直接失败；P: copy 模式仍能 create/changed/read。"""
    base = tmp_path / "plain"
    base.mkdir()
    (base / "a.txt").write_text("hello", encoding="utf-8")

    mgr = WorktreeManager(str(base), str(tmp_path / "wt"), mode="copy")
    p = mgr.create("a")
    assert mgr.read(p, "a.txt") == b"hello"

    with open(os.path.join(p, "a.txt"), "w", encoding="utf-8") as fh:
        fh.write("changed")
    with open(os.path.join(p, "b.txt"), "w", encoding="utf-8") as fh:
        fh.write("new")

    assert mgr.changed(p) == ["a.txt", "b.txt"]
    assert (base / "a.txt").read_text(encoding="utf-8") == "hello"
    mgr.destroy_all()


@needs_git
def test_c6_create_is_idempotent(repo, tmp_path):
    """R: 同 agent 两次 create；I: 每次重建导致已有编辑丢失；P: 内容保留 + 路径相同。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    p1 = mgr.create("a")
    with open(os.path.join(p1, "a.txt"), "w", encoding="utf-8") as fh:
        fh.write("edited")
    p2 = mgr.create("a")
    assert p1 == p2
    assert mgr.read(p2, "a.txt") == b"edited", "重复 create 不得丢弃已有编辑"
    mgr.destroy_all()


def test_c6_copy_detects_same_size_same_mtime(tmp_path):
    """评审 P0-1：copy 模式下「同尺寸 + 恢复 mtime」的改写必须被检出（否则静默丢写）。"""
    base = tmp_path / "plain"
    base.mkdir()
    (base / "a.txt").write_text("hello", encoding="utf-8")     # 5 字节

    mgr = WorktreeManager(str(base), str(tmp_path / "wt"), mode="copy")
    p = mgr.create("a")
    target = os.path.join(p, "a.txt")
    st = os.stat(target)

    with open(target, "w", encoding="utf-8") as fh:
        fh.write("XXXXX")                                      # 同样 5 字节
    os.utime(target, ns=(st.st_atime_ns, st.st_mtime_ns))       # 恢复 mtime

    assert mgr.changed(p) == ["a.txt"], "同尺寸同 mtime 的改写不得漏检"
    mgr.destroy_all()


@needs_git
def test_c6_symlink_escape_rejected(repo, tmp_path):
    """评审 P2：worktree 内符号链接指向外部 → read 必须拒绝。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    p = mgr.create("a")
    os.symlink("/etc/passwd", os.path.join(p, "evil"))
    with pytest.raises(ValueError):
        mgr.read(p, "evil")
    mgr.destroy_all()


@needs_git
def test_c6_concurrent_create_same_agent(repo, tmp_path):
    """评审 P1-3：同 agent 并发 create 不得互删目录/抛 FileExistsError。"""
    mgr = WorktreeManager(str(repo), str(tmp_path / "wt"), mode="git")
    barrier = threading.Barrier(2)
    paths: list[str] = []
    errors: list[BaseException] = []

    def worker():
        barrier.wait()
        try:
            paths.append(mgr.create("same"))
        except BaseException as exc:            # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"并发 create 不得抛异常：{errors}"
    assert len(set(paths)) == 1 and os.path.isdir(paths[0])
    mgr.destroy_all()


def test_c6_missing_base_raises_worktree_error(tmp_path):
    """评审 P1-2：base 不存在 → 包装成 WorktreeError（不是裸 FileNotFoundError）。"""
    mgr = WorktreeManager(str(tmp_path / "nope"), str(tmp_path / "wt"), mode="copy")
    with pytest.raises(WorktreeError):
        mgr.create("a")
