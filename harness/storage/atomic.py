"""持久化原语（方案 §4.2）。

三条硬约束（调研结论）：
1. 临时文件必须与目标**同目录**（跨设备 rename 不原子，会退化为 copy+unlink）；
2. 用 `os.replace` 而非 `os.rename`（Windows 覆盖语义不同）；
3. `fsync` 数据之后还要 `fsync` 父目录（rename 本身要持久）。
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def write_all(fd: int, data: bytes) -> None:
    """循环写，处理短写（`write(2)` 允许只写一部分）。"""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("write 返回 0，无法继续")
        view = view[written:]


def fsync_dir(path: str | Path) -> None:
    """把目录项变更刷盘（rename / unlink 的持久性依赖这一步）。"""
    directory = os.path.dirname(os.path.abspath(os.fspath(path))) or "."
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: str | Path, data: bytes) -> None:
    """同目录临时文件 → fsync → 原子替换 → fsync 目录。"""
    target = os.path.abspath(os.fspath(path))
    directory = os.path.dirname(target)
    os.makedirs(directory, exist_ok=True)

    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
    try:
        write_all(fd, data)
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    else:
        os.close(fd)

    os.replace(tmp, target)
    fsync_dir(target)


def atomic_delete(path: str | Path) -> None:
    target = os.path.abspath(os.fspath(path))
    try:
        os.unlink(target)
    except FileNotFoundError:
        return
    fsync_dir(target)
