"""S24-P1：多进程 Raft 子进程入口（参数转发，不作为 pytest 用例收集）。"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harness.storage.raft_node import RaftNode


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--partners", required=True)
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--index", type=int, required=True)
    args = ap.parse_args(argv)
    node = RaftNode(args.addr, json.loads(args.partners), args.data_dir,
                    args.index)
    return node.serve_stdio()


if __name__ == "__main__":
    raise SystemExit(main())
