"""S11 egress 子进程探针（S11-5 / S11-6 的正负对照共用）。

用法::

    python tests/_egress_child.py exit 7
    python tests/_egress_child.py connect 192.0.2.1 443 0.5

`connect` 输出 `OK` 或 `ERR:<errno 名>`（`ENETUNREACH` / `ECONNREFUSED` / ...），
退出码 0=连上、1=失败。**只打印 errno 名**，避免不同 libc 文案差异。
"""

from __future__ import annotations

import errno as errno_mod
import socket
import sys


def _connect(host: str, port: int, timeout: float) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
    except OSError as exc:
        name = errno_mod.errorcode.get(exc.errno, str(exc.errno))
        print(f"ERR:{name}")
        return 1
    finally:
        sock.close()
    print("OK")
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print("ERR:no-mode")
        return 2
    mode = argv[0]
    if mode == "exit":
        return int(argv[1])
    if mode == "connect":
        return _connect(argv[1], int(argv[2]), float(argv[3]))
    print(f"ERR:unknown-mode:{mode}")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
