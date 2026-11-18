"""python -m pureib: lists ACTIVE native InfiniBand ports and the ib:// prefix to use for each."""

import ipaddress
import sys

from . import backend


def main() -> int:
    rows = backend.local_addresses()
    if not rows:
        print('no ACTIVE native InfiniBand port found', file=sys.stderr)
        return 1
    for name, gid, port in rows:
        print(f'{name} port {port}: ib://[{ipaddress.IPv6Address(gid)}]:<service port>')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
