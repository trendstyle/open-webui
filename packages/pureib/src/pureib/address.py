"""ib:// addresses for native InfiniBand services.

ib://[<gid>]:<port>   a peer reached through its port GID
ib://auto:<port>      listen on the first ACTIVE native IB port (listeners only)

The port becomes the rdma_cm service ID, using the same encoding librdmacm
uses for RDMA_PS_IB, so two processes agree on it without any IP addressing.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from urllib.parse import urlsplit

SCHEME = 'ib'
RDMA_PS_IB = 0x013F


@dataclass(frozen=True)
class IBAddress:
    gid: bytes | None  # None only for "auto" listeners
    port: int

    @property
    def service_id(self) -> int:
        return (RDMA_PS_IB << 16) + self.port

    def __str__(self) -> str:
        host = 'auto' if self.gid is None else f'[{ipaddress.IPv6Address(self.gid)}]'
        return f'{SCHEME}://{host}:{self.port}'


def parse_address(text: str, *, allow_auto: bool = False) -> IBAddress:
    parts = urlsplit(str(text))
    if parts.scheme != SCHEME:
        raise ValueError(f'expected an ib:// address, got {text!r}')
    port = parts.port
    if port is None or not 1 <= port <= 65535:
        raise ValueError('ib:// addresses need a port between 1 and 65535')
    host = parts.hostname or ''
    if host == 'auto':
        if not allow_auto:
            raise ValueError('ib://auto is only valid for listeners')
        return IBAddress(None, port)
    try:
        gid = ipaddress.IPv6Address(host).packed
    except ValueError as exc:
        raise ValueError(f"ib:// host must be a bracketed GID or 'auto': {text!r}") from exc
    return IBAddress(gid, port)
