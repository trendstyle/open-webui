"""aiohttp connector that carries HTTP and WebSocket over native InfiniBand."""

from __future__ import annotations

import aiohttp
from aiohttp.connector import HIGH_LEVEL_SCHEMA_SET, BaseConnector

from .address import parse_address
from .transport import IBConfig, connect


class IBConnector(BaseConnector):
    """Use with aiohttp.ClientSession(connector=IBConnector()) and ib://[gid]:port URLs."""

    allowed_protocol_schema_set = HIGH_LEVEL_SCHEMA_SET | frozenset({'ib'})

    def __init__(self, *, config: IBConfig | None = None, **kwargs):
        super().__init__(**kwargs)
        self._ib_config = config or IBConfig()

    async def _create_connection(self, req, traces, timeout):
        address = parse_address(str(req.url))
        try:
            _transport, protocol = await connect(
                address,
                self._factory,
                config=self._ib_config,
                timeout=timeout.sock_connect or 10.0,
            )
        except (OSError, ValueError) as exc:
            raise aiohttp.ClientConnectionError(f'native InfiniBand connect to {address} failed: {exc}') from exc
        return protocol
