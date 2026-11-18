"""uvicorn server that accepts HTTP/1.1 and WebSocket over native InfiniBand."""

from __future__ import annotations

import asyncio

import uvicorn

from .address import IBAddress, parse_address
from .transport import IBConfig, create_ib_server


class IBServer(uvicorn.Server):
    def __init__(self, config: uvicorn.Config, address: IBAddress, ib_config: IBConfig | None = None):
        super().__init__(config)
        self.ib_address = address
        self.ib_config = ib_config or IBConfig()

    async def startup(self, sockets=None):
        # Uvicorn owns lifespan and shutdown; no TCP socket is opened.
        await super().startup(sockets=[])
        self.started = False
        if self.should_exit:
            return

        def protocol():
            return self.config.http_protocol_class(
                config=self.config,
                server_state=self.server_state,
                app_state=self.lifespan.state,
            )

        listener = await create_ib_server(self.ib_address, protocol, config=self.ib_config)
        self.servers.append(listener)
        self.ib_address = listener.address
        self.started = True


def serve_ib(app, address: str, *, ib_config: IBConfig | None = None, **options) -> None:
    """Blocking entry point: serves `app` on an ib:// address (ib://auto:<port> picks a port)."""
    config = uvicorn.Config(app, loop='asyncio', **options)
    server = IBServer(config, parse_address(address, allow_auto=True), ib_config)
    asyncio.run(server.serve())
