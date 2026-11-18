"""pureib: HTTP and WebSocket over native InfiniBand (ibverbs + rdma_cm), without IPoIB or TCP.

Modules:
  address     ib:// URL parsing
  transport   asyncio transport, listener and connect
  http_client aiohttp connector (IBConnector)
  http_server uvicorn server (IBServer, serve_ib)
  testing     in-process fabric for tests without RDMA hardware
"""

from .address import IBAddress, parse_address
from .transport import IBConfig, IBTransport, connect, create_ib_server

__all__ = ['IBAddress', 'IBConfig', 'IBTransport', 'connect', 'create_ib_server', 'parse_address']
__version__ = '0.1.0'
