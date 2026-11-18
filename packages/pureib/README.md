# pureib

HTTP/1.1 and WebSocket over native InfiniBand: ibverbs and rdma_cm, reliable-connected queue pairs, no IPoIB, no TCP, no IP addressing on the data path.

## Where it fits

Browsers cannot speak ibverbs, so the browser leg of Open WebUI stays HTTPS and WebSocket. pureib applies to the server-to-server legs:

- Open WebUI to Open Terminal (`ib://` terminal connections in Settings > Admin > Integrations > Open Terminal).
- Open WebUI to Open Computer (`cptr`).
- Any model server or tool service that adopts pureib.

Chat traffic is paced by people, so the latency gain is small there. The gain is real for high-rate server-to-server traffic: bulk file transfer, many small tool calls, and streaming output.

## Why this design

- **Native IB addressing.** Connections use `rdma_cm` with `AF_IB` addresses. The port number becomes the IB service ID, with the same encoding librdmacm uses for `RDMA_PS_IB`.
- **One RC queue pair per connection.** The data path is `ibv_post_send`, `ibv_post_recv`, and the completion queue. Nothing goes through a kernel socket.
- **Registered arena.** Each connection registers one buffer up front. Receives and sends reuse fixed slots, so there is no per-message registration.
- **Inline sends for small messages.** Payloads up to the device's inline limit are copied at post time, which skips a slot and a copy.
- **Credits in the immediate data.** Each message carries the receiver's freed-slot count. Credit-only messages handle idle periods. The sender never has more data outstanding than the receiver has slots, so nothing is dropped.
- **Completion channel in asyncio.** The completion channel and the rdma_cm channel are non-blocking file descriptors registered with the event loop. The code arms the notification before the final poll, so no wakeup is lost.
- **Rejected alternatives.** rsockets adds a socket emulation layer with buffer copies. UCX and libfabric are higher-level dependencies. Raw ibverbs with a TCP bootstrap breaks the no-TCP requirement.

## Build and run

Requirements: `libibverbs-dev`, `librdmacm-dev`, a C compiler, Python 3.11 or newer, and an active IB port.

```bash
pip install ./packages/pureib
python -m pureib                              # prints the ib:// prefix for each active port
open-terminal run --ib 'ib://[<gid>]:8000'    # or ib://auto:8000 to use the first active port
```

In Open WebUI, set the Open Terminal URL to `ib://[<gid>]:8000`. Open WebUI needs pureib installed for `ib://` to work.

## Status and limits

- The native layer compiles with `-Wall -Wextra -Werror`. It has not run against a real HCA. The development environment had none.
- The protocol and the aiohttp and uvicorn integration are tested over an in-process fabric (`pureib.testing.FakeFabric`). The suite covers large payloads, ordering, backpressure, close, handshake rejection, and HTTP and WebSocket traffic. Hardware-only checks skip without a device.
- Native IB only. RoCE needs IP-based addressing, which is not implemented.
- Each connection uses its own completion queue. A shared queue and a shared receive queue are later optimizations.
- Terminal server connections use pureib. The orchestrator path in `utils/terminals.py` still uses TCP.

## Tests

```bash
cd packages/pureib
python setup.py build_ext --inplace   # needs libibverbs and librdmacm headers
PYTHONPATH=src pytest
```
