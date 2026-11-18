import asyncio

import pytest
from pureib.address import IBAddress
from pureib.transport import IBConfig, connect, create_ib_server


class Collector(asyncio.Protocol):
    def __init__(self):
        self.transport = None
        self.buf = bytearray()
        self.lost = asyncio.get_running_loop().create_future()
        self.paused = 0
        self.resumed = 0

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.buf += data

    def pause_writing(self):
        self.paused += 1

    def resume_writing(self):
        self.resumed += 1

    def connection_lost(self, exc):
        if not self.lost.done():
            self.lost.set_result(exc)


class Echo(Collector):
    def data_received(self, data):
        self.transport.write(data)


async def until(pred, timeout=10.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not pred():
        if loop.time() > end:
            raise TimeoutError('condition not reached')
        await asyncio.sleep(0.001)


def addr_for(fabric, port):
    return IBAddress(fabric.local_addresses()[0][1], port)


async def test_large_echo_round_trip(fabric):
    server = await create_ib_server(addr_for(fabric, 7001), Echo)
    try:
        client_t, client = await connect(addr_for(fabric, 7001), Collector)
        payload = bytes(range(256)) * (3 * 1024 * 1024 // 256)
        client_t.write(payload)
        await until(lambda: len(client.buf) == len(payload))
        assert bytes(client.buf) == payload
        client_t.close()
        assert await asyncio.wait_for(client.lost, 10) is None
    finally:
        server.close()


async def test_many_small_messages_keep_order(fabric):
    server = await create_ib_server(addr_for(fabric, 7002), Echo)
    try:
        client_t, client = await connect(addr_for(fabric, 7002), Collector)
        expected = b''.join(b'%06d;' % i for i in range(5000))
        for i in range(5000):
            client_t.write(b'%06d;' % i)
        await until(lambda: len(client.buf) == len(expected))
        assert bytes(client.buf) == expected
        client_t.close()
    finally:
        server.close()


async def test_backpressure_pauses_and_resumes_without_loss(fabric):
    cfg = IBConfig(data_slots=2, ctrl_slots=1, send_slots=2, slot_size=4096, max_inline=64)
    received = []

    def factory():
        proto = Collector()
        received.append(proto)
        return proto

    server = await create_ib_server(addr_for(fabric, 7003), factory, config=cfg)
    try:
        client_t, client = await connect(addr_for(fabric, 7003), Collector, config=cfg)
        client_t.set_write_buffer_limits(high=16384, low=4096)
        await until(lambda: len(received) == 1)
        server_side = received[0]
        server_side.transport.pause_reading()

        payload = bytes((i * 7) % 251 for i in range(300_000))
        client_t.write(payload)
        assert client.paused >= 1
        await asyncio.sleep(0.05)
        assert len(server_side.buf) == 0

        server_side.transport.resume_reading()
        await until(lambda: len(server_side.buf) == len(payload))
        assert bytes(server_side.buf) == payload
        assert client.resumed >= 1
        client_t.close()
    finally:
        server.close()


async def test_close_reaches_both_sides(fabric):
    seen = []

    def factory():
        proto = Collector()
        seen.append(proto)
        return proto

    server = await create_ib_server(addr_for(fabric, 7004), factory)
    try:
        client_t, client = await connect(addr_for(fabric, 7004), Collector)
        await until(lambda: len(seen) == 1)
        client_t.close()
        assert await asyncio.wait_for(client.lost, 10) is None
        assert await asyncio.wait_for(seen[0].lost, 10) is None
    finally:
        server.close()


async def test_connect_to_missing_service_fails(fabric):
    with pytest.raises(ConnectionError):
        await connect(addr_for(fabric, 7999), Collector, timeout=2)


async def test_handshake_rejects_foreign_peers(fabric):
    made = []

    def factory():
        proto = Collector()
        made.append(proto)
        return proto

    addr = addr_for(fabric, 7005)
    server = await create_ib_server(addr, factory)
    try:
        cfg = IBConfig()
        rogue = fabric.Cm()
        rogue.resolve(addr.gid, addr.service_id, 1000)
        await asyncio.sleep(0.01)
        rogue.resolve_route(1000)
        rogue.setup(cfg.sq_depth, cfg.rq_depth, cfg.max_inline, bytearray(cfg.arena_size))
        rogue.connect(b'not pureib')
        await asyncio.sleep(0.05)
        assert made == []
    finally:
        server.close()


def test_config_rejects_nonpositive_sizes():
    with pytest.raises(ValueError):
        IBConfig(data_slots=0)
