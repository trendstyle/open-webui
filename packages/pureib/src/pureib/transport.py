"""asyncio transport over one native InfiniBand connection.

Every message is one reliable-connected SEND with immediate data. The immediate
carries credits: how many receive slots the sender has freed since its last
message. A sender keeps no more data messages outstanding than the receiver
has data slots, so receive queues never run dry for data. Zero-length messages
carry credits alone. Credits return only after the protocol accepts the bytes,
so a paused reader pushes backpressure back to the sending side.
"""

from __future__ import annotations

import asyncio
import collections
import ipaddress
import struct
from dataclasses import dataclass

from . import backend
from .address import IBAddress
from .backend import (
    CONNECT_REQUEST,
    DEVICE_REMOVAL,
    DISCONNECTED,
    ERROR_EVENTS,
    ESTABLISHED,
    EVENT_NAMES,
    REJECTED,
    TIMEWAIT_EXIT,
    CmEvent,
)

MAGIC = b'PUREIB01'
_HELLO = struct.Struct('<8sHI')  # magic, data slots, slot size
_KIND_SHIFT = 40
_IDX_MASK = (1 << _KIND_SHIFT) - 1
WR_RECV, WR_SEND, WR_INLINE, WR_CTRL = 1, 2, 3, 4
_CLOSE_GRACE = 5.0
_HANDSHAKE_TIMEOUT = 10.0


@dataclass(frozen=True)
class IBConfig:
    """Sizing for one connection. Peers advertise data slots and slot size in the handshake."""

    data_slots: int = 16
    ctrl_slots: int = 4
    send_slots: int = 16
    slot_size: int = 32 * 1024
    max_inline: int = 128

    def __post_init__(self):
        if min(self.data_slots, self.ctrl_slots, self.send_slots, self.slot_size, self.max_inline) < 1:
            raise ValueError('pureib sizes must be positive')
        if self.data_slots > 0xFFFF or self.slot_size > 0xFFFFFFFF:
            raise ValueError('pureib sizes exceed the handshake encoding')

    def hello(self) -> bytes:
        return _HELLO.pack(MAGIC, self.data_slots, self.slot_size)

    @property
    def arena_size(self) -> int:
        return (self.data_slots + self.ctrl_slots + self.send_slots) * self.slot_size

    @property
    def sq_depth(self) -> int:
        return self.send_slots + self.ctrl_slots + 16

    @property
    def rq_depth(self) -> int:
        return self.data_slots + self.ctrl_slots + 4


def _parse_hello(data: bytes) -> tuple[int, int]:
    if len(data) != _HELLO.size:
        raise ValueError('peer did not speak pureib')
    magic, data_slots, slot_size = _HELLO.unpack(data)
    if magic != MAGIC or data_slots < 1 or slot_size < 1:
        raise ValueError('peer did not speak pureib')
    return data_slots, slot_size


def _host_port(pair):
    if pair is None:
        return None
    gid, sid = pair
    return str(ipaddress.IPv6Address(gid)), sid & 0xFFFF


def _cm_error(ev: CmEvent) -> Exception:
    name = EVENT_NAMES.get(ev.kind, str(ev.kind))
    if ev.kind == REJECTED:
        return ConnectionRefusedError(f'rdma_cm {name} (status {ev.status})')
    return ConnectionError(f'rdma_cm {name} (status {ev.status})')


async def _expect(loop, cm, kinds, timeout):
    """Waits for one of `kinds` on the connection's rdma_cm channel."""
    fut = loop.create_future()
    fd = cm.fileno()

    def on_readable():
        while not fut.done():
            raw = cm.cm_event()
            if raw is None:
                return
            ev = CmEvent(*raw)
            if ev.kind in kinds:
                fut.set_result(ev)
            elif ev.kind in ERROR_EVENTS:
                fut.set_exception(_cm_error(ev))

    loop.add_reader(fd, on_readable)
    try:
        return await asyncio.wait_for(fut, timeout)
    finally:
        loop.remove_reader(fd)


class IBTransport(asyncio.Transport):
    """Byte stream over a native InfiniBand connection, shaped for asyncio protocols."""

    def __init__(self, cm, arena, cfg: IBConfig, peer_hello: tuple[int, int], protocol):
        loop = asyncio.get_running_loop()
        super().__init__({'peername': _host_port(cm.peer()), 'sockname': _host_port(cm.local())})
        self._loop = loop
        self._cm = cm
        self._cfg = cfg
        self._protocol = protocol
        self._arena = arena
        self._mv = memoryview(arena)
        peer_data_slots, peer_slot_size = peer_hello
        self._slot = min(cfg.slot_size, peer_slot_size)
        self._n_recv = cfg.data_slots + cfg.ctrl_slots
        self._peer_credits = peer_data_slots
        self._owed = 0
        self._free = collections.deque(range(self._n_recv, self._n_recv + cfg.send_slots))
        self._sq_inflight = 0
        self._max_inline = min(cfg.max_inline, cm.max_inline)
        self._out = bytearray()
        self._out_pos = 0
        self._rx: list[bytes] = []
        self._high = 64 * 1024
        self._low = 16 * 1024
        self._reading = True
        self._paused_writing = False
        self._closing = False
        self._disconnecting = False
        self._finished = False
        self._ctrl_wanted = False
        self._flush_scheduled = False
        self._close_timer = None

        for i in range(self._n_recv):
            cm.post_recv((WR_RECV << _KIND_SHIFT) | i, i * cfg.slot_size, cfg.slot_size)
        loop.add_reader(cm.cq_fileno(), self._on_cq)
        loop.add_reader(cm.fileno(), self._on_cm)

    # ----- asyncio.Transport surface -------------------------------------------------

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def is_closing(self) -> bool:
        return self._closing or self._finished

    def is_reading(self) -> bool:
        return self._reading and not self._finished

    def pause_reading(self) -> None:
        self._reading = False

    def resume_reading(self) -> None:
        self._reading = True
        if self._rx:
            self._loop.call_soon(self._resume_delivery)

    def get_write_buffer_size(self) -> int:
        return self._out_len()

    def get_write_buffer_limits(self):
        return self._low, self._high

    def set_write_buffer_limits(self, high=None, low=None) -> None:
        high = 64 * 1024 if high is None else high
        low = high // 4 if low is None else low
        if low < 0 or high < low:
            raise ValueError('expected 0 <= low <= high')
        self._high, self._low = high, low

    def can_write_eof(self) -> bool:
        return False

    def write(self, data) -> None:
        if self._closing or self._finished or not data:
            return
        self._out += data
        self._try_send()
        if not self._paused_writing and self._out_len() > self._high:
            self._paused_writing = True
            self._protocol.pause_writing()

    def writelines(self, list_of_data) -> None:
        self.write(b''.join(list_of_data))

    def close(self) -> None:
        if self._closing or self._finished:
            return
        self._closing = True
        self._close_timer = self._loop.call_later(_CLOSE_GRACE, self._finish, None)
        self._try_send()

    def abort(self) -> None:
        self._finish(None)

    # ----- completion and CM handling ------------------------------------------------

    def _on_cq(self) -> None:
        if self._finished:
            return
        try:
            self._cm.cq_drain()
            while True:
                wcs = self._cm.poll(64)
                if not wcs:
                    self._cm.cq_arm()
                    wcs = self._cm.poll(64)
                    if not wcs:
                        break
                for wr, status, _opcode, byte_len, imm in wcs:
                    self._completion(wr, status, byte_len, imm)
            self._deliver()
            self._try_send()
        except OSError as exc:
            self._finish(exc)

    def _completion(self, wr: int, status: int, byte_len: int, imm: int) -> None:
        if status:
            raise OSError(f'completion error {status} on work request kind {wr >> _KIND_SHIFT}')
        kind = wr >> _KIND_SHIFT
        idx = wr & _IDX_MASK
        if kind == WR_RECV:
            off = idx * self._cfg.slot_size
            if byte_len:
                self._rx.append(self._mv[off : off + byte_len].tobytes())
            self._cm.post_recv(wr, off, self._cfg.slot_size)
            self._peer_credits += imm
            return
        self._sq_inflight -= 1
        if kind == WR_SEND:
            self._free.append(idx)

    def _on_cm(self) -> None:
        if self._finished:
            return
        try:
            while True:
                raw = self._cm.cm_event()
                if raw is None:
                    break
                ev = CmEvent(*raw)
                if ev.kind == DISCONNECTED or ev.kind == DEVICE_REMOVAL:
                    self._on_cq()
                    self._finish(OSError('device removed') if ev.kind == DEVICE_REMOVAL else None)
                    return
                if ev.kind == TIMEWAIT_EXIT:
                    continue
        except OSError as exc:
            self._finish(exc)

    # ----- data path -----------------------------------------------------------------

    def _out_len(self) -> int:
        return len(self._out) - self._out_pos

    def _compact_out(self) -> None:
        if self._out_pos and self._out_pos == len(self._out):
            self._out.clear()
            self._out_pos = 0
        elif self._out_pos > 65536 and self._out_pos * 2 > len(self._out):
            del self._out[: self._out_pos]
            self._out_pos = 0

    def _try_send(self) -> None:
        if self._finished:
            return
        try:
            cfg = self._cfg
            while self._out_len() and self._peer_credits > 0 and self._sq_inflight < cfg.sq_depth:
                n = min(self._out_len(), self._slot)
                start = self._out_pos
                imm = self._owed
                self._owed = 0
                if n <= self._max_inline:
                    self._cm.post_send_inline(WR_INLINE << _KIND_SHIFT, bytes(self._out[start : start + n]), imm)
                else:
                    if not self._free:
                        break
                    idx = self._free.popleft()
                    off = idx * cfg.slot_size
                    self._mv[off : off + n] = self._out[start : start + n]
                    self._cm.post_send((WR_SEND << _KIND_SHIFT) | idx, off, n, imm)
                self._out_pos += n
                self._peer_credits -= 1
                self._sq_inflight += 1
            self._compact_out()

            threshold = max(1, cfg.data_slots // 2)
            if (
                self._owed
                and self._sq_inflight < cfg.sq_depth
                and (self._owed >= threshold or (self._ctrl_wanted and not self._out_len()))
            ):
                self._cm.post_send_inline(WR_CTRL << _KIND_SHIFT, b'', self._owed)
                self._owed = 0
                self._sq_inflight += 1
            self._ctrl_wanted = False

            if self._paused_writing and self._out_len() <= self._low:
                self._paused_writing = False
                self._protocol.resume_writing()
            if self._closing and not self._out_len() and self._sq_inflight == 0:
                self._begin_disconnect()
        except OSError as exc:
            self._finish(exc)

    def _deliver(self) -> None:
        if not self._rx or not self._reading or self._finished:
            return
        chunks = self._rx
        self._rx = []
        self._owed += len(chunks)
        data = chunks[0] if len(chunks) == 1 else b''.join(chunks)
        try:
            self._protocol.data_received(data)
        except Exception as exc:  # asyncio semantics: a protocol error closes the transport
            self._finish(exc)
            return
        self._ctrl_wanted = True
        if not self._flush_scheduled:
            self._flush_scheduled = True
            self._loop.call_soon(self._scheduled_flush)

    def _scheduled_flush(self) -> None:
        self._flush_scheduled = False
        self._try_send()

    def _resume_delivery(self) -> None:
        self._deliver()
        self._try_send()

    def _begin_disconnect(self) -> None:
        if self._disconnecting:
            return
        self._disconnecting = True
        try:
            self._cm.disconnect()
        except OSError as exc:
            self._finish(exc)

    def _finish(self, exc) -> None:
        if self._finished:
            return
        self._finished = True
        if self._close_timer is not None:
            self._close_timer.cancel()
        for fd_of in (self._cm.cq_fileno, self._cm.fileno):
            try:
                self._loop.remove_reader(fd_of())
            except (OSError, ValueError):
                pass
        try:
            self._cm.close()
        except OSError:
            pass
        self._mv.release()
        self._protocol.connection_lost(exc)


def _make_arena(cfg: IBConfig) -> bytearray:
    return bytearray(cfg.arena_size)


async def connect(
    address: IBAddress, protocol_factory, *, config: IBConfig | None = None, timeout: float = _HANDSHAKE_TIMEOUT
):
    """Opens a native IB connection to `address` and returns (transport, protocol)."""
    if address.gid is None:
        raise ValueError('connecting needs an explicit GID')
    cfg = config or IBConfig()
    loop = asyncio.get_running_loop()
    cm = backend.new_cm()
    try:
        ms = max(1, int(timeout * 1000))
        cm.resolve(address.gid, address.service_id, ms)
        await _expect(loop, cm, (backend.ADDR_RESOLVED,), timeout)
        cm.resolve_route(ms)
        await _expect(loop, cm, (backend.ROUTE_RESOLVED,), timeout)
        arena = _make_arena(cfg)
        cm.setup(cfg.sq_depth, cfg.rq_depth, cfg.max_inline, arena)
        cm.connect(cfg.hello())
        ev = await _expect(loop, cm, (ESTABLISHED,), timeout)
        peer = _parse_hello(ev.private or b'')
    except BaseException:
        cm.close()
        raise
    protocol = protocol_factory()
    transport = IBTransport(cm, arena, cfg, peer, protocol)
    protocol.connection_made(transport)
    return transport, protocol


class IBListener:
    """Accepts native IB connections for one service ID and hands each to a new protocol."""

    def __init__(self, address: IBAddress, protocol_factory, cfg: IBConfig, backlog: int):
        self._loop = asyncio.get_running_loop()
        self._factory = protocol_factory
        self._cfg = cfg
        self._tasks: set[asyncio.Task] = set()
        self._closed = False
        self.address = address
        self._cm = backend.new_cm()
        try:
            self._cm.bind(address.gid, address.service_id)
            self._cm.listen(backlog)
        except BaseException:
            self._cm.close()
            raise
        self._loop.add_reader(self._cm.fileno(), self._on_cm)

    @property
    def sockets(self):
        return ()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._loop.remove_reader(self._cm.fileno())
        except (OSError, ValueError):
            pass
        self._cm.close()

    async def wait_closed(self) -> None:
        return None

    def _on_cm(self) -> None:
        while not self._closed:
            try:
                raw = self._cm.cm_event()
            except OSError:
                return
            if raw is None:
                return
            ev = CmEvent(*raw)
            if ev.kind == CONNECT_REQUEST and ev.child is not None:
                self._accept(ev.child, ev.private)
            elif ev.kind == DEVICE_REMOVAL:
                self.close()
                return

    def _accept(self, child, private: bytes) -> None:
        try:
            peer = _parse_hello(private or b'')
            arena = _make_arena(self._cfg)
            child.setup(self._cfg.sq_depth, self._cfg.rq_depth, self._cfg.max_inline, arena)
            child.accept(self._cfg.hello())
        except (OSError, ValueError):
            child.close()
            return
        task = self._loop.create_task(self._establish(child, arena, peer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _establish(self, child, arena, peer) -> None:
        try:
            await _expect(self._loop, child, (ESTABLISHED,), _HANDSHAKE_TIMEOUT)
        except (TimeoutError, OSError):
            child.close()
            return
        protocol = self._factory()
        transport = IBTransport(child, arena, self._cfg, peer, protocol)
        protocol.connection_made(transport)


async def create_ib_server(
    address: IBAddress, protocol_factory, *, config: IBConfig | None = None, backlog: int = 128
) -> IBListener:
    """Starts listening. `address.gid` may be None, in which case the first ACTIVE port is used."""
    gid = address.gid
    if gid is None:
        ports = backend.local_addresses()
        if not ports:
            raise OSError('no ACTIVE native InfiniBand port is available')
        gid = ports[0][1]
        address = IBAddress(gid, address.port)
    return IBListener(address, protocol_factory, config or IBConfig(), backlog)
