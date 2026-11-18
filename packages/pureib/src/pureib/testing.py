"""In-process model of the native verbs layer, for machines without RDMA hardware.

It models what pureib relies on: rdma_cm resolution on (GID, service ID),
connection requests and establishment with private data, reliable sends that
land in posted receives in order, completion queues with notification channels,
and disconnects. A message that arrives before a receive is posted waits, which
is what RNR retry does on hardware. It does not model timing or hardware limits
beyond max inline data.

Use it with backend.set_provider(FakeFabric()).
"""

from __future__ import annotations

import collections
import errno
import os

from .backend import ADDR_ERROR, ADDR_RESOLVED, CONNECT_REQUEST, DISCONNECTED, ESTABLISHED, ROUTE_ERROR, ROUTE_RESOLVED

_OP_SEND = 0
_OP_RECV = 128


class FakeFabric:
    """Stands in for the native module: provides Cm() and local_addresses()."""

    def __init__(self, ports=None, max_inline: int = 128):
        self._listeners: dict[tuple[bytes, int], _FakeCm] = {}
        self._ports = list(ports) if ports else [('fake0', bytes(range(16)), 1)]
        self._max_inline = max_inline
        self._sid = 0x013F0000 + 1000

    def Cm(self):
        return _FakeCm(self)

    def local_addresses(self):
        return list(self._ports)

    def _ephemeral_sid(self) -> int:
        self._sid += 1
        return self._sid


class _FakeCm:
    def __init__(self, fabric: FakeFabric):
        self._fabric = fabric
        self._ev_r, self._ev_w = os.pipe()
        self._cq_r, self._cq_w = os.pipe()
        os.set_blocking(self._ev_r, False)
        os.set_blocking(self._cq_r, False)
        self._events: collections.deque = collections.deque()
        self._cqes: collections.deque = collections.deque()
        self._armed = False
        self._key = None
        self._local = None
        self._target = None
        self._requester = None
        self._peer = None
        self._listening = False
        self._arena = None
        self._max_inline = 0
        self._recvs: collections.deque = collections.deque()
        self._inbound: collections.deque = collections.deque()
        self._closed = False

    # ----- rdma_cm -------------------------------------------------------------------

    def fileno(self) -> int:
        return self._ev_r

    def cq_fileno(self) -> int:
        return self._cq_r

    def _push_event(self, kind, status=0, private=None, child=None) -> None:
        if self._closed:
            return
        self._events.append((kind, status, private, child))
        os.write(self._ev_w, b'x')

    def cm_event(self):
        if not self._events:
            return None
        os.read(self._ev_r, 1)
        return self._events.popleft()

    def bind(self, gid, sid) -> None:
        key = (bytes(gid), sid)
        if len(key[0]) != 16:
            raise ValueError('GID must be 16 bytes')
        if key in self._fabric._listeners:
            raise OSError(errno.EADDRINUSE, f'rdma_bind_addr failed: {errno.errorcode[errno.EADDRINUSE]}')
        self._fabric._listeners[key] = self
        self._key = key
        self._local = key

    def listen(self, backlog) -> None:
        self._listening = True

    def resolve(self, gid, sid, timeout_ms) -> None:
        key = (bytes(gid), sid)
        self._local = (self._fabric._ports[0][1], self._fabric._ephemeral_sid())
        target = self._fabric._listeners.get(key)
        if target is None or not target._listening:
            self._target = None
            self._push_event(ADDR_ERROR, -errno.ETIMEDOUT)
        else:
            self._target = target
            self._push_event(ADDR_RESOLVED, 0)

    def resolve_route(self, timeout_ms) -> None:
        if self._target is None:
            self._push_event(ROUTE_ERROR, -errno.ENETUNREACH)
        else:
            self._push_event(ROUTE_RESOLVED, 0)

    def setup(self, sq_depth, rq_depth, max_inline, arena) -> None:
        if self._arena is not None:
            raise ValueError('queue pair already set up')
        if not isinstance(arena, bytearray):
            raise TypeError('arena must be a bytearray')
        self._arena = arena
        self._armed = True  # ibv_req_notify_cq is armed during setup, as on hardware
        self._max_inline = min(max_inline, self._fabric._max_inline)

    @property
    def max_inline(self) -> int:
        return self._max_inline

    def connect(self, private) -> None:
        if self._target is None or self._arena is None:
            raise OSError(errno.ENOTCONN, 'rdma_connect failed: not routed or not set up')
        child = _FakeCm(self._fabric)
        child._requester = self
        child._local = self._target._key
        self._target._push_event(CONNECT_REQUEST, 0, bytes(private), child)

    def accept(self, private) -> None:
        req = self._requester
        if req is None or req._closed or self._arena is None:
            raise OSError(errno.ECONNABORTED, 'rdma_accept failed')
        self._peer = req
        req._peer = self
        req._push_event(ESTABLISHED, 0, bytes(private), None)
        self._push_event(ESTABLISHED, 0, None, None)

    def disconnect(self) -> None:
        peer = self._peer
        self._peer = None
        self._push_event(DISCONNECTED, 0)
        if peer is not None:
            peer._peer = None
            peer._push_event(DISCONNECTED, 0)

    def peer(self):
        return self._peer._local if self._peer is not None else None

    def local(self):
        return self._local

    # ----- verbs ---------------------------------------------------------------------

    def _check_range(self, offset, length) -> None:
        if self._arena is None:
            raise ValueError('queue pair not setup')
        if offset < 0 or length < 0 or offset + length > len(self._arena):
            raise ValueError('range outside the arena')

    def post_recv(self, wr_id, offset, length) -> None:
        self._check_range(offset, length)
        self._recvs.append((wr_id, offset, length))
        self._deliver_inbound()

    def post_send(self, wr_id, offset, length, imm) -> None:
        self._check_range(offset, length)
        self._transmit(wr_id, bytes(self._arena[offset : offset + length]), imm)

    def post_send_inline(self, wr_id, data, imm) -> None:
        if len(data) > self._max_inline:
            raise ValueError(f'{len(data)} bytes exceed max inline {self._max_inline}')
        self._transmit(wr_id, bytes(data), imm)

    def _transmit(self, wr_id, payload: bytes, imm: int) -> None:
        peer = self._peer
        if peer is None or peer._closed:
            raise OSError(errno.ENOTCONN, 'ibv_post_send failed: queue pair not connected')
        peer._inbound.append((payload, imm))
        peer._deliver_inbound()
        self._push_cqe((wr_id, 0, _OP_SEND, len(payload), 0))

    def _deliver_inbound(self) -> None:
        while self._inbound and self._recvs:
            payload, imm = self._inbound.popleft()
            wr_id, offset, length = self._recvs.popleft()
            if len(payload) > length:
                raise AssertionError('message larger than the posted receive')
            self._arena[offset : offset + len(payload)] = payload
            self._push_cqe((wr_id, 0, _OP_RECV, len(payload), imm))

    def _push_cqe(self, entry) -> None:
        if self._closed:
            return
        self._cqes.append(entry)
        if self._armed:
            self._armed = False
            os.write(self._cq_w, b'x')

    def poll(self, max_entries: int):
        out = []
        while self._cqes and len(out) < max_entries:
            out.append(self._cqes.popleft())
        return out

    def cq_drain(self) -> int:
        total = 0
        while True:
            try:
                chunk = os.read(self._cq_r, 4096)
            except BlockingIOError:
                break
            if not chunk:
                break
            total += len(chunk)
        return total

    def cq_arm(self) -> None:
        self._armed = True

    def close(self) -> None:
        if self._closed:
            return
        peer = self._peer
        if self._key is not None and self._fabric._listeners.get(self._key) is self:
            del self._fabric._listeners[self._key]
        if peer is not None:
            peer._peer = None
            peer._push_event(DISCONNECTED, 0)
        self._peer = None
        self._closed = True
        for fd in (self._ev_r, self._ev_w, self._cq_r, self._cq_w):
            os.close(fd)
