"""Selects the verbs provider: the native extension, or an injected fabric in tests."""

from __future__ import annotations

from collections import namedtuple

# rdma_cm event kinds, numbered as in <rdma/rdma_cma.h>.
ADDR_RESOLVED, ADDR_ERROR, ROUTE_RESOLVED, ROUTE_ERROR = 0, 1, 2, 3
CONNECT_REQUEST, CONNECT_RESPONSE, CONNECT_ERROR, UNREACHABLE = 4, 5, 6, 7
REJECTED, ESTABLISHED, DISCONNECTED, DEVICE_REMOVAL = 8, 9, 10, 11
TIMEWAIT_EXIT = 15

EVENT_NAMES = {
    ADDR_RESOLVED: 'ADDR_RESOLVED',
    ADDR_ERROR: 'ADDR_ERROR',
    ROUTE_RESOLVED: 'ROUTE_RESOLVED',
    ROUTE_ERROR: 'ROUTE_ERROR',
    CONNECT_REQUEST: 'CONNECT_REQUEST',
    CONNECT_RESPONSE: 'CONNECT_RESPONSE',
    CONNECT_ERROR: 'CONNECT_ERROR',
    UNREACHABLE: 'UNREACHABLE',
    REJECTED: 'REJECTED',
    ESTABLISHED: 'ESTABLISHED',
    DISCONNECTED: 'DISCONNECTED',
    DEVICE_REMOVAL: 'DEVICE_REMOVAL',
    TIMEWAIT_EXIT: 'TIMEWAIT_EXIT',
}
ERROR_EVENTS = frozenset({ADDR_ERROR, ROUTE_ERROR, CONNECT_ERROR, UNREACHABLE, REJECTED, DEVICE_REMOVAL})

CmEvent = namedtuple('CmEvent', 'kind status private child')

_provider = None


def set_provider(provider) -> None:
    """Install a verbs provider. Tests pass a fabric; production uses the native module."""
    global _provider
    _provider = provider


def provider():
    global _provider
    if _provider is None:
        try:
            from . import _verbs as native
        except ImportError as exc:
            raise ImportError(
                'the pureib native extension is not built; install libibverbs-dev and '
                'librdmacm-dev, then run: pip install ./packages/pureib'
            ) from exc
        _provider = native
    return _provider


def new_cm():
    return provider().Cm()


def local_addresses():
    return list(provider().local_addresses())
