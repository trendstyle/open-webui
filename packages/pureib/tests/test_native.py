"""Checks the compiled extension without requiring RDMA hardware."""

import pytest
from pureib import backend


@pytest.fixture
def native():
    backend.set_provider(None)
    native = pytest.importorskip('pureib._verbs')
    yield native
    backend.set_provider(None)


def test_extension_reports_abi_and_devices(native):
    assert native.ABI_VERSION == 1
    assert isinstance(native.local_addresses(), list)


def test_connection_object_fails_cleanly_without_a_device(native):
    if native.local_addresses():
        pytest.skip('RDMA hardware present; device path is exercised by integration runs')
    with pytest.raises(OSError):
        native.Cm()


def _cm_or_skip(native):
    try:
        return native.Cm()
    except OSError as exc:
        pytest.skip(f'no RDMA device to construct a connection object: {exc}')


def test_bind_validates_gid_length(native):
    cm = _cm_or_skip(native)
    try:
        with pytest.raises(ValueError):
            cm.bind(b'\x00' * 15, 0x013F0001)
    finally:
        cm.close()


def test_post_paths_reject_before_touching_verbs(native):
    cm = _cm_or_skip(native)
    try:
        with pytest.raises(ValueError):
            cm.post_send_inline(1, b'x', 0)
    finally:
        cm.close()
