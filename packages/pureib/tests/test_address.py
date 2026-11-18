import ipaddress

import pytest
from pureib.address import IBAddress, parse_address


def test_gid_and_port_round_trip():
    gid = bytes(range(16))
    addr = parse_address(f'ib://[{ipaddress.IPv6Address(gid)}]:8000')
    assert addr == IBAddress(gid, 8000)
    assert addr.service_id == (0x013F << 16) + 8000
    assert parse_address(str(addr)) == addr


def test_auto_only_for_listeners():
    assert parse_address('ib://auto:9000', allow_auto=True) == IBAddress(None, 9000)
    with pytest.raises(ValueError):
        parse_address('ib://auto:9000')


@pytest.mark.parametrize(
    'text',
    ['http://[::1]:80', 'ib://[::1]', 'ib://[::1]:0', 'ib://[::1]:70000', 'ib://device-name:80'],
)
def test_rejects_invalid_addresses(text):
    with pytest.raises(ValueError):
        parse_address(text)
