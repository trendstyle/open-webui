"""Open WebUI's terminal client calling a real Open Terminal server over native InfiniBand.

The Open Terminal app runs behind pureib's uvicorn server on the in-process fabric. Calls go
through backend/open_webui/utils/tether.py::get_terminal_client, the same factory Open WebUI
uses for terminal sessions. Skips when either repository is not importable.
"""

import asyncio
import ipaddress
import json
import os
import pathlib
import sys
import tempfile

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
BACKEND = REPO_ROOT / 'backend'
if BACKEND.is_dir() and str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

_TMP = pathlib.Path(tempfile.mkdtemp(prefix='pureib-it-'))
(_TMP / 'files').mkdir(parents=True, exist_ok=True)
os.environ.setdefault('WEBUI_SECRET_KEY', 'pureib-integration-secret')
os.environ.setdefault('DATA_DIR', str(_TMP / 'owui'))
os.environ.setdefault('OPEN_TERMINAL_API_KEY', 'pureib-integration-key')
os.environ.setdefault('OPEN_TERMINAL_FILE_BROWSER_ROOT', str(_TMP / 'files'))

ot_main = pytest.importorskip('open_terminal.main', reason='open-terminal is not installed')
tether = pytest.importorskip('open_webui.utils.tether', reason='open-webui backend is not importable')

import aiohttp  # noqa: E402
import uvicorn  # noqa: E402
from pureib.address import IBAddress  # noqa: E402
from pureib.http_server import IBServer  # noqa: E402

KEY = os.environ['OPEN_TERMINAL_API_KEY']
AUTH = {'Authorization': f'Bearer {KEY}'}


@pytest.fixture
async def open_terminal(fabric):
    gid = fabric.local_addresses()[0][1]
    server = IBServer(uvicorn.Config(ot_main.app, loop='asyncio', log_level='warning'), IBAddress(gid, 8000))
    task = asyncio.create_task(server.serve())
    while not server.started:
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    yield f'ib://[{ipaddress.IPv6Address(gid)}]:8000'
    server.should_exit = True
    await asyncio.wait_for(task, 20)


def terminal_session(url):
    return tether.get_terminal_client(
        url,
        user_id='pureib-test-user',
        service='open_terminal',
        timeout=aiohttp.ClientTimeout(total=60, connect=10),
        trust_env=True,
    )


async def test_health_through_open_webui_client(open_terminal):
    async with terminal_session(open_terminal) as session:
        async with session.get(f'{open_terminal}/health') as resp:
            assert resp.status == 200
            assert await resp.json() == {'status': 'ok'}


async def test_protected_routes_reject_a_missing_key(open_terminal):
    async with terminal_session(open_terminal) as session:
        async with session.get(f'{open_terminal}/system/info', ssl=True) as resp:
            assert resp.status == 401


async def test_command_runs_with_bearer_key(open_terminal):
    async with terminal_session(open_terminal) as session:
        async with session.post(
            f'{open_terminal}/execute?wait=20',
            json={'command': 'echo hello-over-ib'},
            headers=AUTH,
            ssl=True,
        ) as resp:
            body = await resp.json()
            assert resp.status == 200, body
            assert 'hello-over-ib' in json.dumps(body)


async def test_large_upload_lands_intact_on_the_server(open_terminal):
    payload = os.urandom(3 * 1024 * 1024)
    target_dir = _TMP / 'files' / 'upload'
    form = aiohttp.FormData()
    form.add_field('file', payload, filename='blob.bin', content_type='application/octet-stream')
    async with terminal_session(open_terminal) as session:
        async with session.post(
            f'{open_terminal}/files/upload',
            params={'directory': str(target_dir)},
            data=form,
            headers=AUTH,
            ssl=True,
        ) as resp:
            assert resp.status in (200, 201), await resp.text()
        assert (target_dir / 'blob.bin').read_bytes() == payload


async def test_events_websocket_rejects_a_wrong_key(open_terminal):
    async with terminal_session(open_terminal) as session:
        async with session.ws_connect(f'{open_terminal}/events', ssl=True) as ws:
            await ws.send_json({'type': 'auth', 'token': 'not-the-key'})
            msg = await asyncio.wait_for(ws.receive(), 10)
            assert msg.type == aiohttp.WSMsgType.CLOSE
            assert ws.close_code == 4001


async def test_events_websocket_accepts_the_right_key(open_terminal):
    async with terminal_session(open_terminal) as session:
        async with session.ws_connect(f'{open_terminal}/events', ssl=True) as ws:
            await ws.send_json({'type': 'auth', 'token': KEY})
            try:
                msg = await asyncio.wait_for(ws.receive(), 1.0)
            except TimeoutError:
                return  # authenticated and idle, which is the expected steady state
            assert msg.type != aiohttp.WSMsgType.CLOSE


async def test_client_fails_fast_when_no_server_listens(fabric):
    gid = fabric.local_addresses()[0][1]
    url = f'ib://[{ipaddress.IPv6Address(gid)}]:8099'
    async with terminal_session(url) as session:
        with pytest.raises(aiohttp.ClientConnectionError):
            async with session.get(f'{url}/health'):
                pass
