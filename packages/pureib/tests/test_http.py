"""HTTP and WebSocket traffic through uvicorn and aiohttp, carried over the in-process fabric."""

import asyncio
import ipaddress

import aiohttp
import pytest
import uvicorn
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import StreamingResponse
from pureib.address import IBAddress
from pureib.http_client import IBConnector
from pureib.http_server import IBServer

app = FastAPI()


@app.get('/health')
def health():
    return {'ok': True}


@app.post('/echo')
async def echo(request: Request):
    return Response(await request.body(), media_type='application/octet-stream')


@app.get('/stream')
def stream():
    def chunks():
        for i in range(100):
            yield bytes([i]) * 1000

    return StreamingResponse(chunks(), media_type='application/octet-stream')


@app.websocket('/ws')
async def ws_upper(ws: WebSocket):
    await ws.accept()
    while True:
        message = await ws.receive_text()
        await ws.send_text(message.upper())


@pytest.fixture
async def ib_server(fabric):
    gid = fabric.local_addresses()[0][1]
    address = IBAddress(gid, 8080)
    config = uvicorn.Config(app, loop='asyncio', log_level='warning')
    server = IBServer(config, address)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
        assert not task.done(), task.exception()
    base = f'ib://[{ipaddress.IPv6Address(gid)}]:8080'
    yield base
    server.should_exit = True
    await asyncio.wait_for(task, 10)


async def test_get_json(ib_server):
    async with aiohttp.ClientSession(connector=IBConnector()) as session:
        async with session.get(f'{ib_server}/health') as resp:
            assert resp.status == 200
            assert await resp.json() == {'ok': True}


async def test_request_accepts_the_ssl_argument_the_app_passes(ib_server):
    # Open WebUI passes ssl=AIOHTTP_CLIENT_SESSION_SSL on every terminal request; ib:// must ignore it.
    async with aiohttp.ClientSession(connector=IBConnector()) as session:
        async with session.get(f'{ib_server}/health', ssl=True) as resp:
            assert resp.status == 200


async def test_post_large_body_round_trip(ib_server):
    body = bytes(range(256)) * (5 * 1024 * 1024 // 256)
    async with aiohttp.ClientSession(connector=IBConnector()) as session:
        async with session.post(f'{ib_server}/echo', data=body) as resp:
            assert resp.status == 200
            assert await resp.read() == body


async def test_streamed_response(ib_server):
    expected = b''.join(bytes([i]) * 1000 for i in range(100))
    async with aiohttp.ClientSession(connector=IBConnector()) as session:
        async with session.get(f'{ib_server}/stream') as resp:
            assert await resp.read() == expected


async def test_websocket_echo(ib_server):
    async with aiohttp.ClientSession(connector=IBConnector()) as session:
        async with session.ws_connect(f'{ib_server}/ws') as ws:
            await ws.send_str('hello over ibverbs')
            assert await ws.receive_str() == 'HELLO OVER IBVERBS'
