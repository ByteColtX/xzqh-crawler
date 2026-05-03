from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from xzqh_crawler.client import XzqhClient, XzqhClientError


@asynccontextmanager
async def run_test_server(app: web.Application, port: int):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_client_retries_retry_after_and_preserves_headers(unused_tcp_port_factory):
    state = {"calls": 0}

    async def handler(request: web.Request) -> web.Response:
        state["calls"] += 1
        assert request.headers["Referer"].endswith("XzqhVersionPublish.html")
        if state["calls"] == 1:
            return web.json_response(
                {"status": 429, "message": "busy"},
                status=429,
                headers={"Retry-After": "0"},
            )
        return web.json_response(
            {
                "status": 200,
                "data": {
                    "code": "00",
                    "name": "中国",
                    "level": 0,
                    "children": [],
                },
            },
        )

    app = web.Application()
    app.router.add_get("/xzqh/getList", handler)

    async with run_test_server(app, unused_tcp_port_factory()) as base_url:
        async with XzqhClient(
            base_url=base_url,
            request_timeout=3,
            retry_attempts=2,
            retry_base_delay=0.01,
        ) as client:
            tree = await client.fetch_tree(code="0", max_level=1)

    assert tree.code == "00"
    assert state["calls"] == 2


@pytest.mark.asyncio
async def test_client_retries_invalid_json(unused_tcp_port_factory):
    state = {"calls": 0}

    async def handler(_: web.Request) -> web.StreamResponse:
        state["calls"] += 1
        if state["calls"] == 1:
            return web.Response(text="{broken json", content_type="application/json")
        return web.json_response(
            {
                "status": 200,
                "data": {
                    "code": "6202",
                    "name": "嘉峪关市",
                    "level": 2,
                    "children": [],
                },
            },
        )

    app = web.Application()
    app.router.add_get("/xzqh/getList", handler)

    async with run_test_server(app, unused_tcp_port_factory()) as base_url:
        async with XzqhClient(
            base_url=base_url,
            request_timeout=3,
            retry_attempts=2,
            retry_base_delay=0.01,
        ) as client:
            tree = await client.fetch_tree(code="6202", max_level=4)

    assert tree.code == "6202"
    assert state["calls"] == 2


@pytest.mark.asyncio
async def test_fetch_township_tree_returns_none_after_retries(unused_tcp_port_factory):
    async def handler(_: web.Request) -> web.Response:
        return web.json_response({"status": 503, "message": "busy"}, status=503)

    app = web.Application()
    app.router.add_get("/xzqh/getList", handler)

    async with run_test_server(app, unused_tcp_port_factory()) as base_url:
        async with XzqhClient(
            base_url=base_url,
            request_timeout=3,
            retry_attempts=2,
            retry_base_delay=0.01,
        ) as client:
            tree = await client.fetch_township_tree("6202")

    assert tree is None


@pytest.mark.asyncio
async def test_fetch_township_tree_logs_debug_without_traceback_for_empty_data(
    unused_tcp_port_factory,
    caplog,
):
    async def handler(_: web.Request) -> web.Response:
        return web.json_response({"status": 200, "data": None})

    app = web.Application()
    app.router.add_get("/xzqh/getList", handler)

    caplog.set_level(logging.DEBUG, logger="xzqh_crawler.client")

    async with run_test_server(app, unused_tcp_port_factory()) as base_url:
        async with XzqhClient(
            base_url=base_url,
            request_timeout=3,
            retry_attempts=1,
            retry_base_delay=0.01,
        ) as client:
            tree = await client.fetch_township_tree("120104")

    assert tree is None
    assert any("乡级数据为空" in record.message for record in caplog.records)
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.asyncio
async def test_fetch_township_tree_rejects_invalid_parent_code():
    async with XzqhClient(base_url="http://127.0.0.1:1") as client:
        with pytest.raises(ValueError):
            await client.fetch_township_tree("62A0")


@pytest.mark.asyncio
async def test_fetch_tree_raises_for_persistent_non_retryable_error(unused_tcp_port_factory):
    async def handler(_: web.Request) -> web.Response:
        return web.json_response({"status": 400, "message": "bad"}, status=400)

    app = web.Application()
    app.router.add_get("/xzqh/getList", handler)

    async with run_test_server(app, unused_tcp_port_factory()) as base_url:
        async with XzqhClient(
            base_url=base_url,
            request_timeout=3,
            retry_attempts=2,
            retry_base_delay=0.01,
        ) as client:
            with pytest.raises(XzqhClientError):
                await client.fetch_tree(code="0", max_level=1)
