from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from xzqh_crawler.crawler import XzqhCrawler
from xzqh_crawler.database import Database


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
async def test_crawler_crawls_level1_to_level4_and_updates_jobs(tmp_path, unused_tcp_port_factory):
    async def handler(request: web.Request) -> web.Response:
        code = request.query["code"]
        max_level = request.query["maxLevel"]

        if code == "0" and max_level == "3":
            return web.json_response(
                {
                    "status": 200,
                    "data": {
                        "code": "00",
                        "name": "中国",
                        "level": 0,
                        "children": [
                            {
                                "code": "11",
                                "name": "北京市",
                                "level": 1,
                                "children": [
                                    {
                                        "code": "110101",
                                        "name": "东城区",
                                        "level": 3,
                                        "children": [],
                                    },
                                ],
                            },
                            {
                                "code": "62",
                                "name": "甘肃省",
                                "level": 1,
                                "children": [
                                    {
                                        "code": "6202",
                                        "name": "嘉峪关市",
                                        "level": 2,
                                        "children": [],
                                    },
                                ],
                            },
                        ],
                    },
                },
            )

        if code == "110101" and max_level == "4":
            return web.json_response(
                {
                    "status": 200,
                    "data": {
                        "code": "110101",
                        "name": "东城区",
                        "level": 3,
                        "children": [
                            {
                                "code": "110101001",
                                "name": "景山街道",
                                "level": 4,
                                "children": [],
                            },
                        ],
                    },
                },
            )

        if code == "6202" and max_level == "4":
            return web.json_response(
                {
                    "status": 200,
                    "data": {
                        "code": "6202",
                        "name": "嘉峪关市",
                        "level": 2,
                        "children": [
                            {
                                "code": "620201001",
                                "name": "雄关街道",
                                "level": 4,
                                "children": [],
                            },
                        ],
                    },
                },
            )

        raise AssertionError(f"未预期的请求: code={code} maxLevel={max_level}")

    app = web.Application()
    app.router.add_get("/xzqh/getList", handler)

    db_path = tmp_path / "crawler.db"
    async with run_test_server(app, unused_tcp_port_factory()) as base_url:
        crawler = XzqhCrawler(
            db_path=str(db_path),
            base_url=base_url,
            request_timeout=3,
            max_concurrency=4,
            retry_attempts=2,
            retry_base_delay=0.01,
            write_batch_size=1,
        )
        success = await crawler.crawl()

    assert success is True
    assert crawler.stats["provinces"] == 2
    assert crawler.stats["cities"] == 1
    assert crawler.stats["counties"] == 1
    assert crawler.stats["townships"] == 2

    async with Database(str(db_path)) as db:
        stats = await db.get_statistics()
        job_counts = await db.get_crawl_job_counts()

    assert stats["total"] == 6
    assert stats["level_4"] == 2
    assert job_counts == {"pending": 0, "ok": 2, "failed": 0}
