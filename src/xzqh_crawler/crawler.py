"""异步行政区划抓取流程。"""
from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from .client import XzqhClient
from .database import Database, TaskWrite
from .models import CrawlStats, Division, DivisionLevel, FetchResult, TaskState
from .utils import log_event

logger = logging.getLogger(__name__)
PROGRESS_LOG_INTERVAL = 25
PROGRESS_LOG_SECONDS = 5.0

@dataclass(slots=True, frozen=True)
class WriterStats:
    saved_divisions: int
    succeeded: int
    failed: int

class XzqhCrawler:
    def __init__(self, *, db_path: str = "./data/xzqh.db", base_url: str = "https://dmfw.mca.gov.cn", request_timeout: float = 45.0, max_concurrency: int = 6, retry_attempts: int = 5, retry_base_delay: float = 2.0, retry_max_delay: float = 60.0, request_interval: float = 0.2) -> None:
        self.db_path, self.base_url = db_path, base_url
        self.request_timeout, self.max_concurrency = request_timeout, max_concurrency
        self.retry_attempts, self.retry_base_delay = retry_attempts, retry_base_delay
        self.retry_max_delay, self.request_interval = retry_max_delay, request_interval
        self.stats = CrawlStats()

    async def crawl(self) -> bool:
        return await self._run(mode="full", include_tree=True)

    async def retry_failed_jobs(self) -> bool:
        return await self._run(mode="resume", include_tree=False)

    async def _run(self, *, mode: str, include_tree: bool) -> bool:
        self.stats = CrawlStats(start_time=datetime.now())
        try:
            async with Database(self.db_path) as db, XzqhClient(base_url=self.base_url, request_timeout=self.request_timeout, max_concurrency=self.max_concurrency, retry_attempts=self.retry_attempts, retry_base_delay=self.retry_base_delay, retry_max_delay=self.retry_max_delay, request_interval=self.request_interval) as client:
                await db.start_run(mode=mode)
                if include_tree:
                    root = await client.fetch_tree(code="0", max_level=3)
                    divisions = [d for d in root.flatten() if int(d.level) <= 3]
                    await db.save_divisions(divisions)
                    self._record_l1_l3(divisions)
                parent_codes = await db.get_task_parent_codes(DivisionLevel.TOWNSHIP) if include_tree else []
                if include_tree:
                    await db.ensure_tasks(parent_codes, DivisionLevel.TOWNSHIP)
                pending = await db.list_tasks((TaskState.PENDING, TaskState.FAILED))
                result = await self._crawl_tasks(db, client, pending)
                self.stats.townships = result.saved_divisions
                self.stats.tasks_succeeded = result.succeeded
                self.stats.tasks_failed = result.failed
                self.stats.total = (await db.get_statistics()).get("total", 0)
                await db.finish_run(status="succeeded" if result.failed == 0 else "failed", stats=self.stats)
                if client.remote_error_counts:
                    log_event(
                        logger,
                        logging.INFO,
                        "remote_error_summary",
                        statuses=",".join(
                            f"{status}:{count}"
                            for status, count in sorted(client.remote_error_counts.items())
                        ),
                    )
                return result.failed == 0
        except Exception as exc:
            logger.error("抓取流程失败: %s", exc, exc_info=logger.isEnabledFor(logging.DEBUG))
            return False
        finally:
            self.stats.end_time = datetime.now()

    async def _crawl_tasks(self, db: Database, client: XzqhClient, parent_codes: list[str]) -> WriterStats:
        if not parent_codes:
            return WriterStats(0, 0, 0)
        worker_count = min(self.max_concurrency, len(parent_codes))
        fetch_queue: asyncio.Queue[str | None] = asyncio.Queue()
        write_queue: asyncio.Queue[FetchResult | None] = asyncio.Queue()
        for code in parent_codes:
            await fetch_queue.put(code)
        for _ in range(worker_count):
            await fetch_queue.put(None)
        writer = asyncio.create_task(self._writer_loop(db, write_queue, len(parent_codes)))
        workers = [asyncio.create_task(self._worker(fetch_queue, write_queue, client)) for _ in range(worker_count)]
        await asyncio.gather(*workers)
        await write_queue.put(None)
        return await writer

    async def _worker(self, fetch_queue: asyncio.Queue[str | None], write_queue: asyncio.Queue[FetchResult | None], client: XzqhClient) -> None:
        while (parent_code := await fetch_queue.get()) is not None:
            await write_queue.put(await self._fetch_one(client, parent_code))

    async def _fetch_one(self, client: XzqhClient, parent_code: str) -> FetchResult:
        try:
            node = await client.fetch_township_tree(parent_code)
            if node is None:
                return FetchResult(parent_code, DivisionLevel.TOWNSHIP, TaskState.FAILED, error="客户端未返回有效树结构")
            divisions = tuple(d for d in node.flatten(parent_code=parent_code) if d.level == DivisionLevel.TOWNSHIP)
            return FetchResult(parent_code, DivisionLevel.TOWNSHIP, TaskState.SUCCEEDED, divisions=divisions)
        except Exception as exc:
            return FetchResult(parent_code, DivisionLevel.TOWNSHIP, TaskState.FAILED, error=str(exc))

    async def _writer_loop(self, db: Database, queue: asyncio.Queue[FetchResult | None], total: int) -> WriterStats:
        saved = succeeded = failed = completed = 0
        last_log = time.monotonic()
        started_at = last_log
        while (result := await queue.get()) is not None:
            completed += 1
            if result.state is TaskState.SUCCEEDED: succeeded += 1
            else: failed += 1
            task_write = TaskWrite(result.parent_code, result.target_level, result.state, result.divisions, result.error)
            saved += await db.apply_task_write(task_write)
            now = time.monotonic()
            # 失败任务统一计入进度和最终摘要；不要为每个远端 5xx 单独打一行，
            # 否则批量故障时终端会被重复日志淹没。
            if completed == 1 or completed == total or completed % PROGRESS_LOG_INTERVAL == 0 or now - last_log >= PROGRESS_LOG_SECONDS:
                elapsed = max(now - started_at, 0.001)
                log_event(logger, logging.INFO, "progress", completed=f"{completed}/{total}", succeeded=succeeded, failed=failed, saved=saved, rate=f"{completed / elapsed:.1f}/s")
                last_log = now
        return WriterStats(saved, succeeded, failed)

    def _record_l1_l3(self, divisions: list[Division]) -> None:
        self.stats.provinces = sum(d.level == DivisionLevel.PROVINCE for d in divisions)
        self.stats.cities = sum(d.level == DivisionLevel.CITY for d in divisions)
        self.stats.counties = sum(d.level == DivisionLevel.COUNTY for d in divisions)
