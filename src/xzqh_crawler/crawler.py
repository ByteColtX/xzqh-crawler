"""异步爬虫主流程。"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime

from .client import XzqhClient
from .database import CrawlJobWrite, Database
from .models import AdministrativeDivision

logger = logging.getLogger(__name__)
PROGRESS_LOG_INTERVAL = 25
PROGRESS_LOG_SECONDS = 5.0


@dataclass(slots=True, frozen=True)
class Level4FetchResult:
    """单个 L4 抓取任务的结果。"""

    parent_code: str
    ok: bool
    divisions: tuple[AdministrativeDivision, ...]
    error: str | None = None


@dataclass(slots=True, frozen=True)
class WriterStats:
    """写入协程汇总统计。"""

    saved_divisions: int
    ok_jobs: int
    failed_jobs: int


class XzqhCrawler:
    """行政区划异步爬虫。"""

    def __init__(
        self,
        *,
        db_path: str = "./data/xzqh.db",
        base_url: str = "https://dmfw.mca.gov.cn",
        request_timeout: float = 30.0,
        max_concurrency: int = 20,
        retry_attempts: int = 4,
        retry_base_delay: float = 1.0,
        write_batch_size: int = 200,
    ) -> None:
        """初始化爬虫。

        Args:
            db_path: SQLite 文件路径。
            base_url: API 根地址。
            request_timeout: 单请求超时时间。
            max_concurrency: 抓取协程数量。
            retry_attempts: HTTP 最大重试次数。
            retry_base_delay: HTTP 重试基础退避时间。
            write_batch_size: writer 协程每次事务批量提交的任务数。
        """
        self.db_path = db_path
        self.base_url = base_url
        self.request_timeout = request_timeout
        self.max_concurrency = max_concurrency
        self.retry_attempts = retry_attempts
        self.retry_base_delay = retry_base_delay
        self.write_batch_size = write_batch_size
        self.stats: dict[str, object] = {}
        self._reset_stats()

    async def crawl(self) -> bool:
        """抓取全部 1-4 级行政区划数据。

        Returns:
            bool: 是否成功完成抓取。
        """
        self._reset_stats()
        self.stats["start_time"] = datetime.now()
        logger.info(
            "开始抓取: db=%s concurrency=%s timeout=%ss",
            self.db_path,
            self.max_concurrency,
            self.request_timeout,
        )

        try:
            async with Database(self.db_path) as db, XzqhClient(
                base_url=self.base_url,
                request_timeout=self.request_timeout,
                max_concurrency=self.max_concurrency,
                retry_attempts=self.retry_attempts,
                retry_base_delay=self.retry_base_delay,
            ) as client:
                logger.info("开始抓取 L1-L3 数据")
                root_node = await client.fetch_tree(code="0", max_level=3)
                divisions = [
                    division
                    for division in root_node.flatten()
                    if division.level in (1, 2, 3)
                ]
                await db.save_divisions(divisions)
                self._record_level1_to_3_stats(divisions)
                logger.info(
                    "L1-L3 完成: provinces=%s cities=%s counties=%s saved=%s",
                    self.stats["provinces"],
                    self.stats["cities"],
                    self.stats["counties"],
                    len(divisions),
                )

                parent_codes = await db.get_parent_codes_for_townships()
                inserted_jobs = await db.ensure_crawl_jobs(parent_codes)
                pending_jobs = await db.list_crawl_jobs(("pending", "failed"))
                logger.info(
                    "L4 任务准备完成: discovered=%s newly_added=%s pending=%s",
                    len(parent_codes),
                    inserted_jobs,
                    len(pending_jobs),
                )
                writer_stats = await self._crawl_level4_jobs(
                    db=db,
                    client=client,
                    parent_codes=pending_jobs,
                )
                self._record_level4_stats(writer_stats)
                await self._record_final_totals(db)
                logger.info(
                    "抓取完成: total=%s l4_saved=%s jobs_ok=%s jobs_failed=%s",
                    self.stats["total"],
                    self.stats["townships"],
                    self.stats["jobs_ok"],
                    self.stats["jobs_failed"],
                )
                return True
        except Exception as exc:
            logger.error(
                "抓取流程失败: error=%s",
                exc,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            return False
        finally:
            self.stats["end_time"] = datetime.now()
            if self.stats["start_time"] and self.stats["end_time"]:
                self.stats["duration"] = (
                    self.stats["end_time"] - self.stats["start_time"]
                )

    async def retry_failed_jobs(self) -> bool:
        """只重试失败或待处理的 L4 任务。

        Returns:
            bool: 是否成功完成补抓。
        """
        self._reset_stats()
        self.stats["start_time"] = datetime.now()
        logger.info(
            "开始补抓失败任务: db=%s concurrency=%s timeout=%ss",
            self.db_path,
            self.max_concurrency,
            self.request_timeout,
        )

        try:
            async with Database(self.db_path) as db, XzqhClient(
                base_url=self.base_url,
                request_timeout=self.request_timeout,
                max_concurrency=self.max_concurrency,
                retry_attempts=self.retry_attempts,
                retry_base_delay=self.retry_base_delay,
            ) as client:
                pending_jobs = await db.list_crawl_jobs(("pending", "failed"))
                logger.info("待补抓 L4 任务: %s", len(pending_jobs))
                writer_stats = await self._crawl_level4_jobs(
                    db=db,
                    client=client,
                    parent_codes=pending_jobs,
                )
                self._record_level4_stats(writer_stats)
                await self._record_final_totals(db)
                logger.info(
                    "补抓完成: total=%s l4_saved=%s jobs_ok=%s jobs_failed=%s",
                    self.stats["total"],
                    self.stats["townships"],
                    self.stats["jobs_ok"],
                    self.stats["jobs_failed"],
                )
                return True
        except Exception as exc:
            logger.error(
                "补抓流程失败: error=%s",
                exc,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            return False
        finally:
            self.stats["end_time"] = datetime.now()
            if self.stats["start_time"] and self.stats["end_time"]:
                self.stats["duration"] = (
                    self.stats["end_time"] - self.stats["start_time"]
                )

    async def _crawl_level4_jobs(
        self,
        *,
        db: Database,
        client: XzqhClient,
        parent_codes: list[str],
    ) -> WriterStats:
        """并发抓取 L4 任务。"""
        if not parent_codes:
            logger.info("没有待处理的 L4 任务")
            return WriterStats(saved_divisions=0, ok_jobs=0, failed_jobs=0)

        worker_count = min(self.max_concurrency, len(parent_codes))
        logger.info(
            "开始抓取 L4 数据: jobs=%s concurrency=%s",
            len(parent_codes),
            worker_count,
        )

        fetch_queue: asyncio.Queue[str | None] = asyncio.Queue()
        write_queue: asyncio.Queue[Level4FetchResult | None] = asyncio.Queue()

        for parent_code in parent_codes:
            await fetch_queue.put(parent_code)
        for _ in range(worker_count):
            await fetch_queue.put(None)

        writer_task = asyncio.create_task(
            self._writer_loop(
                db=db,
                write_queue=write_queue,
                total_jobs=len(parent_codes),
            ),
        )
        worker_tasks = [
            asyncio.create_task(
                self._fetch_worker(
                    fetch_queue=fetch_queue,
                    write_queue=write_queue,
                    client=client,
                ),
            )
            for _ in range(worker_count)
        ]

        await asyncio.gather(*worker_tasks)
        await write_queue.put(None)
        return await writer_task

    async def _fetch_worker(
        self,
        *,
        fetch_queue: asyncio.Queue[str | None],
        write_queue: asyncio.Queue[Level4FetchResult | None],
        client: XzqhClient,
    ) -> None:
        """抓取 worker 协程。"""
        while True:
            parent_code = await fetch_queue.get()
            if parent_code is None:
                return

            result = await self._fetch_single_level4_job(client, parent_code)
            await write_queue.put(result)

    async def _fetch_single_level4_job(
        self,
        client: XzqhClient,
        parent_code: str,
    ) -> Level4FetchResult:
        """抓取单个 L4 任务。"""
        try:
            tree_node = await client.fetch_township_tree(parent_code)
        except Exception as exc:
            logger.error(
                "抓取 L4 任务异常: parent_code=%s error=%s",
                parent_code,
                exc,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            return Level4FetchResult(
                parent_code=parent_code,
                ok=False,
                divisions=(),
                error=str(exc),
            )

        if tree_node is None:
            return Level4FetchResult(
                parent_code=parent_code,
                ok=False,
                divisions=(),
                error="客户端未返回有效树结构",
            )

        divisions = tuple(
            division
            for division in tree_node.flatten()
            if division.level == 4
        )
        return Level4FetchResult(
            parent_code=parent_code,
            ok=True,
            divisions=divisions,
        )

    async def _writer_loop(
        self,
        *,
        db: Database,
        write_queue: asyncio.Queue[Level4FetchResult | None],
        total_jobs: int,
    ) -> WriterStats:
        """串行写库协程。"""
        pending: list[CrawlJobWrite] = []
        saved_divisions = 0
        ok_jobs = 0
        failed_jobs = 0
        completed_jobs = 0
        last_progress_log_at = time.monotonic()

        while True:
            result = await write_queue.get()
            if result is None:
                break

            completed_jobs += 1
            if result.ok:
                ok_jobs += 1
            else:
                failed_jobs += 1

            pending.append(
                CrawlJobWrite(
                    parent_code=result.parent_code,
                    state="ok" if result.ok else "failed",
                    divisions=result.divisions,
                    last_error=result.error,
                ),
            )
            if len(pending) >= self.write_batch_size:
                saved_divisions += await db.apply_crawl_job_writes(tuple(pending))
                pending.clear()
            now = time.monotonic()
            should_log_progress = (
                completed_jobs == 1
                or completed_jobs == total_jobs
                or completed_jobs % PROGRESS_LOG_INTERVAL == 0
                or now - last_progress_log_at >= PROGRESS_LOG_SECONDS
                or result.error is not None
            )
            if should_log_progress:
                logger.info(
                    "L4 进度: %s/%s, ok=%s, failed=%s, saved=%s",
                    completed_jobs,
                    total_jobs,
                    ok_jobs,
                    failed_jobs,
                    saved_divisions,
                )
                last_progress_log_at = now

        if pending:
            saved_divisions += await db.apply_crawl_job_writes(tuple(pending))

        return WriterStats(
            saved_divisions=saved_divisions,
            ok_jobs=ok_jobs,
            failed_jobs=failed_jobs,
        )

    def _record_level1_to_3_stats(
        self,
        divisions: list[AdministrativeDivision],
    ) -> None:
        """统计 L1-L3 数量。"""
        self.stats["provinces"] = sum(1 for item in divisions if item.level == 1)
        self.stats["cities"] = sum(1 for item in divisions if item.level == 2)
        self.stats["counties"] = sum(1 for item in divisions if item.level == 3)

    def _record_level4_stats(self, writer_stats: WriterStats) -> None:
        """统计 L4 数量和任务结果。"""
        self.stats["townships"] = writer_stats.saved_divisions
        self.stats["jobs_ok"] = writer_stats.ok_jobs
        self.stats["jobs_failed"] = writer_stats.failed_jobs

    async def _record_final_totals(self, db: Database) -> None:
        """更新最终总量统计。"""
        db_stats = await db.get_statistics()
        self.stats["total"] = db_stats.get("total", 0)

    def _reset_stats(self) -> None:
        """重置统计信息。"""
        self.stats = {
            "provinces": 0,
            "cities": 0,
            "counties": 0,
            "townships": 0,
            "total": 0,
            "jobs_ok": 0,
            "jobs_failed": 0,
            "start_time": None,
            "end_time": None,
            "duration": None,
        }
