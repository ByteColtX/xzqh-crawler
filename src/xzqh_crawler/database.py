"""异步数据库访问模块。"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

from .models import AdministrativeDivision

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class CrawlJobWrite:
    """单个抓取任务的落库结果。"""

    parent_code: str
    state: str
    divisions: tuple[AdministrativeDivision, ...] = ()
    last_error: str | None = None


class Database:
    """SQLite 异步访问封装。"""

    def __init__(self, db_path: str = "./data/xzqh.db") -> None:
        """初始化数据库对象。

        Args:
            db_path: SQLite 文件路径。
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> Database:
        """进入异步上下文。"""
        await self.open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, exc_tb: object) -> None:
        """退出异步上下文。"""
        await self.close()

    async def open(self) -> None:
        """打开数据库连接并初始化表结构。"""
        if self._conn is not None:
            return

        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._conn.execute("PRAGMA synchronous = NORMAL")
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS divisions (
                code TEXT PRIMARY KEY,
                name TEXT,
                level INTEGER NOT NULL,
                type TEXT,
                parent_code TEXT,
                parent_name TEXT,
                name_path TEXT,
                fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_divisions_level
            ON divisions(level);

            CREATE INDEX IF NOT EXISTS idx_divisions_parent_code
            ON divisions(parent_code);

            CREATE TABLE IF NOT EXISTS crawl_jobs (
                parent_code TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_crawl_jobs_state
            ON crawl_jobs(state);
            """,
        )
        await self._conn.commit()

    async def close(self) -> None:
        """关闭数据库连接。"""
        if self._conn is None:
            return
        await self._conn.close()
        self._conn = None

    async def save_divisions(self, divisions: Sequence[AdministrativeDivision]) -> int:
        """批量写入行政区划记录。

        Args:
            divisions: 待保存的行政区划记录列表。

        Returns:
            int: 实际提交的记录数。
        """
        if not divisions:
            return 0

        filtered_divisions = self._filter_numeric_code_divisions(divisions)
        if not filtered_divisions:
            return 0

        conn = self._require_connection()
        rows = [
            (
                division.code,
                division.name,
                division.level,
                division.type,
                division.parent_code,
                division.parent_name,
                division.name_path,
            )
            for division in filtered_divisions
        ]
        await conn.executemany(
            """
            INSERT INTO divisions (
                code,
                name,
                level,
                type,
                parent_code,
                parent_name,
                name_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name,
                level = excluded.level,
                type = excluded.type,
                parent_code = excluded.parent_code,
                parent_name = excluded.parent_name,
                name_path = excluded.name_path,
                fetched_at = CURRENT_TIMESTAMP
            """,
            rows,
        )
        await conn.commit()
        return len(rows)

    async def get_division(self, code: str) -> AdministrativeDivision | None:
        """根据代码查询单条行政区划记录。"""
        conn = self._require_connection()
        async with conn.execute(
            "SELECT * FROM divisions WHERE code = ?",
            (code,),
        ) as cursor:
            row = await cursor.fetchone()
        return self._row_to_division(row) if row else None

    async def get_parent_codes_for_townships(self) -> list[str]:
        """查询需要补抓 L4 的父节点代码。"""
        conn = self._require_connection()
        async with conn.execute(
            """
            SELECT code
            FROM divisions
            WHERE level IN (2, 3)
              AND parent_code IN (SELECT code FROM divisions WHERE level = 1)
            ORDER BY code
            """,
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row["code"]) for row in rows]

    async def ensure_crawl_jobs(self, parent_codes: Sequence[str]) -> int:
        """确保抓取任务存在。"""
        if not parent_codes:
            return 0

        conn = self._require_connection()
        before = conn.total_changes
        await conn.executemany(
            """
            INSERT OR IGNORE INTO crawl_jobs (
                parent_code,
                state,
                retry_count,
                last_error,
                updated_at
            ) VALUES (?, 'pending', 0, NULL, CURRENT_TIMESTAMP)
            """,
            [(code,) for code in parent_codes],
        )
        await conn.commit()
        return conn.total_changes - before

    async def list_crawl_jobs(self, states: Sequence[str]) -> list[str]:
        """按状态列出待抓取任务。"""
        if not states:
            return []

        placeholders = ",".join("?" for _ in states)
        conn = self._require_connection()
        async with conn.execute(
            f"""
            SELECT parent_code
            FROM crawl_jobs
            WHERE state IN ({placeholders})
            ORDER BY parent_code
            """,
            list(states),
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row["parent_code"]) for row in rows]

    async def apply_crawl_job_writes(self, writes: Sequence[CrawlJobWrite]) -> int:
        """批量提交抓取结果。

        Args:
            writes: 已完成的抓取任务结果。

        Returns:
            int: 写入到 `divisions` 表中的记录数。
        """
        if not writes:
            return 0

        conn = self._require_connection()
        division_rows: list[tuple[str, str | None, int, str | None, str | None, str | None, str | None]] = []
        ok_codes: list[tuple[str]] = []
        failed_rows: list[tuple[str | None, str]] = []

        for write in writes:
            if write.state == "ok":
                ok_codes.append((write.parent_code,))
                division_rows.extend(
                    (
                        division.code,
                        division.name,
                        division.level,
                        division.type,
                        division.parent_code,
                        division.parent_name,
                        division.name_path,
                    )
                    for division in self._filter_numeric_code_divisions(write.divisions)
                )
            else:
                failed_rows.append(
                    ((write.last_error or "未知错误")[:500], write.parent_code),
                )

        await conn.execute("BEGIN")
        try:
            if division_rows:
                await conn.executemany(
                    """
                    INSERT INTO divisions (
                        code,
                        name,
                        level,
                        type,
                        parent_code,
                        parent_name,
                        name_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                        name = excluded.name,
                        level = excluded.level,
                        type = excluded.type,
                        parent_code = excluded.parent_code,
                        parent_name = excluded.parent_name,
                        name_path = excluded.name_path,
                        fetched_at = CURRENT_TIMESTAMP
                    """,
                    division_rows,
                )

            if ok_codes:
                await conn.executemany(
                    """
                    UPDATE crawl_jobs
                    SET state = 'ok',
                        last_error = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE parent_code = ?
                    """,
                    ok_codes,
                )

            if failed_rows:
                await conn.executemany(
                    """
                    UPDATE crawl_jobs
                    SET state = 'failed',
                        retry_count = retry_count + 1,
                        last_error = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE parent_code = ?
                    """,
                    failed_rows,
                )

            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

        return len(division_rows)

    async def get_statistics(self) -> dict[str, int]:
        """返回分层统计信息。"""
        conn = self._require_connection()
        stats: dict[str, int] = {}
        async with conn.execute(
            """
            SELECT level, COUNT(*) AS count
            FROM divisions
            GROUP BY level
            ORDER BY level
            """,
        ) as cursor:
            async for row in cursor:
                stats[f"level_{row['level']}"] = int(row["count"])

        async with conn.execute("SELECT COUNT(*) AS total FROM divisions") as cursor:
            row = await cursor.fetchone()
        stats["total"] = int(row["total"]) if row else 0
        return stats

    async def get_crawl_job_counts(self) -> dict[str, int]:
        """返回任务状态统计。"""
        conn = self._require_connection()
        counts = {"pending": 0, "ok": 0, "failed": 0}
        async with conn.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM crawl_jobs
            GROUP BY state
            """,
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            counts[str(row["state"])] = int(row["count"])
        return counts

    def _require_connection(self) -> aiosqlite.Connection:
        """返回已建立的连接。"""
        if self._conn is None:
            raise RuntimeError("数据库尚未打开")
        return self._conn

    def _row_to_division(self, row: aiosqlite.Row) -> AdministrativeDivision:
        """把查询结果转换成数据模型。"""
        return AdministrativeDivision(
            code=str(row["code"]),
            name=row["name"],
            level=int(row["level"]),
            type=row["type"],
            parent_code=row["parent_code"],
            parent_name=row["parent_name"],
            name_path=row["name_path"],
        )

    def _filter_numeric_code_divisions(
        self,
        divisions: Sequence[AdministrativeDivision],
    ) -> list[AdministrativeDivision]:
        """过滤 `code` 非数字的脏数据。

        Args:
            divisions: 待落库的行政区划记录。

        Returns:
            list[AdministrativeDivision]: 仅包含数字代码的记录。
        """
        valid_divisions: list[AdministrativeDivision] = []
        for division in divisions:
            if division.code and division.code.isdigit():
                valid_divisions.append(division)
                continue
            logger.debug(
                "跳过非数字 code 的行政区划记录: code=%r name=%r",
                division.code,
                division.name,
            )
        return valid_divisions
