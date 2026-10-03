"""异步 SQLite 持久化。"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from .models import Division, DivisionLevel, TaskState

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TaskWrite:
    parent_code: str
    target_level: DivisionLevel
    state: TaskState
    divisions: tuple[Division, ...] = ()
    last_error: str | None = None


class Database:
    def __init__(self, db_path: str = "./data/xzqh.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: aiosqlite.Connection | None = None
        self.run_id: str | None = None

    async def __aenter__(self) -> Database:
        await self.open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, exc_tb: object) -> None:
        await self.close()

    async def open(self) -> None:
        if self._conn is not None:
            return
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._conn.execute("PRAGMA synchronous = NORMAL")
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS divisions (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                level INTEGER NOT NULL CHECK (level BETWEEN 1 AND 4),
                kind TEXT,
                parent_code TEXT REFERENCES divisions(code),
                source_version TEXT,
                fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_divisions_parent_level
                ON divisions(parent_code, level);
            CREATE INDEX IF NOT EXISTS idx_divisions_level
                ON divisions(level);

            CREATE TABLE IF NOT EXISTS crawl_runs (
                id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                source_version TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                stats_json TEXT
            );

            CREATE TABLE IF NOT EXISTS crawl_tasks (
                run_id TEXT NOT NULL REFERENCES crawl_runs(id),
                parent_code TEXT NOT NULL,
                target_level INTEGER NOT NULL CHECK (target_level BETWEEN 1 AND 4),
                state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                started_at TEXT,
                lease_until TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, parent_code, target_level)
            );
            CREATE INDEX IF NOT EXISTS idx_crawl_tasks_state
                ON crawl_tasks(run_id, state);

            CREATE TABLE IF NOT EXISTS division_rejections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                raw_code TEXT,
                raw_payload TEXT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """,
        )
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def start_run(self, mode: str = "full", source_version: str | None = None) -> str:
        conn = self._require_connection()
        previous_run_id: str | None = None
        if mode == "resume":
            async with conn.execute(
                "SELECT id FROM crawl_runs WHERE id <> ? ORDER BY started_at DESC LIMIT 1",
                (self.run_id or "",),
            ) as cursor:
                row = await cursor.fetchone()
            previous_run_id = str(row["id"]) if row else None
        self.run_id = uuid.uuid4().hex
        await conn.execute(
            "INSERT INTO crawl_runs(id, mode, source_version, status, started_at) VALUES (?, ?, ?, 'running', ?)",
            (self.run_id, mode, source_version, datetime.now(UTC).isoformat()),
        )
        if mode == "resume" and previous_run_id:
            await conn.execute(
                """
                INSERT INTO crawl_tasks(run_id, parent_code, target_level, state, attempts, last_error)
                SELECT ?, parent_code, target_level, state, attempts, last_error
                FROM crawl_tasks
                WHERE run_id = ? AND state IN ('pending', 'failed')
                """,
                (self.run_id, previous_run_id),
            )
        await conn.commit()
        return self.run_id

    async def finish_run(self, *, status: str, stats: object | None = None) -> None:
        if self.run_id is None:
            return
        conn = self._require_connection()
        payload = json.dumps(stats, default=str, ensure_ascii=False) if stats is not None else None
        await conn.execute(
            "UPDATE crawl_runs SET status = ?, finished_at = ?, stats_json = ? WHERE id = ?",
            (status, datetime.now(UTC).isoformat(), payload, self.run_id),
        )
        await conn.commit()

    async def save_divisions(self, divisions: Sequence[Division]) -> int:
        conn = self._require_connection()
        await conn.execute("BEGIN")
        try:
            saved = await self._upsert_divisions(divisions)
            await conn.commit()
            return saved
        except Exception:
            await conn.rollback()
            raise

    async def _upsert_divisions(self, divisions: Sequence[Division]) -> int:
        conn = self._require_connection()
        valid: list[Division] = []
        for division in divisions:
            if division.validate():
                valid.append(division)
            else:
                await conn.execute(
                    "INSERT INTO division_rejections(run_id, raw_code, raw_payload, reason) VALUES (?, ?, ?, ?)",
                    (self.run_id, division.code, None, "代码长度与层级不匹配"),
                )
        if not valid:
            return 0
        await conn.executemany(
            """
            INSERT INTO divisions(code, name, level, kind, parent_code, source_version)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name, level = excluded.level, kind = excluded.kind,
                parent_code = excluded.parent_code, source_version = excluded.source_version,
                fetched_at = CURRENT_TIMESTAMP
            """,
            [(d.code, d.name, int(d.level), d.kind, d.parent_code, d.source_version) for d in valid],
        )
        return len(valid)

    async def get_division(self, code: str) -> Division | None:
        conn = self._require_connection()
        async with conn.execute("SELECT * FROM divisions WHERE code = ?", (code,)) as cursor:
            row = await cursor.fetchone()
        return self._row_to_division(row) if row else None

    async def get_task_parent_codes(self, target_level: DivisionLevel = DivisionLevel.TOWNSHIP) -> list[str]:
        conn = self._require_connection()
        parent_levels = (2, 3) if target_level is DivisionLevel.TOWNSHIP else (int(target_level) - 1,)
        placeholders = ",".join("?" for _ in parent_levels)
        async with conn.execute(
            f"SELECT code FROM divisions WHERE level IN ({placeholders}) ORDER BY code",
            parent_levels,
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row["code"]) for row in rows]

    async def ensure_tasks(self, parent_codes: Sequence[str], target_level: DivisionLevel = DivisionLevel.TOWNSHIP) -> int:
        if not parent_codes:
            return 0
        if self.run_id is None:
            await self.start_run(mode="resume")
        conn = self._require_connection()
        before = conn.total_changes
        await conn.executemany(
            "INSERT OR IGNORE INTO crawl_tasks(run_id, parent_code, target_level, state) VALUES (?, ?, ?, 'pending')",
            [(self.run_id, code, int(target_level)) for code in parent_codes],
        )
        await conn.commit()
        return conn.total_changes - before

    async def list_tasks(self, states: Sequence[TaskState]) -> list[str]:
        if not states or self.run_id is None:
            return []
        placeholders = ",".join("?" for _ in states)
        conn = self._require_connection()
        async with conn.execute(
            f"SELECT parent_code FROM crawl_tasks WHERE run_id = ? AND state IN ({placeholders}) ORDER BY parent_code",
            [self.run_id, *(str(state) for state in states)],
        ) as cursor:
            rows = await cursor.fetchall()
        return [str(row["parent_code"]) for row in rows]

    async def apply_task_write(self, write: TaskWrite) -> int:
        """在一个独立事务中写入一个任务结果。"""
        conn = self._require_connection()
        await conn.execute("BEGIN")
        try:
            saved = 0
            if write.state is TaskState.SUCCEEDED:
                saved += await self._upsert_divisions(write.divisions)
            await conn.execute(
                "UPDATE crawl_tasks SET state = ?, attempts = attempts + 1, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE run_id = ? AND parent_code = ? AND target_level = ?",
                (str(write.state), write.last_error, self.run_id, write.parent_code, int(write.target_level)),
            )
            await conn.commit()
            return saved
        except Exception:
            await conn.rollback()
            raise

    async def apply_task_writes(self, writes: Sequence[TaskWrite]) -> int:
        """逐任务提交结果；每个任务使用独立事务。"""
        saved = 0
        for write in writes:
            saved += await self.apply_task_write(write)
        return saved

    async def get_statistics(self) -> dict[str, int]:
        conn = self._require_connection()
        stats: dict[str, int] = {}
        async with conn.execute("SELECT level, COUNT(*) AS count FROM divisions GROUP BY level ORDER BY level") as cursor:
            async for row in cursor:
                stats[f"level_{row['level']}"] = int(row["count"])
        async with conn.execute("SELECT COUNT(*) AS total FROM divisions") as cursor:
            row = await cursor.fetchone()
        stats["total"] = int(row["total"]) if row else 0
        return stats

    async def get_task_counts(self) -> dict[str, int]:
        counts = {state.value: 0 for state in TaskState}
        conn = self._require_connection()
        run_id = self.run_id
        if run_id is None:
            async with conn.execute("SELECT id FROM crawl_runs ORDER BY started_at DESC LIMIT 1") as cursor:
                row = await cursor.fetchone()
            run_id = str(row["id"]) if row else None
        if run_id is None:
            return counts
        async with conn.execute("SELECT state, COUNT(*) AS count FROM crawl_tasks WHERE run_id = ? GROUP BY state", (run_id,)) as cursor:
            async for row in cursor:
                counts[str(row["state"])] = int(row["count"])
        return counts

    def _require_connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("数据库尚未打开")
        return self._conn

    def _row_to_division(self, row: aiosqlite.Row) -> Division:
        return Division(code=str(row["code"]), name=str(row["name"]), level=int(row["level"]), kind=row["kind"], parent_code=row["parent_code"], source_version=row["source_version"])
