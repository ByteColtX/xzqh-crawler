"""数据库操作模块"""

import sqlite3
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path

from .models import AdministrativeDivision

logger = logging.getLogger(__name__)


class Database:
    """数据库操作类"""

    def __init__(self, db_path: str = "./data/xzqh.db"):
        """初始化数据库

        Args:
            db_path: 数据库文件路径
        """
        import threading

        # SQLite connection is shared by multiple worker threads in this project.
        # Serialize access to avoid races.
        self._lock = threading.RLock()

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn: Optional[sqlite3.Connection] = None
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # Allow using the same connection object across worker threads.
            # We still need to ensure DB operations are serialized at a higher level.
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            
            # 创建表
            self._create_tables()
            logger.info(f"数据库初始化完成: {self.db_path}")
            
        except sqlite3.Error as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def _create_tables(self):
        """创建数据库表"""
        with self._lock:
            cursor = self.conn.cursor()

            # 创建行政区划主表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS xzqh (
                    code TEXT PRIMARY KEY,
                    name TEXT,
                    level INTEGER NOT NULL,
                    type TEXT,
                    parent_code TEXT,
                    parent_name TEXT,
                    name_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS xzqh_jobs (
                    code TEXT PRIMARY KEY,
                    max_level INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    try_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_xzqh_jobs_status ON xzqh_jobs(status)")

            # short-code compatibility (older DBs may already exist)
            cursor.execute("PRAGMA foreign_keys = ON")

            # 创建数据版本表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_code VARCHAR(20) UNIQUE,
                    data_year INTEGER,
                    record_count INTEGER,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 创建索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_xzqh_level ON xzqh(level)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_xzqh_parent_code ON xzqh(parent_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_xzqh_name ON xzqh(name)")

            self.conn.commit()
            logger.debug("数据库表创建完成")
    
    def save_division(self, division: AdministrativeDivision) -> bool:
        """
        保存或更新行政区划数据
        
        Args:
            division: 行政区划对象
            
        Returns:
            是否成功
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()
            
            # 检查是否已存在
            cursor.execute("SELECT 1 FROM xzqh WHERE code = ?", (division.code,))
            exists = cursor.fetchone() is not None
            
            if exists:
                # 更新数据
                cursor.execute("""
                    UPDATE xzqh SET
                        name = ?,
                        level = ?,
                        type = ?,
                        parent_code = ?,
                        parent_name = ?,
                        name_path = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE code = ?
                """, (
                    division.name,
                    division.level,
                    division.type,
                    division.parent_code,
                    division.parent_name,
                    division.name_path,
                    division.code,
                ))
            else:
                # 插入新数据
                cursor.execute("""
                    INSERT INTO xzqh (
                        code, name, level, type,
                        parent_code, parent_name, name_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    division.code,
                    division.name,
                    division.level,
                    division.type,
                    division.parent_code,
                    division.parent_name,
                    division.name_path,
                ))
            
            with self._lock:
                self.conn.commit()
            logger.debug(f"{'更新' if exists else '插入'}行政区划数据: {division.code} - {division.name}")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"保存行政区划数据失败: {e}, 数据: {division}")
            with self._lock:
                self.conn.rollback()
            return False
    
    def save_divisions_batch(self, divisions: List[AdministrativeDivision]) -> int:
        """
        批量保存行政区划数据
        
        Args:
            divisions: 行政区划对象列表
            
        Returns:
            成功保存的数量
        """
        if not divisions:
            return 0
        
        try:
            with self._lock:
                cursor = self.conn.cursor()
                success_count = 0
            
            for division in divisions:
                try:
                    # 使用UPSERT语法（SQLite 3.24+）
                    cursor.execute("""
                        INSERT INTO xzqh (
                            code, name, level, type,
                            parent_code, parent_name, name_path
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(code) DO UPDATE SET
                            name = excluded.name,
                            level = excluded.level,
                            type = excluded.type,
                            parent_code = excluded.parent_code,
                            parent_name = excluded.parent_name,
                            name_path = excluded.name_path,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        division.code,
                        division.name,
                        division.level,
                        division.type,
                        division.parent_code,
                        division.parent_name,
                        division.name_path,
                    ))
                    success_count += 1
                    
                except sqlite3.Error as e:
                    logger.warning(f"批量保存单个数据失败: {e}, 代码: {division.code}")
                    continue
            
            with self._lock:
                self.conn.commit()
            logger.info(f"批量保存完成: {success_count}/{len(divisions)} 条数据")
            return success_count
            
        except sqlite3.Error as e:
            logger.error(f"批量保存数据失败: {e}")
            with self._lock:
                self.conn.rollback()
            return 0
    
    def get_division(self, code: str) -> Optional[AdministrativeDivision]:
        """
        根据代码获取行政区划数据
        
        Args:
            code: 行政区划代码
            
        Returns:
            行政区划对象，如果不存在则返回None
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM xzqh WHERE code = ?", (code,))
                row = cursor.fetchone()
            
            if row:
                return self._row_to_division(row)
            return None
            
        except sqlite3.Error as e:
            logger.error(f"查询行政区划数据失败: {e}, 代码: {code}")
            return None
    
    def get_divisions_by_level(self, level: int) -> List[AdministrativeDivision]:
        """
        根据层级获取行政区划数据
        
        Args:
            level: 层级 (1-4)
            
        Returns:
            行政区划对象列表
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM xzqh WHERE level = ? ORDER BY code", (level,))
                rows = cursor.fetchall()

            return [self._row_to_division(row) for row in rows]
            
        except sqlite3.Error as e:
            logger.error(f"查询层级数据失败: {e}, 层级: {level}")
            return []
    
    def get_county_codes(self) -> List[str]:
        """获取所有县级行政区划代码（兼容旧逻辑）"""
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute("SELECT code FROM xzqh WHERE level = 3 ORDER BY code")
                rows = cursor.fetchall()
            return [row["code"] for row in rows]
        except sqlite3.Error as e:
            logger.error(f"获取县级代码失败: {e}")
            return []

    def get_parent_codes_for_townships(self) -> List[str]:
        """获取需要请求 maxLevel=4 的父节点 code（适配新接口语义）。

        规则：对每个省(L1)的直接子节点：
        - 如果子节点是 L2：请求该 L2 一次（覆盖其下全部 L3+L4）
        - 如果子节点是 L3：请求该 L3 一次（覆盖其下 L4；用于直辖市/省直管县级市等混合结构）

        这样可正确处理海南这种 L1 下既有 L2 又有 L3 的情况。
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    SELECT code
                    FROM xzqh
                    WHERE level IN (2, 3)
                      AND parent_code IN (SELECT code FROM xzqh WHERE level = 1)
                    ORDER BY code
                    """
                )
                rows = cursor.fetchall()
            return [row["code"] for row in rows]
        except sqlite3.Error as e:
            logger.error(f"获取乡级父节点代码失败: {e}")
            return []

    def ensure_level4_jobs(self, codes: List[str]) -> int:
        """确保给定 codes 都在 jobs 表中（max_level 固定为 4）。

        新增的 job 状态为 pending；已存在则忽略。

        Returns:
            新插入的 job 数量（best-effort）
        """
        if not codes:
            return 0

        now = datetime.now().isoformat(timespec="seconds")
        rows = [(str(code), 4, "pending", 0, None, now, now) for code in codes]

        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO xzqh_jobs
                      (code, max_level, status, try_count, last_error, updated_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                inserted = cursor.rowcount
                self.conn.commit()
            return max(inserted, 0)
        except sqlite3.Error as e:
            logger.error(f"创建 level4 jobs 失败: {e}")
            with self._lock:
                self.conn.rollback()
            return 0

    def list_level4_jobs_by_status(self, statuses: List[str]) -> List[str]:
        """按状态列出 jobs code（max_level=4）。"""
        if not statuses:
            return []
        try:
            with self._lock:
                cursor = self.conn.cursor()
                qs = ",".join(["?"] * len(statuses))
                cursor.execute(
                    f"""
                    SELECT code FROM xzqh_jobs
                    WHERE max_level=4 AND status IN ({qs})
                    ORDER BY code
                    """,
                    statuses,
                )
                rows = cursor.fetchall()
            return [row["code"] for row in rows]
        except sqlite3.Error as e:
            logger.error(f"查询 level4 jobs 失败: {e}")
            return []

    def mark_level4_job_ok(self, code: str) -> None:
        """将 job 标记为 ok。"""
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    UPDATE xzqh_jobs
                    SET status='ok', updated_at=CURRENT_TIMESTAMP
                    WHERE code=? AND max_level=4
                    """,
                    (str(code),),
                )
                self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"更新 job ok 失败: {e}")
            with self._lock:
                self.conn.rollback()

    def mark_level4_job_failed(self, code: str, error: str) -> None:
        """将 job 标记为 failed，try_count+1，记录 last_error。"""
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    UPDATE xzqh_jobs
                    SET status='failed',
                        try_count=try_count+1,
                        last_error=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE code=? AND max_level=4
                    """,
                    (error[:500], str(code)),
                )
                self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"更新 job failed 失败: {e}")
            with self._lock:
                self.conn.rollback()
    
    def get_statistics(self) -> Dict[str, int]:
        """
        获取数据统计信息
        
        Returns:
            统计信息字典
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()
                stats = {}

                # 按层级统计
                cursor.execute("""
                    SELECT level, COUNT(*) as count 
                    FROM xzqh 
                    GROUP BY level 
                    ORDER BY level
                """)
                for row in cursor.fetchall():
                    stats[f"level_{row['level']}"] = row["count"]

                # 总记录数
                cursor.execute("SELECT COUNT(*) as total FROM xzqh")
                stats["total"] = cursor.fetchone()["total"]

                return stats
            
        except sqlite3.Error as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}
    
    def save_version_info(
        self, 
        version_code: str, 
        data_year: int, 
        record_count: int
    ) -> bool:
        """
        保存数据版本信息
        
        Args:
            version_code: 版本标识
            data_year: 数据年份
            record_count: 记录数
            
        Returns:
            是否成功
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()

                cursor.execute("""
                    INSERT INTO data_versions (version_code, data_year, record_count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(version_code) DO UPDATE SET
                        data_year = excluded.data_year,
                        record_count = excluded.record_count,
                        fetched_at = CURRENT_TIMESTAMP
                """, (version_code, data_year, record_count))

                self.conn.commit()

            logger.info(f"保存版本信息: {version_code}, 记录数: {record_count}")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"保存版本信息失败: {e}")
            with self._lock:
                self.conn.rollback()
            return False
    
    def get_latest_version(self) -> Optional[Dict[str, Any]]:
        """
        获取最新版本信息
        
        Returns:
            版本信息字典，如果不存在则返回None
        """
        try:
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT * FROM data_versions 
                    ORDER BY fetched_at DESC 
                    LIMIT 1
                """)
                row = cursor.fetchone()

            if row:
                return dict(row)
            return None
            
        except sqlite3.Error as e:
            logger.error(f"获取最新版本失败: {e}")
            return None
    
    def _row_to_division(self, row: sqlite3.Row) -> AdministrativeDivision:
        """将数据库行转换为行政区划对象"""
        return AdministrativeDivision(
            code=row["code"],
            name=row["name"],
            level=row["level"],
            type=row["type"],
            parent_code=row["parent_code"],
            parent_name=row["parent_name"],
            name_path=row["name_path"],
        )
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.debug("数据库连接已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()