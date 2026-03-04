"""爬虫核心逻辑"""

import logging
import time
from typing import Optional, List, Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from pathlib import Path

from .client import XzqhClient
from .database import Database
from .models import TreeNode, AdministrativeDivision
from .progress import ProgressReporter

logger = logging.getLogger(__name__)


class XzqhCrawler:
    """行政区划爬虫"""
    
    def __init__(
        self,
        db_path: str = "./data/xzqh.db",
        base_url: str = "https://dmfw.mca.gov.cn",
        max_workers: int = 10,
        batch_size: int = 100,
        fetch_townships: bool = True,
        township_batch_delay: float = 2.0,
        township_max_retries: int = 3,
        show_progress: bool = True,
        wait_on_finish: bool = True,
    ):
        """
        初始化爬虫
        
        Args:
            db_path: 数据库文件路径
            base_url: API基础URL
            max_workers: 最大工作线程数（用于获取乡级数据）
            batch_size: 批量处理大小
            fetch_townships: 是否获取乡级数据
            township_batch_delay: 乡级数据批次间延迟（秒）
            township_max_retries: 乡级数据最大重试次数
        """
        self.db_path = db_path
        self.base_url = base_url
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.fetch_townships = fetch_townships
        self.township_batch_delay = township_batch_delay
        self.township_max_retries = township_max_retries
        self.show_progress = show_progress
        self.wait_on_finish = wait_on_finish
        
        self.client: Optional[XzqhClient] = None
        self.db: Optional[Database] = None
        
        # 统计信息
        self.stats = {
            "provinces": 0,
            "cities": 0,
            "counties": 0,
            "townships": 0,
            "total": 0,
            "start_time": None,
            "end_time": None,
            "duration": None,
        }
    
    def _init_components(self):
        """初始化组件"""
        if self.client is None:
            self.client = XzqhClient(base_url=self.base_url)
        
        if self.db is None:
            self.db = Database(db_path=self.db_path)
    
    def fetch_all(self) -> bool:
        """
        获取完整的行政区划数据
        
        Returns:
            是否成功
        """
        logger.info("开始获取完整的行政区划数据")
        self.stats["start_time"] = datetime.now()
        
        try:
            self._init_components()
            
            # 1. 获取1-3级数据（省、地、县）
            if not self._fetch_level1_to_3():
                logger.error("获取1-3级数据失败")
                return False
            
            # 2. 获取4级数据（乡级）- 可选
            if self.fetch_townships:
                if not self._fetch_level4_via_jobs(generate_jobs=True):
                    logger.warning("获取4级数据失败或跳过")
            else:
                logger.info("跳过乡级数据获取")
            
            self.stats["end_time"] = datetime.now()
            self.stats["duration"] = self.stats["end_time"] - self.stats["start_time"]
            logger.info("行政区划数据获取完成")
            return True
            
        except Exception:
            logger.exception("获取数据失败")
            return False
        finally:
            self._cleanup()
    
    def _fetch_level1_to_3(self) -> bool:
        """获取1-3级数据"""
        logger.info("开始获取1-3级数据（省、地、县）")
        
        try:
            # 获取树形数据
            root_node = self.client.get_tree_data(code="0", max_level=3)
            
            # 扁平化处理
            divisions = root_node.flatten()
            
            # 过滤掉根节点（code="00"）
            divisions = [d for d in divisions if d.code != "00"]
            
            # 统计各层级数量
            for division in divisions:
                if division.level == 1:
                    self.stats["provinces"] += 1
                elif division.level == 2:
                    self.stats["cities"] += 1
                elif division.level == 3:
                    self.stats["counties"] += 1
            
            self.stats["total"] = len(divisions)
            
            if not divisions:
                logger.warning("未获取到1-3级数据")
                return False
            
            # 保存到数据库
            saved_count = self.db.save_divisions_batch(divisions)
            
            logger.info(f"1-3级数据获取完成: 获取{len(divisions)}条，保存{saved_count}条")
            logger.info(f"统计: 省{self.stats['provinces']}个, 地{self.stats['cities']}个, 县{self.stats['counties']}个")
            
            return saved_count > 0
            
        except Exception:
            logger.exception("获取1-3级数据失败")
            return False
    
    # 失败任务不落文件：统一写入数据库 xzqh_jobs

    def _resolve_province_name(self, prov_code2: str) -> str:
        """Resolve 2-digit province code to Chinese name from current DB.

        For progress display only. Returns empty string if not found.
        """
        if not self.db:
            return ""
        try:
            div = self.db.get_division(prov_code2)
            return div.name if div else ""
        except Exception:
            return ""

    def _fetch_level4_via_jobs(self, *, generate_jobs: bool) -> bool:
        """获取4级数据（乡级），基于数据库任务表 xzqh_jobs。"""
        logger.info("开始获取4级数据（乡级，jobs模式）")

        try:
            # 1) 生成 jobs（首次全量时）
            if generate_jobs:
                parent_codes = self.db.get_parent_codes_for_townships()
                if not parent_codes:
                    logger.warning("未找到可用于获取乡级数据的父节点，跳过")
                    return True

                inserted = self.db.ensure_level4_jobs(parent_codes)
                logger.info(f"已确保 {len(parent_codes)} 个 level4 job（新插入 {inserted} 个）")

            # 2) 读取待处理 jobs
            job_codes = self.db.list_level4_jobs_by_status(["pending", "failed"])
            if not job_codes:
                logger.info("没有 pending/failed 的 level4 job，跳过")
                return True

            logger.info(f"待处理 level4 job: {len(job_codes)} 个")

            # 3) 并发抓取（每个 job 代表一次 maxLevel=4 请求）
            all_level4_divisions = self._fetch_townships_concurrent(job_codes)
            self.stats["townships"] = len(all_level4_divisions)

            if all_level4_divisions:
                saved_count = self.db.save_divisions_batch(all_level4_divisions)
                logger.info(
                    f"乡级数据保存完成: 获取{len(all_level4_divisions)}条，保存{saved_count}条"
                )
                self.stats["total"] += saved_count
                self._analyze_township_distribution(all_level4_divisions)
            else:
                logger.info("本轮未获取到新的乡级数据")

            return True

        except Exception:
            logger.exception("获取4级数据失败（jobs模式）")
            return False

    def retry_failed_level4_jobs(self) -> bool:
        """仅重试数据库任务表中 failed/pending 的乡级抓取任务。"""
        try:
            self._init_components()
            return self._fetch_level4_via_jobs(generate_jobs=False)
        finally:
            self._cleanup()
    
    def _analyze_township_distribution(self, townships: List[AdministrativeDivision]):
        """分析乡级数据分布"""
        if not townships:
            return
        
        # 按省份统计
        province_stats = {}
        for township in townships:
            # 从代码提取省份代码（前2位）
            province_code = township.code[:2]
            if province_code not in province_stats:
                province_stats[province_code] = 0
            province_stats[province_code] += 1
        
        # 按类型统计
        type_stats = {}
        for township in townships:
            township_type = township.type or "未知"
            if township_type not in type_stats:
                type_stats[township_type] = 0
            type_stats[township_type] += 1
        
        logger.info(f"乡级数据分布统计:")
        logger.info(f"  总乡级数据: {len(townships)} 条")
        
        if province_stats:
            top_provinces = sorted(province_stats.items(), key=lambda x: x[1], reverse=True)[:5]
            logger.info(f"  省份分布（前5）: {dict(top_provinces)}")
        
        if type_stats:
            logger.info(f"  类型分布: {dict(sorted(type_stats.items()))}")
    
    def _fetch_townships_concurrent(self, county_codes: List[str]) -> List[AdministrativeDivision]:
        """并发获取乡级数据"""
        all_township_divisions: List[AdministrativeDivision] = []

        reporter: Optional[ProgressReporter] = None
        live = None
        if self.show_progress:
            reporter = ProgressReporter(
                refresh_per_second=4.0,
                max_workers=self.max_workers,
                province_name_resolver=self._resolve_province_name,
            )
            # queue all upfront (static set); still renders as done/queued.
            for code in county_codes:
                reporter.add_queued(code)
            live = reporter.live()
            live.__enter__()

        try:
            total_batches = (len(county_codes) - 1) // self.batch_size + 1

            # 分批处理，避免内存占用过大和API限制
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(county_codes))
                batch = county_codes[start_idx:end_idx]

                logger.info(
                    f"处理批次 {batch_num + 1}/{total_batches}: {len(batch)}个父节点(县/市)"
                )

                batch_townships = self._fetch_townships_batch(batch, reporter=reporter)
                all_township_divisions.extend(batch_townships)

                # 批次间延迟，避免请求过快触发API限制
                if batch_num + 1 < total_batches:
                    delay_time = self.township_batch_delay
                    logger.debug(f"批次间延迟 {delay_time}秒")
                    time.sleep(delay_time)

            return all_township_divisions
        finally:
            if live is not None:
                # final refresh
                live.update(reporter.render() if reporter else "")

                # Keep the panel on screen until user confirms.
                if self.wait_on_finish:
                    try:
                        # NOTE: with Live(screen=True), printing to console here may be
                        # cleared by the final Live refresh on some terminals. So we also
                        # embed the hint into the panel itself (see reporter.set_footer).
                        if reporter is not None:
                            reporter.set_footer("完成。按 Enter 显示摘要并退出…")
                            live.update(reporter.render(), refresh=True)

                        input()
                    except (EOFError, KeyboardInterrupt):
                        pass

                live.__exit__(None, None, None)

    
    def _fetch_townships_batch(
        self,
        county_codes: List[str],
        *,
        reporter: Optional[ProgressReporter] = None,
    ) -> List[AdministrativeDivision]:
        """批量获取乡级数据（使用线程池）"""
        all_township_divisions: List[AdministrativeDivision] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(self._fetch_township_for_county, code, reporter=reporter): code
                for code in county_codes
            }
            
            # 收集结果
            completed = 0
            success_count = 0
            fail_count = 0
            
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                completed += 1
                
                try:
                    township_divisions = future.result(timeout=30)  # 设置超时
                    
                    if township_divisions:
                        all_township_divisions.extend(township_divisions)
                        success_count += 1
                    else:
                        fail_count += 1
                        logger.debug(f"父节点代码 {code} 没有乡级数据")
                    
                    # 定期报告进度
                    if completed % 5 == 0 or completed == len(county_codes):
                        logger.debug(f"批次进度: {completed}/{len(county_codes)}，成功{success_count}，失败{fail_count}，已获取{len(all_township_divisions)}条")
                        
                except FuturesTimeoutError:
                    logger.warning(f"获取父节点代码 {code} 的乡级数据超时")
                    self.db.mark_level4_job_failed(code, "future_timeout")
                    fail_count += 1
                    if reporter:
                        reporter.mark_done(code, ok=False)
                except Exception as e:
                    logger.exception(f"获取父节点代码 {code} 的乡级数据失败")
                    self.db.mark_level4_job_failed(code, f"future_exception: {e}")
                    fail_count += 1
                    if reporter:
                        reporter.mark_done(code, ok=False)
        
        logger.info(f"批次完成: 处理{len(county_codes)}个父节点(县/市)，成功{success_count}，失败{fail_count}，获取{len(all_township_divisions)}条乡级数据")
        return all_township_divisions
    
    def _fetch_township_for_county(
        self,
        county_code: str,
        *,
        reporter: Optional[ProgressReporter] = None,
    ) -> List[AdministrativeDivision]:
        """获取单个父节点(县/市)的乡级数据（带重试机制）"""
        max_retries = self.township_max_retries
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                # 获取乡级树形数据
                tree_node = self.client.get_township_tree(county_code)

                if tree_node is None:
                    if attempt < max_retries - 1:
                        logger.debug(
                            f"父节点代码 {county_code} 获取失败，第{attempt + 1}次重试"
                        )
                        time.sleep(retry_delay)
                        continue
                    # 最终失败：记录到 jobs 表，以便后续补抓
                    self.db.mark_level4_job_failed(county_code, "client_returned_none")
                    if reporter:
                        reporter.mark_done(county_code, ok=False)
                    return []

                # 扁平化处理
                divisions = tree_node.flatten()

                # 只保留乡级数据（level=4）
                township_divisions = [d for d in divisions if d.level == 4]

                # 请求成功（即便没有 L4 children，也属于“确实没有”，记为 ok）
                self.db.mark_level4_job_ok(county_code)
                if reporter:
                    reporter.mark_done(county_code, ok=True)
                    reporter.add_level4_nodes(county_code, len(township_divisions))

                if township_divisions:
                    logger.debug(
                        f"父节点代码 {county_code}: 获取{len(township_divisions)}条乡级数据"
                    )
                else:
                    logger.debug(f"父节点代码 {county_code}: 没有乡级数据")

                return township_divisions

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"父节点代码 {county_code} 获取失败（第{attempt + 1}次）: {e}，{retry_delay}秒后重试"
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(f"父节点代码 {county_code} 获取失败（最终）: {e}")
                    self.db.mark_level4_job_failed(county_code, f"exception_final: {e}")
                    if reporter:
                        reporter.mark_done(county_code, ok=False)

        return []
    
    
    def _cleanup(self):
        """清理资源"""
        try:
            if self.client:
                self.client.close()
                self.client = None
            
            if self.db:
                self.db.close()
                self.db = None
                
        except Exception as e:
            logger.warning(f"清理资源时发生错误: {e}")
    
    def __enter__(self):
        self._init_components()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

