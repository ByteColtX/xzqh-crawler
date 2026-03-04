"""主入口文件"""

import sys
import argparse
from typing import Optional

from .config import get_config, create_default_config
from .utils import setup_logging
from .crawler import XzqhCrawler


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="行政区划代码爬虫")
    parser.add_argument(
        "--config",
        help="配置文件路径",
    )
    parser.add_argument(
        "--db-path",
        help="数据库文件路径",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="最大工作线程数",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="日志级别",
    )
    parser.add_argument(
        "--log-file",
        help="日志文件路径",
    )
    parser.add_argument(
        "--create-config",
        action="store_true",
        help="创建默认配置文件",
    )
    parser.add_argument(
        "--progress",
        dest="progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="抓取乡级数据时显示实时进度面板（rich）",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="仅重试数据库任务表中 failed/pending 的乡级抓取任务（不重新抓 1-3 级、不重新生成任务）",
    )
    
    args = parser.parse_args()
    
    # 如果指定了创建配置文件，则创建并退出
    if args.create_config:
        create_default_config()
        print("默认配置文件已创建: config.toml")
        return
    
    # 加载配置
    config = get_config(args.config)
    
    # 覆盖命令行参数
    if args.db_path:
        config.db_path = args.db_path
    if args.max_workers:
        config.max_workers = args.max_workers
    if args.log_level:
        config.log_level = args.log_level
    if args.log_file:
        config.log_file = args.log_file
    
    # 设置日志
    # 当开启 rich 进度面板时，控制台日志会破坏 Live 刷新区域，导致面板被“刷掉”。
    # 因此默认在 progress=true 时关闭控制台日志，仅写入 --log-file。
    setup_logging(
        level=config.log_level,
        log_file=config.log_file,
        console=not args.progress,
    )
    
    # 创建并运行爬虫
    crawler = XzqhCrawler(
        db_path=config.db_path,
        base_url=config.base_url,
        max_workers=config.max_workers,
        batch_size=config.batch_size,
        show_progress=args.progress,
        wait_on_finish=args.progress,
    )
    
    if args.retry_failed:
        success = crawler.retry_failed_level4_jobs()
    else:
        success = crawler.fetch_all()
    
    if success:
        print("\n✅ 数据获取完成！")

        # progress 模式下默认关闭 console logger（仅写入 --log-file），
        # 因此这里强制使用 stdout 输出一个最小统计摘要，确保用户能看见。
        try:
            from collections import Counter
            from .database import Database

            db = Database(db_path=config.db_path)
            stats = db.get_statistics() or {}

            # 仅为了计数，分别按 status 查询（避免改动 database API）
            job_status_counts: Counter[str] = Counter()
            job_status_counts["ok"] = len(db.list_level4_jobs_by_status(["ok"]))
            job_status_counts["failed"] = len(db.list_level4_jobs_by_status(["failed"]))
            job_status_counts["pending"] = len(db.list_level4_jobs_by_status(["pending"]))

            level_counts = {
                int(k.split("_")[1]): v for k, v in stats.items() if k.startswith("level_")
            }

            lines = []
            lines.append("=" * 60)
            lines.append("统计摘要")
            lines.append("=" * 60)
            lines.append(f"DB: {config.db_path}")
            lines.append(f"总行数: {stats.get('total', 0)}")
            if level_counts:
                lines.append(f"Level 分布: {dict(sorted(level_counts.items()))}")
            lines.append(
                "Jobs(level4) 状态: "
                + f"ok={job_status_counts['ok']} failed={job_status_counts['failed']} pending={job_status_counts['pending']}"
            )
            lines.append("=" * 60)
            print("\n" + "\n".join(lines))
        except Exception:
            # 摘要失败不影响主流程
            pass

        sys.exit(0)
    else:
        print("\n❌ 数据获取失败！")
        sys.exit(1)


if __name__ == "__main__":
    main()