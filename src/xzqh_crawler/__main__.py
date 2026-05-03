"""命令行入口。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass

from .crawler import XzqhCrawler
from .database import Database
from .utils import setup_logging

DEFAULT_DB_PATH = "./data/xzqh.db"
DEFAULT_BASE_URL = "https://dmfw.mca.gov.cn"
DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_MAX_CONCURRENCY = 20
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_BASE_DELAY = 1.0
DEFAULT_WRITE_BATCH_SIZE = 200


@dataclass(slots=True)
class CliOptions:
    """命令行参数。"""

    db_path: str = DEFAULT_DB_PATH
    base_url: str = DEFAULT_BASE_URL
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY
    write_batch_size: int = DEFAULT_WRITE_BATCH_SIZE
    log_level: str = "INFO"
    log_file: str | None = None


def main() -> None:
    """命令行主入口。"""
    args = _build_parser().parse_args()
    options = _resolve_options(args)

    setup_logging(
        level=options.log_level,
        log_file=options.log_file,
    )

    success = asyncio.run(_run(options, retry_failed=args.resume))
    sys.exit(0 if success else 1)


async def _run(options: CliOptions, *, retry_failed: bool) -> bool:
    """运行爬虫并打印摘要。"""
    crawler = XzqhCrawler(
        db_path=options.db_path,
        base_url=options.base_url,
        request_timeout=options.request_timeout,
        max_concurrency=options.max_concurrency,
        retry_attempts=options.retry_attempts,
        retry_base_delay=options.retry_base_delay,
        write_batch_size=options.write_batch_size,
    )

    success = await (
        crawler.retry_failed_jobs() if retry_failed else crawler.crawl()
    )

    if success:
        print("\n数据获取完成。")
        await _print_summary(options.db_path)
    else:
        print("\n数据获取失败。")
    return success


def _build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="xzqh",
        description="抓取民政部行政区划数据并写入 SQLite。",
        epilog=(
            "示例:\n"
            "  xzqh\n"
            "  xzqh --db ./data/custom.db\n"
            "  xzqh --db ./data/xzqh.db -c 40 -t 20\n"
            "  xzqh --resume"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser._positionals.title = "参数"

    help_group = parser.add_argument_group("帮助")
    help_group.add_argument("-h", "--help", action="help", help="显示帮助并退出")

    run_group = parser.add_argument_group("运行选项")
    run_group.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help=f"SQLite 文件路径，默认 {DEFAULT_DB_PATH}",
    )
    run_group.add_argument(
        "-c",
        "--concurrency",
        dest="max_concurrency",
        type=int,
        metavar="N",
        default=DEFAULT_MAX_CONCURRENCY,
        help=f"最大并发抓取数，默认 {DEFAULT_MAX_CONCURRENCY}",
    )
    run_group.add_argument(
        "-t",
        "--timeout",
        dest="request_timeout",
        type=float,
        metavar="SEC",
        default=DEFAULT_REQUEST_TIMEOUT,
        help=f"单请求总超时时间（秒），默认 {DEFAULT_REQUEST_TIMEOUT}",
    )
    run_group.add_argument(
        "-r",
        "--resume",
        action="store_true",
        help="只处理 pending/failed 的任务",
    )
    debug_group = parser.add_argument_group("调试选项")
    debug_group.add_argument("-d", "--debug", action="store_true", help="输出调试日志")
    debug_group.add_argument("-l", "--log", dest="log_file", metavar="FILE", help="日志文件路径")
    debug_group.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=argparse.SUPPRESS,
    )
    debug_group.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_RETRY_ATTEMPTS,
        help=argparse.SUPPRESS,
    )
    debug_group.add_argument(
        "--retry-base-delay",
        type=float,
        default=DEFAULT_RETRY_BASE_DELAY,
        help=argparse.SUPPRESS,
    )
    debug_group.add_argument(
        "--write-batch-size",
        type=int,
        default=DEFAULT_WRITE_BATCH_SIZE,
        help=argparse.SUPPRESS,
    )
    return parser


def _resolve_options(args: argparse.Namespace) -> CliOptions:
    """把参数解析结果转换成运行配置。"""
    return CliOptions(
        db_path=args.db,
        base_url=args.base_url,
        request_timeout=args.request_timeout,
        max_concurrency=args.max_concurrency,
        retry_attempts=args.retry_attempts,
        retry_base_delay=args.retry_base_delay,
        write_batch_size=args.write_batch_size,
        log_level="DEBUG" if args.debug else "INFO",
        log_file=args.log_file,
    )


async def _print_summary(db_path: str) -> None:
    """打印数据库统计摘要。"""
    async with Database(db_path) as db:
        stats = await db.get_statistics()
        job_counts = await db.get_crawl_job_counts()

    lines = [
        "=" * 60,
        "统计摘要",
        "=" * 60,
        f"DB: {db_path}",
        f"总行数: {stats.get('total', 0)}",
        (
            "Level 分布: "
            f"{ {int(key.split('_')[1]): value for key, value in stats.items() if key.startswith('level_')} }"
        ),
        (
            "Jobs 状态: "
            f"ok={job_counts.get('ok', 0)} "
            f"failed={job_counts.get('failed', 0)} "
            f"pending={job_counts.get('pending', 0)}"
        ),
        "=" * 60,
    ]
    print("\n" + "\n".join(lines))


if __name__ == "__main__":
    main()
