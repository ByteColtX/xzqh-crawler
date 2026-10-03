"""命令行入口。"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass

from .crawler import XzqhCrawler
from .database import Database
from .utils import setup_logging

DEFAULT_DB_PATH = "./data/xzqh.db"
DEFAULT_BASE_URL = "https://dmfw.mca.gov.cn"
DEFAULT_REQUEST_TIMEOUT = 45.0
DEFAULT_MAX_CONCURRENCY = 6
DEFAULT_RETRY_ATTEMPTS = 5
DEFAULT_RETRY_BASE_DELAY = 2.0
DEFAULT_RETRY_MAX_DELAY = 60.0
DEFAULT_REQUEST_INTERVAL = 0.2

@dataclass(slots=True)
class CliOptions:
    db_path: str = DEFAULT_DB_PATH
    base_url: str = DEFAULT_BASE_URL
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY
    retry_max_delay: float = DEFAULT_RETRY_MAX_DELAY
    request_interval: float = DEFAULT_REQUEST_INTERVAL
    log_level: str = "INFO"
    log_file: str | None = None
    output_format: str = "text"
    quiet: bool = False

def main() -> int:
    args = _build_parser().parse_args()
    if args.command == "status":
        setup_logging(level="WARNING", console=not args.quiet)
        asyncio.run(_print_status(args.database, args.output_format))
        return 0
    options = _resolve_options(args)
    setup_logging(level=options.log_level, log_file=options.log_file, console=not options.quiet)
    success = asyncio.run(_run(options, resume=args.command == "resume"))
    return 0 if success else 1

async def _run(options: CliOptions, *, resume: bool) -> bool:
    crawler = XzqhCrawler(db_path=options.db_path, base_url=options.base_url, request_timeout=options.request_timeout, max_concurrency=options.max_concurrency, retry_attempts=options.retry_attempts, retry_base_delay=options.retry_base_delay, retry_max_delay=options.retry_max_delay, request_interval=options.request_interval)
    success = await (crawler.retry_failed_jobs() if resume else crawler.crawl())
    if not success and options.output_format == "text":
        print("提示：可降低并发或使用 xzqh resume --database PATH 重试失败任务。", file=sys.stderr)
    await _print_status(options.db_path, options.output_format, heading="抓取完成" if success else "抓取失败或存在失败任务")
    return success

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xzqh", description="抓取民政部行政区划数据并写入 SQLite。")
    parser.add_argument("--version", action="version", version="xzqh-crawler 0.1.0")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (("crawl", "抓取完整的 1-4 级行政区划数据"), ("resume", "重试 pending/failed 任务")):
        command = subparsers.add_parser(name, help=help_text, description=help_text)
        _add_run_options(command)
    status = subparsers.add_parser("status", help="查看数据库和最近一次运行状态")
    status.add_argument("-d", "--database", default=DEFAULT_DB_PATH, metavar="PATH")
    status.add_argument("--format", dest="output_format", choices=("text", "json"), default="text")
    status.add_argument("-q", "--quiet", action="store_true")
    return parser

def _add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-d", "--database", default=DEFAULT_DB_PATH, metavar="PATH", help=f"SQLite 路径（默认 {DEFAULT_DB_PATH}）")
    parser.add_argument("-c", "--concurrency", dest="max_concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY, metavar="N")
    parser.add_argument("-t", "--timeout", dest="request_timeout", type=float, default=DEFAULT_REQUEST_TIMEOUT, metavar="SECONDS")
    parser.add_argument("--retry-attempts", type=int, default=DEFAULT_RETRY_ATTEMPTS, metavar="N")
    parser.add_argument("--retry-base-delay", type=float, default=DEFAULT_RETRY_BASE_DELAY, metavar="SECONDS")
    parser.add_argument("--retry-max-delay", type=float, default=DEFAULT_RETRY_MAX_DELAY, metavar="SECONDS")
    parser.add_argument("--request-interval", type=float, default=DEFAULT_REQUEST_INTERVAL, metavar="SECONDS")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, metavar="URL")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    parser.add_argument("--log-file", metavar="PATH")
    parser.add_argument("--format", dest="output_format", choices=("text", "json"), default="text")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")

def _resolve_options(args: argparse.Namespace) -> CliOptions:
    for name in ("max_concurrency", "retry_attempts"):
        if getattr(args, name) < 1:
            raise SystemExit(f"xzqh: error: --{name.replace('_', '-')} must be greater than zero")
    for name in ("request_timeout", "retry_base_delay", "retry_max_delay", "request_interval"):
        if getattr(args, name) < 0:
            raise SystemExit(f"xzqh: error: --{name.replace('_', '-')} must not be negative")
    if args.retry_max_delay < args.retry_base_delay:
        raise SystemExit("xzqh: error: --retry-max-delay must be greater than or equal to --retry-base-delay")
    return CliOptions(db_path=args.database, base_url=args.base_url, request_timeout=args.request_timeout, max_concurrency=args.max_concurrency, retry_attempts=args.retry_attempts, retry_base_delay=args.retry_base_delay, retry_max_delay=args.retry_max_delay, request_interval=args.request_interval, log_level="DEBUG" if args.verbose else args.log_level, log_file=args.log_file, output_format=args.output_format, quiet=args.quiet)

async def _print_status(db_path: str, output_format: str, *, heading: str | None = None) -> None:
    async with Database(db_path) as db:
        stats = await db.get_statistics()
        tasks = await db.get_task_counts()
    payload = {"database": db_path, "divisions": {key.removeprefix("level_"): value for key, value in stats.items() if key.startswith("level_")}, "total": stats.get("total", 0), "tasks": tasks}
    if output_format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if heading:
        print(heading, file=sys.stdout)
    print(f"Database: {db_path}")
    print(f"Divisions: total={payload['total']} levels={payload['divisions']}")
    print("Tasks: " + " ".join(f"{key}={value}" for key, value in tasks.items()))

if __name__ == "__main__":
    raise SystemExit(main())
