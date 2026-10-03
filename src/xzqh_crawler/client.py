"""民政部行政区划接口客户端。"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

from .models import ApiDivisionNode
from .utils import log_event

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class BackoffController:
    """控制请求节奏并在远端错误高发时短暂冷却。"""

    request_interval: float = 0.2
    cooldown_seconds: float = 10.0
    _lock: asyncio.Lock = field(init=False, repr=False)
    _next_request_at: float = field(init=False, default=0.0, repr=False)
    _cooldown_until: float = field(init=False, default=0.0, repr=False)

    def __post_init__(self) -> None:
        self._lock = asyncio.Lock()
        self._next_request_at = 0.0
        self._cooldown_until = 0.0

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            wait_seconds = max(
                self._next_request_at,
                self._cooldown_until,
            ) - now
            self._next_request_at = max(now, self._next_request_at) + self.request_interval
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)

    async def cool_down(self, seconds: float | None = None) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            self._cooldown_until = max(
                self._cooldown_until,
                loop.time() + (seconds if seconds is not None else self.cooldown_seconds),
            )


class XzqhClientError(RuntimeError):
    """接口访问异常。"""


class XzqhClient:
    """行政区划异步客户端。"""

    def __init__(
        self,
        *,
        base_url: str = "https://dmfw.mca.gov.cn",
        request_timeout: float = 45.0,
        max_concurrency: int = 6,
        retry_attempts: int = 5,
        retry_base_delay: float = 2.0,
        retry_max_delay: float = 60.0,
        request_interval: float = 0.2,
    ) -> None:
        """初始化客户端。

        Args:
            base_url: API 根地址。
            request_timeout: 单个请求的总超时时间，单位秒。
            max_concurrency: 单主机最大并发连接数。
            retry_attempts: 最大重试次数。
            retry_base_delay: 指数退避的基础延迟，单位秒。
        """
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.max_concurrency = max_concurrency
        self.retry_attempts = retry_attempts
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.backoff = BackoffController(request_interval=request_interval)
        self.remote_error_counts: dict[int, int] = {}
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> XzqhClient:
        """进入异步上下文。"""
        await self.open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, exc_tb: object) -> None:
        """退出异步上下文。"""
        await self.close()

    async def open(self) -> None:
        """打开底层 HTTP 会话。"""
        if self._session is not None:
            return

        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        connector = aiohttp.TCPConnector(limit_per_host=self.max_concurrency)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/136.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": "https://dmfw.mca.gov.cn/XzqhVersionPublish.html",
            },
        )

    async def close(self) -> None:
        """关闭底层 HTTP 会话。"""
        if self._session is None:
            return
        await self._session.close()
        self._session = None

    async def fetch_tree(self, code: str = "0", max_level: int = 3) -> ApiDivisionNode:
        """获取树形行政区划数据。

        Args:
            code: 接口的 `code` 参数。
            max_level: 接口的 `maxLevel` 参数。

        Returns:
            ApiDivisionNode: 解析后的树节点。

        Raises:
            ValueError: 参数不合法。
            XzqhClientError: 请求失败或响应格式错误。
        """
        if not code or not str(code).isdigit():
            raise ValueError(f"无效的行政区划代码: {code}")
        if max_level not in (1, 2, 3, 4):
            raise ValueError(f"无效的 max_level: {max_level}")

        payload = await self._request_json(
            "xzqh/getList",
            {
                "code": str(code),
                "trimCode": "true",
                "maxLevel": max_level,
            },
        )
        tree_data = payload.get("data")
        if not isinstance(tree_data, dict) or not tree_data:
            raise XzqhClientError("接口返回缺少 data 字段或 data 为空")
        return ApiDivisionNode.from_api_data(tree_data)

    async def fetch_township_tree(self, parent_code: str) -> ApiDivisionNode | None:
        """获取某个父节点下的乡级数据。

        Args:
            parent_code: 可作为 `maxLevel=4` 请求参数的父节点代码。

        Returns:
            ApiDivisionNode | None: 请求成功时返回树节点；请求失败时返回 `None`。

        Raises:
            ValueError: 父节点代码格式不合法。
        """
        normalized = str(parent_code).strip()
        if not normalized.isdigit() or len(normalized) < 4:
            raise ValueError(f"无效的父节点代码: {parent_code}")

        try:
            return await self.fetch_tree(code=normalized, max_level=4)
        except XzqhClientError as exc:
            error_message = str(exc)
            if error_message == "接口返回缺少 data 字段或 data 为空":
                logger.debug("乡级数据为空: parent_code=%s", normalized)
            else:
                log_event(
                    logger,
                    logging.DEBUG,
                    "task_request_failed",
                    parent_code=normalized,
                    error=error_message.split(":", 1)[0],
                )
            return None

    async def _request_json(
        self,
        endpoint: str,
        params: dict[str, str | int],
    ) -> dict[str, Any]:
        """发送请求并解析 JSON。

        Args:
            endpoint: API 路径。
            params: 查询参数。

        Returns:
            dict[str, Any]: JSON 对象。

        Raises:
            XzqhClientError: 请求在重试后仍失败。
        """
        if self._session is None:
            await self.open()
        assert self._session is not None

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        for attempt in range(1, self.retry_attempts + 1):
            try:
                await self.backoff.wait()
                async with self._session.get(url, params=params) as response:
                    if response.status in RETRYABLE_STATUS_CODES:
                        self.remote_error_counts[response.status] = self.remote_error_counts.get(response.status, 0) + 1
                        if response.status in {502, 503, 504}:
                            await self.backoff.cool_down()
                        await self._handle_retryable_response(response, attempt, url, params)
                        continue
                    if response.status >= 400:
                        body = await response.text()
                        raise XzqhClientError(
                            f"HTTP {response.status}: {body[:200]}",
                        )

                    try:
                        payload = await response.json(content_type=None)
                    except (
                        aiohttp.ContentTypeError,
                        json.JSONDecodeError,
                        UnicodeDecodeError,
                    ) as exc:
                        if attempt >= self.retry_attempts:
                            raise XzqhClientError("响应不是合法 JSON") from exc
                        await self._sleep_before_retry(attempt, response)
                        continue

                    if not isinstance(payload, dict):
                        if attempt >= self.retry_attempts:
                            raise XzqhClientError("响应 JSON 不是对象")
                        await self._sleep_before_retry(attempt, response)
                        continue
                    return payload

            except XzqhClientError:
                raise
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt >= self.retry_attempts:
                    raise XzqhClientError(
                        f"请求失败且已超过最大重试次数: {url}",
                    ) from exc
                logger.debug(
                    "请求失败，准备重试: url=%s attempt=%s/%s params=%s error=%s",
                    url,
                    attempt,
                    self.retry_attempts,
                    params,
                    exc,
                )
                await self._sleep_before_retry(attempt)

        raise XzqhClientError(f"请求失败且未获得有效响应: {url}")

    async def _handle_retryable_response(
        self,
        response: aiohttp.ClientResponse,
        attempt: int,
        url: str,
        params: dict[str, str | int],
    ) -> None:
        """处理可重试状态码。"""
        body = await response.text()
        if attempt >= self.retry_attempts:
            raise XzqhClientError(
                f"HTTP {response.status}: {body[:200]}",
            )

        log_event(
            logger,
            logging.DEBUG,
            "retry_scheduled",
            status=response.status,
            attempt=f"{attempt}/{self.retry_attempts}",
            params=params,
        )
        await self._sleep_before_retry(attempt, response)

    async def _sleep_before_retry(
        self,
        attempt: int,
        response: aiohttp.ClientResponse | None = None,
    ) -> None:
        """在重试前等待。"""
        delay = min(
            self.retry_max_delay,
            self.retry_base_delay * (2 ** (attempt - 1)),
        )
        delay *= random.uniform(0.5, 1.5)
        retry_after = self._parse_retry_after(
            response.headers.get("Retry-After") if response is not None else None,
        )
        await asyncio.sleep(
            min(self.retry_max_delay, max(delay, retry_after or 0.0)),
        )

    def _parse_retry_after(self, value: str | None) -> float | None:
        """解析 `Retry-After` 响应头。"""
        if not value:
            return None

        try:
            return max(float(value), 0.0)
        except ValueError:
            pass

        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return None

        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)

        now = datetime.now(UTC)
        return max((retry_at - now).total_seconds(), 0.0)
