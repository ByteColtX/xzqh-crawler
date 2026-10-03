"""接口 DTO、行政区划领域模型和抓取任务模型。"""
from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum, StrEnum
from typing import Any

class DivisionLevel(IntEnum):
    PROVINCE = 1
    CITY = 2
    COUNTY = 3
    TOWNSHIP = 4

class TaskState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

@dataclass(frozen=True, slots=True)
class DivisionCode:
    value: str
    def __post_init__(self) -> None:
        value = self.value.strip()
        if not value or not value.isdigit():
            raise ValueError(f"非法行政区划代码: {self.value!r}")
        object.__setattr__(self, "value", value)
    @property
    def province_code(self) -> str:
        return self.value[:2]
    def __str__(self) -> str:
        return self.value

def normalize_xzqh_code(code: str | None) -> str | None:
    if code is None:
        return None
    value = str(code).strip()
    return value or None

@dataclass(slots=True)
class ApiDivisionNode:
    code: str
    name: str | None
    level: int
    kind: str | None = None
    children: list[ApiDivisionNode] = field(default_factory=list)
    @classmethod
    def from_api_data(cls, data: Mapping[str, Any]) -> ApiDivisionNode:
        children_data = data.get("children") or []
        children = [cls.from_api_data(child) for child in children_data if isinstance(child, Mapping)]
        return cls(str(data.get("code") or ""), data.get("name"), int(data.get("level") or 0), data.get("type") or data.get("kind"), children)
    def flatten(self, *, parent_code: str | None = None) -> list[Division]:
        result: list[Division] = []
        next_parent = parent_code
        if self.level in (1, 2, 3, 4):
            result.append(Division(code=self.code, name=self.name or "未知", level=self.level, kind=self.kind, parent_code=parent_code))
            next_parent = self.code
        for child in self.children:
            result.extend(child.flatten(parent_code=next_parent))
        return result

@dataclass(frozen=True, slots=True)
class Division:
    code: str
    name: str
    level: DivisionLevel | int
    kind: str | None = None
    parent_code: str | None = None
    source_version: str | None = None
    fetched_at: datetime | None = None
    def __post_init__(self) -> None:
        object.__setattr__(self, "code", str(self.code).strip())
        object.__setattr__(self, "level", DivisionLevel(self.level))
        if not self.name:
            raise ValueError("行政区划名称不能为空")
        if self.parent_code is not None:
            object.__setattr__(self, "parent_code", str(self.parent_code).strip())
    @property
    def province_code(self) -> str:
        return self.code[:2]
    def validate(self) -> bool:
        return self.code.isdigit() and len(self.code) == {1: 2, 2: 4, 3: 6, 4: 9}[int(self.level)]

@dataclass(frozen=True, slots=True)
class FetchResult:
    parent_code: str
    target_level: DivisionLevel
    state: TaskState
    divisions: tuple[Division, ...] = ()
    error: str | None = None

@dataclass(slots=True)
class CrawlStats:
    provinces: int = 0
    cities: int = 0
    counties: int = 0
    townships: int = 0
    total: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None
