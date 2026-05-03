"""行政区划数据模型。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


def normalize_xzqh_code(code: str | None) -> str | None:
    """规范化行政区划代码。

    新接口在 `trimCode=true` 时直接返回短码或变长码，这里只做空白清理。

    Args:
        code: 原始代码。

    Returns:
        str | None: 清理后的代码；若为空则返回 `None`。
    """
    if code is None:
        return None

    normalized = str(code).strip()
    return normalized or None


@dataclass(slots=True)
class TreeNode:
    """树形行政区划节点。"""

    code: str
    name: str | None
    level: int
    type: str | None = None
    children: list[TreeNode] = field(default_factory=list)

    @classmethod
    def from_api_data(cls, data: Mapping[str, Any]) -> TreeNode:
        """从接口返回结构构建树节点。

        Args:
            data: 接口返回的单个节点数据。

        Returns:
            TreeNode: 树节点对象。
        """
        children_data = data.get("children") or []
        children = [cls.from_api_data(child) for child in children_data]
        return cls(
            code=str(data.get("code") or ""),
            name=data.get("name"),
            level=int(data.get("level") or 0),
            type=data.get("type"),
            children=children,
        )

    def flatten(
        self,
        *,
        parent_code: str | None = None,
        parent_name: str | None = None,
        name_path: str | None = None,
    ) -> list[AdministrativeDivision]:
        """把树形结构展开为扁平记录。

        Args:
            parent_code: 父级代码。
            parent_name: 父级名称。
            name_path: 祖先名称路径。

        Returns:
            list[AdministrativeDivision]: 当前节点及全部子孙节点的扁平列表。
        """
        current_name = self.name or ""
        current_name_path = f"{name_path}/{current_name}" if name_path else current_name
        current = AdministrativeDivision(
            code=self.code,
            name=self.name,
            level=self.level,
            type=self.type,
            parent_code=parent_code,
            parent_name=parent_name,
            name_path=current_name_path,
        )

        result = [current]
        for child in self.children:
            result.extend(
                child.flatten(
                    parent_code=self.code,
                    parent_name=self.name,
                    name_path=current_name_path,
                ),
            )
        return result


@dataclass(slots=True)
class AdministrativeDivision:
    """扁平化行政区划记录。"""

    code: str
    name: str | None
    level: int
    type: str | None = None
    parent_code: str | None = None
    parent_name: str | None = None
    name_path: str | None = None

    @property
    def province_code(self) -> str:
        """返回所属省份代码前缀。"""
        return self.code[:2]

    def validate(self) -> bool:
        """验证记录是否合法。

        Returns:
            bool: 数据是否合法。
        """
        if not self.code or not self.code.isdigit():
            return False
        if len(self.code) not in (1, 2, 4, 6, 9, 12):
            return False
        if self.level not in (1, 2, 3, 4):
            return False
        return bool(self.name)
