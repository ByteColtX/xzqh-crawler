"""行政区划数据模型"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


def normalize_xzqh_code(code: Optional[str]) -> Optional[str]:
    """新接口（trimCode=true）下：code 返回即短码/变长码，不做任何归一化。

    仅做：strip + 空值转 None。
    """
    if code is None:
        return None
    s = str(code).strip()
    return s or None


@dataclass
class TreeNode:
    """树形节点模型（对应API返回的原始结构）"""

    code: str                    # 行政区划代码（trimCode=true 下为短码/变长）
    name: Optional[str]          # 行政区划名称（root 可能为 null）
    level: int                   # 层级 (0-4)
    type: Optional[str] = None   # 类型 (允许为空字符串)
    children: List["TreeNode"] = field(default_factory=list)  # 子节点
    
    @classmethod
    def from_api_data(cls, data: Dict[str, Any]) -> "TreeNode":
        """从API数据创建树形节点"""
        node = cls(
            code=(data.get("code") or ""),
            name=data.get("name"),
            level=int(data.get("level") or 0),
            type=data.get("type"),
        )
        
        # 递归创建子节点（处理children为None的情况）
        children_data = data.get("children")
        if children_data is not None:
            for child_data in children_data:
                child_node = cls.from_api_data(child_data)
                node.children.append(child_node)
        
        return node
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于序列化）"""
        return {
            "code": self.code,
            "name": self.name,
            "level": self.level,
            "type": self.type,
            "children": [child.to_dict() for child in self.children]
        }
    
    def flatten(self, parent_code: Optional[str] = None, 
                parent_name: Optional[str] = None, 
                name_path: Optional[str] = None) -> List["AdministrativeDivision"]:
        """
        将树形结构扁平化
        
        Args:
            parent_code: 父节点代码
            parent_name: 父节点名称
            name_path: 名称路径
            
        Returns:
            扁平化的行政区划对象列表
        """
        # 构建当前节点的名称路径（name 允许为空）
        safe_name = self.name or ""
        current_name_path = f"{name_path}/{safe_name}" if name_path else safe_name
        
        # 创建当前节点的扁平化对象
        current_division = AdministrativeDivision(
            code=(self.code or ""),
            name=self.name,
            level=self.level,
            type=self.type,
            parent_code=(parent_code if parent_code else None),
            parent_name=parent_name,
            name_path=current_name_path,
        )
        
        result = [current_division]
        
        # 递归处理子节点
        for child in self.children:
            child_divisions = child.flatten(
                parent_code=self.code,
                parent_name=self.name,
                name_path=current_name_path
            )
            result.extend(child_divisions)
        
        return result


@dataclass
class AdministrativeDivision:
    """扁平化行政区划数据模型（用于数据库存储）"""

    code: str                    # 行政区划代码（trimCode=true 下为短码/变长）
    name: Optional[str]          # 行政区划名称（root 可能为 null）
    level: int                   # 层级 (0-4)
    type: Optional[str] = None   # 类型 (允许为空字符串)
    parent_code: Optional[str] = None  # 上级代码（从树形结构推导）
    parent_name: Optional[str] = None  # 上级名称（从树形结构推导）
    name_path: Optional[str] = None    # 名称路径（从树形结构推导）
    
    @property
    def is_province(self) -> bool:
        """是否为省级"""
        return self.level == 1
    
    @property
    def is_city(self) -> bool:
        """是否为地级"""
        return self.level == 2
    
    @property
    def is_county(self) -> bool:
        """是否为县级"""
        return self.level == 3
    
    @property
    def is_township(self) -> bool:
        """是否为乡级"""
        return self.level == 4
    
    @classmethod
    def from_api_data(cls, data: Dict[str, Any]) -> "AdministrativeDivision":
        """
        从API数据创建行政区划对象（扁平化数据）
        
        注意：API返回的是树形结构，这个方法适用于已经扁平化的数据
        """
        return cls(
            code=data.get("code", ""),
            name=data.get("name", ""),
            level=data.get("level", 0),
            type=data.get("type"),
            parent_code=data.get("parent_code"),
            parent_name=data.get("parent_name"),
            name_path=data.get("name_path"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "code": self.code,
            "name": self.name,
            "level": self.level,
            "type": self.type,
            "parent_code": self.parent_code,
            "parent_name": self.parent_name,
            "name_path": self.name_path,
        }
    
    def validate(self) -> bool:
        """验证数据有效性"""
        # short/effective codes are allowed (2/4/6/9 digits)
        if not self.code:
            return False
        if self.code.isdigit() and len(self.code) not in (1, 2, 4, 6, 9, 12):
            return False
        
        if not self.name:
            return False
        
        if self.level < 1 or self.level > 4:
            return False
        
        # 验证代码格式（应为数字）
        if not self.code.isdigit():
            return False
        
        return True