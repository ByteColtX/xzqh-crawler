"""配置管理模块"""

import os
import tomllib
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass


@dataclass
class Config:
    """配置类"""
    
    # 数据库配置
    db_path: str = "./data/xzqh.db"
    
    # API配置
    base_url: str = "https://dmfw.mca.gov.cn"
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # 爬虫配置
    max_workers: int = 10
    batch_size: int = 100
    fetch_townships: bool = True  # 是否获取乡级数据
    township_batch_delay: float = 2.0  # 乡级数据批次间延迟（秒）
    township_max_retries: int = 3  # 乡级数据最大重试次数
    
    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        """从字典创建配置对象"""
        return cls(
            db_path=data.get("db_path", "./data/xzqh.db"),
            base_url=data.get("base_url", "https://dmfw.mca.gov.cn"),
            timeout=data.get("timeout", 30),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 1.0),
            max_workers=data.get("max_workers", 10),
            batch_size=data.get("batch_size", 100),
            fetch_townships=data.get("fetch_townships", True),
            township_batch_delay=data.get("township_batch_delay", 2.0),
            township_max_retries=data.get("township_max_retries", 3),
            log_level=data.get("log_level", "INFO"),
            log_file=data.get("log_file"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "db_path": self.db_path,
            "base_url": self.base_url,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "max_workers": self.max_workers,
            "batch_size": self.batch_size,
            "fetch_townships": self.fetch_townships,
            "township_batch_delay": self.township_batch_delay,
            "township_max_retries": self.township_max_retries,
            "log_level": self.log_level,
            "log_file": self.log_file,
        }


class ConfigManager:
    """配置管理器"""
    
    DEFAULT_CONFIG_PATHS = [
        "./config.toml",
        "./config/config.toml",
        "~/.config/xzqh-crawler/config.toml",
    ]
    
    def __init__(self):
        self.config: Optional[Config] = None
    
    def load(self, config_path: Optional[str] = None) -> Config:
        """
        加载配置
        
        Args:
            config_path: 配置文件路径，如果为None则尝试从默认位置加载
            
        Returns:
            配置对象
        """
        # 1. 尝试从指定路径加载
        if config_path:
            config_data = self._load_from_file(config_path)
            if config_data:
                self.config = Config.from_dict(config_data)
                return self.config
        
        # 2. 尝试从默认位置加载
        for path in self.DEFAULT_CONFIG_PATHS:
            expanded_path = os.path.expanduser(path)
            config_data = self._load_from_file(expanded_path)
            if config_data:
                self.config = Config.from_dict(config_data)
                return self.config
        
        # 3. 使用默认配置
        self.config = Config()
        return self.config
    
    def _load_from_file(self, filepath: str) -> Optional[Dict[str, Any]]:
        """从文件加载配置"""
        path = Path(filepath)
        
        if not path.exists():
            return None
        
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
            
            # 支持嵌套配置结构
            config_data = {}
            
            # 数据库配置
            if "database" in data:
                config_data.update({
                    "db_path": data["database"].get("path", "./data/xzqh.db"),
                })
            
            # API配置
            if "api" in data:
                config_data.update({
                    "base_url": data["api"].get("base_url", "https://dmfw.mca.gov.cn"),
                    "timeout": data["api"].get("timeout", 30),
                    "max_retries": data["api"].get("max_retries", 3),
                    "retry_delay": data["api"].get("retry_delay", 1.0),
                })
            
            # 爬虫配置
            if "crawler" in data:
                config_data.update({
                    "max_workers": data["crawler"].get("max_workers", 10),
                    "batch_size": data["crawler"].get("batch_size", 100),
                    "fetch_townships": data["crawler"].get("fetch_townships", True),
                    "township_batch_delay": data["crawler"].get("township_batch_delay", 2.0),
                    "township_max_retries": data["crawler"].get("township_max_retries", 3),
                })
            
            # 日志配置
            if "logging" in data:
                config_data.update({
                    "log_level": data["logging"].get("level", "INFO"),
                    "log_file": data["logging"].get("file"),
                })
            
            # 直接配置项（扁平结构）
            for key in ["db_path", "base_url", "timeout", "max_retries", 
                       "retry_delay", "max_workers", "batch_size", 
                       "fetch_townships", "township_batch_delay", "township_max_retries",
                       "log_level", "log_file"]:
                if key in data:
                    config_data[key] = data[key]
            
            return config_data
            
        except Exception as e:
            print(f"加载配置文件失败: {filepath}, 错误: {e}")
            return None
    
    def save(self, config_path: str, config: Optional[Config] = None):
        """
        保存配置到文件
        
        Args:
            config_path: 配置文件路径
            config: 配置对象，如果为None则使用当前配置
        """
        if config is None:
            config = self.config
        
        if config is None:
            raise ValueError("没有可保存的配置")
        
        try:
            path = Path(config_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            config_dict = config.to_dict()
            
            # 转换为TOML格式
            toml_content = self._dict_to_toml(config_dict)
            
            with open(path, "w", encoding="utf-8") as f:
                f.write(toml_content)
            
            print(f"配置已保存到: {config_path}")
            
        except Exception as e:
            print(f"保存配置文件失败: {e}")
    
    def _dict_to_toml(self, data: Dict[str, Any]) -> str:
        """将字典转换为TOML格式字符串"""
        lines = []
        
        # 数据库配置
        lines.append("[database]")
        lines.append(f'path = "{data["db_path"]}"')
        lines.append("")
        
        # API配置
        lines.append("[api]")
        lines.append(f'base_url = "{data["base_url"]}"')
        lines.append(f"timeout = {data['timeout']}")
        lines.append(f"max_retries = {data['max_retries']}")
        lines.append(f"retry_delay = {data['retry_delay']}")
        lines.append("")
        
        # 爬虫配置
        lines.append("[crawler]")
        lines.append(f"max_workers = {data['max_workers']}")
        lines.append(f"batch_size = {data['batch_size']}")
        lines.append(f"fetch_townships = {str(data['fetch_townships']).lower()}")
        lines.append(f"township_batch_delay = {data['township_batch_delay']}")
        lines.append(f"township_max_retries = {data['township_max_retries']}")
        lines.append("")
        
        # 日志配置
        lines.append("[logging]")
        lines.append(f'level = "{data["log_level"]}"')
        if data["log_file"]:
            lines.append(f'file = "{data["log_file"]}"')
        
        return "\n".join(lines)
    
    def get_config(self) -> Config:
        """获取当前配置"""
        if self.config is None:
            return self.load()
        return self.config
    
    def create_default_config(self, config_path: str = "./config.toml"):
        """创建默认配置文件"""
        default_config = Config()
        self.save(config_path, default_config)


# 全局配置实例
_config_manager = ConfigManager()


def get_config(config_path: Optional[str] = None) -> Config:
    """
    获取配置（便捷函数）
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置对象
    """
    return _config_manager.load(config_path)


def save_config(config_path: str, config: Optional[Config] = None):
    """
    保存配置（便捷函数）
    
    Args:
        config_path: 配置文件路径
        config: 配置对象
    """
    _config_manager.save(config_path, config)


def create_default_config(config_path: str = "./config.toml"):
    """创建默认配置文件（便捷函数）"""
    _config_manager.create_default_config(config_path)