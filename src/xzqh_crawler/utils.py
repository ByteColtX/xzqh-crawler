"""工具函数"""

import logging
import sys
from typing import Optional
from pathlib import Path


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_str: Optional[str] = None,
    console: bool = True,
):
    """
    设置日志配置
    
    Args:
        level: 日志级别
        log_file: 日志文件路径
        format_str: 日志格式字符串
    """
    if format_str is None:
        format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 创建控制台处理器
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(format_str)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
    
    # 创建文件处理器（如果指定了日志文件）
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(format_str)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
    
    # 设置第三方库的日志级别
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def validate_code(code: str) -> bool:
    """
    验证行政区划代码格式
    
    Args:
        code: 行政区划代码
        
    Returns:
        是否有效
    """
    if not code:
        return False
    
    # 检查长度
    if len(code) != 12:
        return False
    
    # 检查是否为数字
    if not code.isdigit():
        return False
    
    return True


def get_code_prefix(code: str, level: int) -> str:
    """
    获取指定层级的代码前缀
    
    Args:
        code: 完整的12位代码
        level: 层级 (1-4)
        
    Returns:
        代码前缀
        
    Raises:
        ValueError: 如果代码格式无效或层级无效
    """
    if not validate_code(code):
        raise ValueError(f"无效的行政区划代码: {code}")
    
    if level < 1 or level > 4:
        raise ValueError(f"无效的层级: {level}")
    
    # 各层级的代码长度
    level_lengths = {
        1: 2,   # 省级: 前2位
        2: 4,   # 地级: 前4位
        3: 6,   # 县级: 前6位
        4: 9,   # 乡级: 前9位
    }
    
    length = level_lengths[level]
    return code[:length].ljust(12, "0")


def format_duration(seconds: float) -> str:
    """
    格式化持续时间
    
    Args:
        seconds: 秒数
        
    Returns:
        格式化后的时间字符串
    """
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{int(hours)}小时{int(minutes)}分{int(seconds)}秒"
    elif minutes > 0:
        return f"{int(minutes)}分{int(seconds)}秒"
    else:
        return f"{seconds:.1f}秒"


def format_number(number: int) -> str:
    """
    格式化数字，添加千位分隔符
    
    Args:
        number: 数字
        
    Returns:
        格式化后的字符串
    """
    return f"{number:,}"


def print_progress(current: int, total: int, prefix: str = "", width: int = 50):
    """
    打印进度条
    
    Args:
        current: 当前进度
        total: 总进度
        prefix: 前缀文本
        width: 进度条宽度
    """
    if total == 0:
        return
    
    percent = current / total
    filled_width = int(width * percent)
    
    bar = "█" * filled_width + "░" * (width - filled_width)
    percent_text = f"{percent*100:.1f}%"
    
    if prefix:
        print(f"\r{prefix} |{bar}| {percent_text} {current}/{total}", end="", flush=True)
    else:
        print(f"\r|{bar}| {percent_text} {current}/{total}", end="", flush=True)
    
    if current == total:
        print()  # 换行