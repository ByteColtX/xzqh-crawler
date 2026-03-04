"""HTTP客户端，用于调用民政部API"""

import time
import logging
from typing import Optional, Dict, Any
from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import TreeNode

logger = logging.getLogger(__name__)


class XzqhClient:
    """行政区划API客户端（dmfw.mca.gov.cn）"""

    def __init__(
        self,
        base_url: str = "https://dmfw.mca.gov.cn",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        request_delay: float = 0.1,
    ):
        """
        初始化客户端
        
        Args:
            base_url: API基础URL
            timeout: 请求超时时间（秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            request_delay: 请求间隔（秒）
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        
        # 创建会话
        self.session = requests.Session()
        
        # 设置重试策略
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # 设置默认请求头
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        发送HTTP请求
        
        Args:
            endpoint: API端点
            params: 请求参数
            
        Returns:
            响应数据字典
            
        Raises:
            requests.exceptions.RequestException: 请求失败
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # 必须设置Referer头
        headers = {
            "Referer": "https://dmfw.mca.gov.cn/XzqhVersionPublish.html",
        }
        
        try:
            logger.debug(f"请求URL: {url}, 参数: {params}")
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"响应状态: {data.get('status')}, 消息: {data.get('message')}")
            return data
            
        except requests.exceptions.RequestException:
            logger.exception(f"请求失败: URL={url}")
            raise
        except ValueError:
            logger.exception("JSON解析失败")
            raise
    
    @lru_cache(maxsize=128)
    def get_tree_data(self, code: str = "0", max_level: int = 3) -> TreeNode:
        """
        获取树形结构数据（带缓存）
        
        Args:
            code: 行政区划代码，默认为"0"表示全国
            max_level: 最大层级，1-4
            
        Returns:
            树形节点对象
        """
        logger.info(f"获取树形数据: code={code}, max_level={max_level}")
        
        params = {
            "code": code,
            "trimCode": "true",
            "maxLevel": max_level,
        }
        
        try:
            data = self._make_request("xzqh/getList", params)
            
            # API返回格式: {"data": {...}, "status": 200, ...}
            tree_data = data.get("data", {})
            
            if not tree_data:
                raise ValueError("API返回的数据为空")
            
            # 创建树形节点
            root_node = TreeNode.from_api_data(tree_data)
            
            logger.info(f"成功获取树形数据，根节点: {root_node.code}")
            return root_node
            
        except Exception:
            logger.exception("获取树形数据失败")
            raise
    
    def get_township_tree(self, parent_code: str) -> Optional[TreeNode]:
        """获取某个父节点的乡级树形数据（maxLevel=4）。

        说明：新接口语义下，parent_code 是“请求 code 参数”，可以是 L2(4位) 或 L3(6位)。

        Returns:
            TreeNode: 请求成功且 data 结构有效（即便 children=[] 也返回 TreeNode）
            None: 请求失败/响应结构异常
        """
        if not parent_code or not str(parent_code).isdigit():
            logger.error(f"父节点代码格式错误: {parent_code}")
            return None

        request_code = str(parent_code)
        if len(request_code) < 4:
            logger.error(f"父节点代码格式错误: {parent_code} (至少4位)")
            return None

        logger.info(f"获取父节点 code={request_code} 的乡级数据")

        try:
            tree_node = self.get_tree_data(code=request_code, max_level=4)
            # 注意：children=[] 代表“确实没有下级”，属于成功。
            return tree_node
        except Exception:
            logger.exception(f"获取父节点 code={request_code} 的乡级数据失败")
            return None
    
    def get_all_township_trees(self, county_codes: list) -> Dict[str, Optional[TreeNode]]:
        """
        批量获取多个县的乡级树形数据
        
        Args:
            county_codes: 县级代码列表
            
        Returns:
            字典：县级代码 -> 树形节点对象（或None）
        """
        logger.info(f"开始批量获取{len(county_codes)}个县的乡级数据")
        
        results = {}
        
        for i, county_code in enumerate(county_codes, 1):
            try:
                tree_node = self.get_township_tree(county_code)
                results[county_code] = tree_node
                
                logger.info(f"进度: {i}/{len(county_codes)} - {county_code}: {'成功' if tree_node else '失败'}")
                
                # 避免请求过快
                if i < len(county_codes):
                    time.sleep(self.request_delay)
                    
            except Exception as e:
                logger.warning(f"获取县级代码 {county_code} 的乡级数据失败: {e}")
                results[county_code] = None
        
        success_count = sum(1 for v in results.values() if v is not None)
        logger.info(f"批量获取完成: 成功{success_count}/{len(county_codes)}个县")
        
        return results
    
    def clear_cache(self):
        """清除缓存"""
        self.get_tree_data.cache_clear()
        logger.debug("API缓存已清除")
    
    def close(self):
        """关闭会话"""
        self.session.close()
        logger.debug("HTTP客户端已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()