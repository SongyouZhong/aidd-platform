"""
Redis 客户端封装
"""

import logging
from typing import Optional
import redis.asyncio as redis
from redis.asyncio import Redis

from app.config import get_settings

logger = logging.getLogger(__name__)

# 全局 Redis 连接池
_redis_pool: Optional[redis.ConnectionPool] = None
_redis_client: Optional[Redis] = None


class RedisClient:
    """
    Redis 客户端封装
    
    提供连接池管理和常用操作
    """
    
    def __init__(self, url: Optional[str] = None):
        """
        Args:
            url: Redis URL，如 redis://localhost:6379/0
        """
        self._url = url or get_settings().redis_url
        self._client: Optional[Redis] = None
    
    async def connect(self) -> Redis:
        """建立连接"""
        if self._client is None:
            self._client = redis.from_url(
                self._url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=20
            )
            # 测试连接
            await self._client.ping()
            logger.info(f"Connected to Redis: {self._url}")
        return self._client
    
    async def disconnect(self) -> None:
        """关闭连接"""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Disconnected from Redis")
    
    async def get_client(self) -> Redis:
        """获取客户端实例"""
        if self._client is None:
            await self.connect()
        return self._client
    
    # =========================================================================
    # 便捷方法
    # =========================================================================
    
    async def ping(self) -> bool:
        """测试连接"""
        try:
            client = await self.get_client()
            return await client.ping()
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False
    
    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """设置键值"""
        client = await self.get_client()
        return await client.set(key, value, ex=ex)
    
    async def get(self, key: str) -> Optional[str]:
        """获取值"""
        client = await self.get_client()
        return await client.get(key)
    
    async def delete(self, *keys: str) -> int:
        """删除键"""
        client = await self.get_client()
        return await client.delete(*keys)
    
    async def exists(self, *keys: str) -> int:
        """检查键是否存在"""
        client = await self.get_client()
        return await client.exists(*keys)


# =========================================================================
# 全局单例
# =========================================================================

_global_client: Optional[RedisClient] = None


async def get_redis_client() -> RedisClient:
    """获取全局 Redis 客户端"""
    global _global_client
    if _global_client is None:
        _global_client = RedisClient()
        await _global_client.connect()
    return _global_client


async def close_redis_client() -> None:
    """关闭全局 Redis 客户端"""
    global _global_client
    if _global_client:
        await _global_client.disconnect()
        _global_client = None
