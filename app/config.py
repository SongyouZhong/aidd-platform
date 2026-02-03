"""
配置管理模块
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional
from functools import lru_cache

import yaml
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """应用配置"""
    
    # 应用
    app_name: str = "aidd-platform"
    app_env: str = Field(default="development", alias="APP_ENV")
    debug: bool = Field(default=True, alias="APP_DEBUG")
    
    # 服务器
    server_host: str = Field(default="0.0.0.0", alias="SERVER_HOST")
    server_port: int = Field(default=8000, alias="SERVER_PORT")
    
    # PostgreSQL
    postgres_host: str = Field(default="10.18.85.10", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=30684, alias="POSTGRES_PORT")
    postgres_user: str = Field(default="appuser", alias="POSTGRES_USER")
    postgres_password: str = Field(default="strongpassword", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="aichemol", alias="POSTGRES_DB")
    postgres_pool_size: int = Field(default=10, alias="POSTGRES_POOL_SIZE")
    
    # Redis
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_password: str = Field(default="", alias="REDIS_PASSWORD")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    
    # MinIO
    minio_endpoint: str = Field(default="172.19.80.100:9090", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    minio_bucket: str = Field(default="aidd-files", alias="MINIO_BUCKET")
    
    # 调度器
    dispatch_interval: float = 1.0
    max_dispatch_batch: int = 100
    default_timeout: int = 3600
    max_retries: int = 3
    
    # Worker 管理
    heartbeat_timeout: int = 30
    heartbeat_check_interval: int = 10
    
    @property
    def database_url(self) -> str:
        """PostgreSQL 连接 URL"""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    @property
    def sync_database_url(self) -> str:
        """同步 PostgreSQL 连接 URL（用于 Alembic）"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    @property
    def redis_url(self) -> str:
        """Redis 连接 URL"""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    class Config:
        env_file = ".env"
        extra = "ignore"


class ConfigLoader:
    """YAML 配置加载器"""
    
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = os.environ.get(
                "AIDD_CONFIG_PATH",
                str(Path(__file__).parent.parent / "config" / "config.yml")
            )
        self._config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load()
    
    def _load(self) -> None:
        """加载配置文件"""
        if self._config_path.exists():
            with open(self._config_path, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            self._config = self._resolve_env_vars(raw)
    
    def _resolve_env_vars(self, obj: Any) -> Any:
        """解析环境变量占位符 ${VAR:default}"""
        if isinstance(obj, str):
            pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
            
            def replace(match):
                var_name = match.group(1)
                default = match.group(2) if match.group(2) is not None else ""
                return os.environ.get(var_name, default)
            
            result = re.sub(pattern, replace, obj)
            
            # 类型转换
            if result.lower() == "true":
                return True
            elif result.lower() == "false":
                return False
            elif result.isdigit():
                return int(result)
            return result
        
        elif isinstance(obj, dict):
            return {k: self._resolve_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_env_vars(item) for item in obj]
        return obj
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置项，支持点号分隔"""
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
    
    def get_service_queue(self, service: str) -> str:
        """获取服务对应的队列名"""
        queues = self.get("service_queues", {})
        return queues.get(service, f"aidd:tasks:{service}")


@lru_cache()
def get_settings() -> Settings:
    """获取配置单例"""
    return Settings()


@lru_cache()
def get_config() -> ConfigLoader:
    """获取 YAML 配置单例"""
    return ConfigLoader()
