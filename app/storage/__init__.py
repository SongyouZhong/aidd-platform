"""
存储模块
支持 MinIO 对象存储和临时文件
"""

from app.storage.minio_client import MinioStorage, get_minio_client
from app.storage.storage import Storage, get_storage

__all__ = [
    "MinioStorage",
    "Storage",
    "get_minio_client",
    "get_storage",
]
