"""
统一存储接口
MinIO 对象存储 + 临时文件
"""

import shutil
import logging
from pathlib import Path
from typing import Optional, Union
from datetime import timedelta
from functools import lru_cache

from app.config import get_config
from app.storage.minio_client import MinioStorage, HAS_MINIO

logger = logging.getLogger(__name__)


class Storage:
    """
    统一存储接口
    
    使用 MinIO 作为主存储，临时文件使用本地磁盘。
    
    用法:
        storage = get_storage()
        
        # 使用 MinIO 存储
        storage.upload('path/file.txt', data)
        data = storage.download('path/file.txt')
        url = storage.get_presigned_url('path/file.txt')
        
        # 临时文件（本地）
        temp_path = storage.save_temp('temp.csv', data)
        storage.clean_temp()
    """
    
    def __init__(self):
        config = get_config()
        
        # 初始化临时目录
        temp_dir = config.get('storage.local.temp_dir', '/tmp/aidd-platform')
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化 MinIO 存储
        self.minio: Optional[MinioStorage] = None
        minio_config = config.get('storage.minio', {})
        
        if minio_config.get('endpoint') and HAS_MINIO:
            try:
                self.minio = MinioStorage(
                    endpoint=minio_config.get('endpoint'),
                    access_key=minio_config.get('access_key'),
                    secret_key=minio_config.get('secret_key'),
                    bucket=minio_config.get('bucket', 'aidd-files'),
                    secure=minio_config.get('secure', False)
                )
                logger.info("MinIO 存储已初始化")
            except Exception as e:
                logger.error(f"MinIO 初始化失败: {e}")
                raise
        else:
            if not HAS_MINIO:
                raise ImportError("minio 库未安装，请运行: pip install minio")
            else:
                raise ValueError("未配置 MinIO endpoint")
    
    # =========================================================================
    # 临时文件（本地磁盘）
    # =========================================================================
    
    def save_temp(self, filename: str, data: Union[bytes, str]) -> Path:
        """
        保存临时文件
        
        Args:
            filename: 文件名（可包含子目录）
            data: 文件内容
        
        Returns:
            临时文件路径
        """
        temp_path = self.temp_dir / filename
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        
        if isinstance(data, str):
            temp_path.write_text(data, encoding='utf-8')
        else:
            temp_path.write_bytes(data)
        
        logger.debug(f"临时文件已保存: {temp_path}")
        return temp_path
    
    def load_temp(self, filename: str) -> bytes:
        """读取临时文件"""
        temp_path = self.temp_dir / filename
        if not temp_path.exists():
            raise FileNotFoundError(f"临时文件不存在: {temp_path}")
        return temp_path.read_bytes()
    
    def clean_temp(self, pattern: str = '*') -> int:
        """
        清理临时文件
        
        Args:
            pattern: glob 模式
        
        Returns:
            清理的文件数量
        """
        count = 0
        for f in self.temp_dir.glob(pattern):
            try:
                if f.is_file():
                    f.unlink()
                elif f.is_dir():
                    shutil.rmtree(f)
                count += 1
            except Exception as e:
                logger.warning(f"清理临时文件失败: {f}, 错误: {e}")
        
        if count > 0:
            logger.info(f"临时文件已清理: {count} 个")
        return count
    
    def get_temp_path(self, filename: str) -> Path:
        """获取临时文件路径（不创建文件）"""
        return self.temp_dir / filename
    
    # =========================================================================
    # MinIO 存储（主存储）
    # =========================================================================
    
    def upload(self, object_name: str, data: Union[bytes, str], content_type: str = 'application/octet-stream') -> str:
        """
        上传文件到 MinIO
        
        Args:
            object_name: 对象名称/路径
            data: 文件内容
            content_type: MIME 类型
        
        Returns:
            MinIO 路径 (bucket/object_name)
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
        return self.minio.upload(object_name, data, content_type)
    
    def upload_file(self, object_name: str, file_path: str, content_type: str = 'application/octet-stream') -> str:
        """上传本地文件到 MinIO"""
        return self.minio.upload_file(object_name, file_path, content_type)
    
    def download(self, object_name: str) -> bytes:
        """从 MinIO 下载文件"""
        return self.minio.download(object_name)
    
    def download_to_file(self, object_name: str, file_path: str) -> str:
        """从 MinIO 下载到本地文件"""
        return self.minio.download_to_file(object_name, file_path)
    
    def download_to_temp(self, object_name: str, filename: Optional[str] = None) -> Path:
        """
        从 MinIO 下载到临时文件
        
        Args:
            object_name: MinIO 对象名称
            filename: 临时文件名，默认使用 object_name 的文件名
        
        Returns:
            临时文件路径
        """
        if filename is None:
            filename = Path(object_name).name
        temp_path = self.get_temp_path(filename)
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        self.minio.download_to_file(object_name, str(temp_path))
        return temp_path
    
    def exists(self, object_name: str) -> bool:
        """检查 MinIO 中文件是否存在"""
        return self.minio.exists(object_name)
    
    def delete(self, object_name: str) -> None:
        """从 MinIO 删除文件"""
        self.minio.delete(object_name)
    
    def get_presigned_url(self, object_name: str, expires_hours: int = 1) -> str:
        """获取 MinIO 预签名 URL"""
        return self.minio.get_presigned_url(object_name, timedelta(hours=expires_hours))
    
    def list_objects(self, prefix: str = "", recursive: bool = True) -> list:
        """列出 MinIO 对象"""
        return self.minio.list_objects(prefix, recursive)


# 全局单例
_storage_instance: Optional[Storage] = None


@lru_cache()
def get_storage() -> Storage:
    """获取存储单例"""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = Storage()
    return _storage_instance
