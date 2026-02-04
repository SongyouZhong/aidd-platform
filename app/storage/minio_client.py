"""
MinIO 对象存储客户端
"""

import logging
from io import BytesIO
from typing import BinaryIO, Optional, Union
from datetime import timedelta
from functools import lru_cache

try:
    from minio import Minio
    from minio.error import S3Error
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False
    Minio = None
    S3Error = Exception

from app.config import get_settings

logger = logging.getLogger(__name__)


class MinioStorage:
    """
    MinIO 对象存储客户端
    
    用法:
        storage = MinioStorage()
        
        # 上传文件
        storage.upload('path/to/file.csv', file_data)
        storage.upload_file('path/to/file.csv', '/local/path/file.csv')
        
        # 下载文件
        data = storage.download('path/to/file.csv')
        storage.download_to_file('path/to/file.csv', '/local/path/file.csv')
        
        # 获取预签名 URL
        url = storage.get_presigned_url('path/to/file.csv')
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
        secure: bool = False
    ):
        if not HAS_MINIO:
            raise ImportError("minio 库未安装，请运行: pip install minio")
        
        # 从配置获取默认值
        settings = get_settings()
        
        self.endpoint = endpoint or settings.minio_endpoint
        self.access_key = access_key or settings.minio_access_key
        self.secret_key = secret_key or settings.minio_secret_key
        self.bucket = bucket or settings.minio_bucket
        self.secure = secure if secure is not None else settings.minio_secure
        
        # 创建 MinIO 客户端
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        
        # 确保 bucket 存在
        self._ensure_bucket()
        
        logger.info(f"MinIO 客户端已初始化: {self.endpoint}/{self.bucket}")
    
    def _ensure_bucket(self) -> None:
        """确保存储桶存在"""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"MinIO bucket 已创建: {self.bucket}")
        except S3Error as e:
            logger.error(f"MinIO bucket 操作失败: {e}")
            raise
    
    def upload(
        self,
        object_name: str,
        data: Union[bytes, BinaryIO],
        content_type: str = 'application/octet-stream'
    ) -> str:
        """
        上传数据到 MinIO
        
        Args:
            object_name: 对象名称（路径）
            data: 文件数据，可以是 bytes 或文件对象
            content_type: MIME 类型
        
        Returns:
            对象的完整路径 (bucket/object_name)
        """
        try:
            if isinstance(data, bytes):
                self.client.put_object(
                    self.bucket,
                    object_name,
                    BytesIO(data),
                    length=len(data),
                    content_type=content_type
                )
            else:
                # 文件对象
                data.seek(0, 2)  # 移到末尾获取大小
                length = data.tell()
                data.seek(0)  # 移回开头
                self.client.put_object(
                    self.bucket,
                    object_name,
                    data,
                    length=length,
                    content_type=content_type
                )
            
            logger.info(f"文件已上传到 MinIO: {self.bucket}/{object_name}")
            return f"{self.bucket}/{object_name}"
        
        except S3Error as e:
            logger.error(f"MinIO 上传失败: {e}")
            raise
    
    def upload_file(
        self,
        object_name: str,
        file_path: str,
        content_type: str = 'application/octet-stream'
    ) -> str:
        """
        上传本地文件到 MinIO
        
        Args:
            object_name: 对象名称（路径）
            file_path: 本地文件路径
            content_type: MIME 类型
        
        Returns:
            对象的完整路径
        """
        try:
            self.client.fput_object(
                self.bucket,
                object_name,
                file_path,
                content_type=content_type
            )
            logger.info(f"文件已上传到 MinIO: {file_path} -> {self.bucket}/{object_name}")
            return f"{self.bucket}/{object_name}"
        except S3Error as e:
            logger.error(f"MinIO 上传失败: {e}")
            raise
    
    def download(self, object_name: str) -> bytes:
        """
        从 MinIO 下载文件
        
        Args:
            object_name: 对象名称
        
        Returns:
            文件内容
        """
        try:
            response = self.client.get_object(self.bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            logger.debug(f"文件已下载: {self.bucket}/{object_name}")
            return data
        except S3Error as e:
            logger.error(f"MinIO 下载失败: {e}")
            raise
    
    def download_to_file(self, object_name: str, file_path: str) -> str:
        """
        下载到本地文件
        
        Args:
            object_name: 对象名称
            file_path: 本地文件路径
        
        Returns:
            本地文件路径
        """
        try:
            self.client.fget_object(self.bucket, object_name, file_path)
            logger.info(f"文件已下载: {self.bucket}/{object_name} -> {file_path}")
            return file_path
        except S3Error as e:
            logger.error(f"MinIO 下载失败: {e}")
            raise
    
    def delete(self, object_name: str) -> None:
        """删除对象"""
        try:
            self.client.remove_object(self.bucket, object_name)
            logger.info(f"对象已删除: {self.bucket}/{object_name}")
        except S3Error as e:
            logger.error(f"MinIO 删除失败: {e}")
            raise
    
    def exists(self, object_name: str) -> bool:
        """检查对象是否存在"""
        try:
            self.client.stat_object(self.bucket, object_name)
            return True
        except S3Error:
            return False
    
    def get_stat(self, object_name: str) -> dict:
        """获取对象元信息"""
        try:
            stat = self.client.stat_object(self.bucket, object_name)
            return {
                'size': stat.size,
                'etag': stat.etag,
                'content_type': stat.content_type,
                'last_modified': stat.last_modified,
                'metadata': stat.metadata
            }
        except S3Error as e:
            logger.error(f"获取对象信息失败: {e}")
            raise
    
    def get_presigned_url(
        self,
        object_name: str,
        expires: timedelta = timedelta(hours=1),
        method: str = 'GET'
    ) -> str:
        """
        获取预签名 URL
        
        Args:
            object_name: 对象名称
            expires: 过期时间
            method: HTTP 方法 (GET 或 PUT)
        
        Returns:
            预签名 URL
        """
        if method.upper() == 'GET':
            return self.client.presigned_get_object(
                self.bucket,
                object_name,
                expires=expires
            )
        else:
            return self.client.presigned_put_object(
                self.bucket,
                object_name,
                expires=expires
            )
    
    def list_objects(
        self,
        prefix: str = "",
        recursive: bool = True
    ) -> list:
        """
        列出对象
        
        Args:
            prefix: 前缀过滤
            recursive: 是否递归列出
        
        Returns:
            对象列表
        """
        objects = self.client.list_objects(
            self.bucket,
            prefix=prefix,
            recursive=recursive
        )
        return [
            {
                'name': obj.object_name,
                'size': obj.size,
                'last_modified': obj.last_modified,
                'etag': obj.etag
            }
            for obj in objects
        ]
    
    def copy(self, source_object: str, dest_object: str) -> str:
        """复制对象"""
        from minio.commonconfig import CopySource
        
        try:
            self.client.copy_object(
                self.bucket,
                dest_object,
                CopySource(self.bucket, source_object)
            )
            logger.info(f"对象已复制: {source_object} -> {dest_object}")
            return f"{self.bucket}/{dest_object}"
        except S3Error as e:
            logger.error(f"MinIO 复制失败: {e}")
            raise


# 全局单例
_minio_client: Optional[MinioStorage] = None


@lru_cache()
def get_minio_client() -> MinioStorage:
    """获取 MinIO 客户端单例"""
    global _minio_client
    if _minio_client is None:
        _minio_client = MinioStorage()
    return _minio_client
