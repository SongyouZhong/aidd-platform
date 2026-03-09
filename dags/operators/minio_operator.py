"""
MinIO 文件操作 Operator

封装 MinIO 文件上传/下载操作，可在 DAG 中复用。
"""

import logging
from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class MinioUploadOperator(BaseOperator):
    """
    上传内容到 MinIO 的 Operator

    参数:
        content: 要上传的内容（字符串）
        object_name: MinIO 中的对象路径
        content_type: MIME 类型
    """

    template_fields = ['content', 'object_name']

    @apply_defaults
    def __init__(
        self,
        content: str,
        object_name: str,
        content_type: str = 'text/csv',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.content = content
        self.object_name = object_name
        self.content_type = content_type

    def execute(self, context):
        from common.minio_utils import upload_bytes

        data = self.content.encode('utf-8') if isinstance(self.content, str) else self.content
        result = upload_bytes(data, self.object_name, self.content_type)
        logger.info(f"文件已上传到 MinIO: {self.object_name}")
        return result


class MinioDownloadOperator(BaseOperator):
    """
    从 MinIO 下载文件内容的 Operator

    参数:
        object_name: MinIO 中的对象路径
    """

    template_fields = ['object_name']

    @apply_defaults
    def __init__(
        self,
        object_name: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.object_name = object_name

    def execute(self, context):
        from common.minio_utils import download_file_as_text

        content = download_file_as_text(self.object_name)
        logger.info(f"文件已从 MinIO 下载: {self.object_name} ({len(content)} bytes)")
        return content
