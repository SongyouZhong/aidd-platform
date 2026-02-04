"""
任务模型
定义计算任务的数据结构
"""

from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

from .resource import ResourceRequirement


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"         # 等待调度
    QUEUED = "queued"           # 已入队列，等待 Worker 处理
    RUNNING = "running"         # 执行中
    SUCCESS = "success"         # 成功完成
    FAILED = "failed"           # 执行失败
    CANCELLED = "cancelled"     # 已取消
    TIMEOUT = "timeout"         # 执行超时


class TaskPriority(int, Enum):
    """任务优先级（数值越小优先级越高）"""
    CRITICAL = 0    # 最高优先级，紧急任务
    HIGH = 1        # 高优先级
    NORMAL = 2      # 正常优先级
    LOW = 3         # 低优先级
    BATCH = 4       # 批量任务，最低优先级


class Task(BaseModel):
    """
    计算任务模型
    
    一个 Task 代表一个原子计算单元，例如：
    - 对一批分子运行 QikProp
    - 对一个配体进行分子对接
    """
    
    # 唯一标识
    id: str = Field(..., description="任务唯一 ID (UUID)")
    
    # 关联的批量作业（可选）
    job_id: Optional[str] = Field(default=None, description="所属批量作业 ID")
    
    # 任务名称（可选，便于用户识别）
    name: Optional[str] = Field(default=None, description="任务名称")
    
    # =========================================================================
    # 任务类型
    # =========================================================================
    service: str = Field(..., description="计算服务类型: admet, docking, md, qsar")
    task_type: str = Field(..., description="具体任务类型: qikprop, vina, glide")
    
    # =========================================================================
    # 调度相关
    # =========================================================================
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL,
        description="任务优先级"
    )
    resources: ResourceRequirement = Field(
        default_factory=ResourceRequirement,
        description="资源需求"
    )
    
    # =========================================================================
    # 输入输出
    # =========================================================================
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="任务参数"
    )
    input_files: List[str] = Field(
        default_factory=list,
        description="输入文件路径 (MinIO)"
    )
    output_files: List[str] = Field(
        default_factory=list,
        description="输出文件路径 (MinIO)"
    )
    result: Optional[Dict[str, Any]] = Field(
        default=None,
        description="执行结果"
    )
    
    # =========================================================================
    # 状态
    # =========================================================================
    status: TaskStatus = Field(
        default=TaskStatus.PENDING,
        description="当前状态"
    )
    progress: float = Field(
        default=0.0,
        ge=0,
        le=100,
        description="执行进度 (0-100)"
    )
    
    # =========================================================================
    # 时间戳
    # =========================================================================
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="创建时间"
    )
    scheduled_at: Optional[datetime] = Field(
        default=None,
        description="调度时间（进入队列）"
    )
    started_at: Optional[datetime] = Field(
        default=None,
        description="开始执行时间"
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="完成时间"
    )
    
    # =========================================================================
    # 执行信息
    # =========================================================================
    worker_id: Optional[str] = Field(
        default=None,
        description="执行任务的 Worker ID"
    )
    retry_count: int = Field(
        default=0,
        ge=0,
        description="重试次数"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="最大重试次数"
    )
    timeout_seconds: int = Field(
        default=3600,
        ge=60,
        description="超时时间（秒）"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="错误信息"
    )
    
    # =========================================================================
    # 用户信息
    # =========================================================================
    user_id: Optional[str] = Field(
        default=None,
        description="提交任务的用户 ID"
    )
    
    @property
    def is_finished(self) -> bool:
        """任务是否已完成（成功、失败或取消）"""
        return self.status in [
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT
        ]
    
    @property
    def can_retry(self) -> bool:
        """是否可以重试"""
        return (
            self.status == TaskStatus.FAILED and
            self.retry_count < self.max_retries
        )
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """执行耗时（秒）"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def to_queue_message(self) -> Dict[str, Any]:
        """转换为队列消息格式"""
        return {
            "task_id": self.id,
            "task_type": self.task_type,
            "payload": self.input_params,
            "input_files": self.input_files,
            "timeout": self.timeout_seconds,
            "created_at": self.created_at.isoformat(),
        }
