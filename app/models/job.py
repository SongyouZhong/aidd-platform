"""
作业模型
定义批量计算作业（包含多个任务）
"""

from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

from .task import TaskPriority
from .resource import ResourceRequirement


class JobStatus(str, Enum):
    """作业状态"""
    PENDING = "pending"         # 等待处理
    RUNNING = "running"         # 执行中（部分任务在运行）
    SUCCESS = "success"         # 全部成功
    PARTIAL = "partial"         # 部分成功
    FAILED = "failed"           # 全部失败
    CANCELLED = "cancelled"     # 已取消


class Job(BaseModel):
    """
    批量作业模型
    
    一个 Job 包含多个 Task，用于：
    - 虚拟筛选：对 10000 个分子进行对接
    - 批量 ADMET 预测：对 1000 个分子计算性质
    
    Job 会被拆分成多个 Task 并行执行
    """
    
    # 唯一标识
    id: str = Field(..., description="作业唯一 ID (UUID)")
    name: str = Field(..., description="作业名称")
    description: Optional[str] = Field(default=None, description="作业描述")
    
    # =========================================================================
    # 作业类型
    # =========================================================================
    service: str = Field(..., description="计算服务类型")
    task_type: str = Field(..., description="任务类型")
    
    # =========================================================================
    # 任务配置
    # =========================================================================
    priority: TaskPriority = Field(
        default=TaskPriority.NORMAL,
        description="优先级（应用于所有子任务）"
    )
    task_resources: ResourceRequirement = Field(
        default_factory=ResourceRequirement,
        description="每个任务的资源需求"
    )
    task_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="任务通用参数"
    )
    
    # =========================================================================
    # 输入文件
    # =========================================================================
    input_files: List[str] = Field(
        default_factory=list,
        description="输入文件列表（每个文件对应一个任务）"
    )
    
    # =========================================================================
    # 状态统计
    # =========================================================================
    status: JobStatus = Field(
        default=JobStatus.PENDING,
        description="作业状态"
    )
    total_tasks: int = Field(default=0, ge=0, description="总任务数")
    pending_tasks: int = Field(default=0, ge=0, description="等待中")
    running_tasks: int = Field(default=0, ge=0, description="执行中")
    success_tasks: int = Field(default=0, ge=0, description="成功数")
    failed_tasks: int = Field(default=0, ge=0, description="失败数")
    
    # =========================================================================
    # 时间戳
    # =========================================================================
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="创建时间"
    )
    started_at: Optional[datetime] = Field(
        default=None,
        description="开始时间（第一个任务开始）"
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="完成时间（所有任务结束）"
    )
    
    # =========================================================================
    # 用户信息
    # =========================================================================
    user_id: Optional[str] = Field(default=None, description="提交用户")
    
    @property
    def progress(self) -> float:
        """计算整体进度"""
        if self.total_tasks == 0:
            return 0.0
        finished = self.success_tasks + self.failed_tasks
        return (finished / self.total_tasks) * 100
    
    @property
    def is_finished(self) -> bool:
        """作业是否已完成"""
        return self.status in [
            JobStatus.SUCCESS,
            JobStatus.PARTIAL,
            JobStatus.FAILED,
            JobStatus.CANCELLED
        ]
    
    def update_status(self) -> None:
        """根据任务统计更新作业状态"""
        finished = self.success_tasks + self.failed_tasks
        
        if finished == 0 and self.running_tasks == 0:
            self.status = JobStatus.PENDING
        elif finished < self.total_tasks:
            self.status = JobStatus.RUNNING
        elif self.failed_tasks == 0:
            self.status = JobStatus.SUCCESS
        elif self.success_tasks == 0:
            self.status = JobStatus.FAILED
        else:
            self.status = JobStatus.PARTIAL
