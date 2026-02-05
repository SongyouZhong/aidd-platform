"""
Worker 模型
定义计算节点（Worker）的数据结构
"""

from enum import Enum
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

from .resource import ResourceUsage


class WorkerStatus(str, Enum):
    """Worker 状态"""
    ONLINE = "online"       # 在线，可接受任务
    BUSY = "busy"           # 忙碌，资源已满
    OFFLINE = "offline"     # 离线
    DRAINING = "draining"   # 排空中，不接受新任务


class Worker(BaseModel):
    """
    计算节点模型
    
    每个 Worker 是一个运行 aidd-toolkit 的计算节点，
    可以是物理机、虚拟机或容器
    """
    
    # 唯一标识
    id: str = Field(..., description="Worker 唯一 ID")
    hostname: str = Field(..., description="主机名")
    ip_address: Optional[str] = Field(default=None, description="IP 地址")
    port: int = Field(default=8080, description="端口号")
    
    # =========================================================================
    # 支持的服务
    # =========================================================================
    supported_services: List[str] = Field(
        default_factory=list,
        description="支持的服务类型: ['admet', 'docking']"
    )
    labels: Dict[str, str] = Field(
        default_factory=dict,
        description="标签，用于调度匹配: {'gpu': 'nvidia', 'zone': 'a'}"
    )
    
    # =========================================================================
    # 资源信息
    # =========================================================================
    total_resources: ResourceUsage = Field(
        default_factory=ResourceUsage,
        description="总资源"
    )
    used_resources: ResourceUsage = Field(
        default_factory=ResourceUsage,
        description="已用资源"
    )
    
    # =========================================================================
    # 状态
    # =========================================================================
    status: WorkerStatus = Field(
        default=WorkerStatus.OFFLINE,
        description="当前状态"
    )
    current_tasks: List[str] = Field(
        default_factory=list,
        description="当前执行的任务 ID 列表"
    )
    max_concurrent_tasks: int = Field(
        default=10,
        ge=1,
        description="最大并发任务数"
    )
    
    # =========================================================================
    # 心跳信息
    # =========================================================================
    last_heartbeat: Optional[datetime] = Field(
        default=None,
        description="最后心跳时间"
    )
    registered_at: Optional[datetime] = Field(
        default=None,
        description="注册时间"
    )
    
    # =========================================================================
    # 统计信息
    # =========================================================================
    total_tasks_completed: int = Field(default=0, ge=0, description="完成任务总数")
    total_tasks_failed: int = Field(default=0, ge=0, description="失败任务总数")
    
    @property
    def available_resources(self) -> ResourceUsage:
        """计算可用资源"""
        return ResourceUsage(
            cpu_cores=max(0, self.total_resources.cpu_cores - self.used_resources.cpu_cores),
            memory_gb=max(0, self.total_resources.memory_gb - self.used_resources.memory_gb),
            gpu_count=max(0, self.total_resources.gpu_count - self.used_resources.gpu_count),
            gpu_memory_gb=max(0, self.total_resources.gpu_memory_gb - self.used_resources.gpu_memory_gb),
        )
    
    @property
    def is_available(self) -> bool:
        """是否可接受新任务"""
        return (
            self.status == WorkerStatus.ONLINE and
            len(self.current_tasks) < self.max_concurrent_tasks
        )
    
    @property
    def utilization(self) -> float:
        """资源利用率 (0-100)"""
        if self.total_resources.cpu_cores == 0:
            return 0.0
        return (self.used_resources.cpu_cores / self.total_resources.cpu_cores) * 100
    
    def can_handle_service(self, service: str) -> bool:
        """检查是否支持指定服务"""
        return service in self.supported_services or len(self.supported_services) == 0
    
    def add_task(self, task_id: str) -> None:
        """添加任务"""
        if task_id not in self.current_tasks:
            self.current_tasks.append(task_id)
        if len(self.current_tasks) >= self.max_concurrent_tasks:
            self.status = WorkerStatus.BUSY
    
    def remove_task(self, task_id: str) -> None:
        """移除任务"""
        if task_id in self.current_tasks:
            self.current_tasks.remove(task_id)
        if self.status == WorkerStatus.BUSY and len(self.current_tasks) < self.max_concurrent_tasks:
            self.status = WorkerStatus.ONLINE
