"""
资源模型
定义计算资源需求和使用情况
"""

from typing import Optional
from pydantic import BaseModel, Field


class ResourceRequirement(BaseModel):
    """
    任务资源需求
    用于调度器判断 Worker 是否能承接任务
    """
    
    # CPU 需求
    cpu_cores: int = Field(default=1, ge=1, description="需要的 CPU 核心数")
    
    # 内存需求
    memory_gb: float = Field(default=4.0, ge=0.5, description="需要的内存 (GB)")
    
    # GPU 需求
    gpu_count: int = Field(default=0, ge=0, description="需要的 GPU 数量")
    gpu_memory_gb: float = Field(default=0, ge=0, description="需要的 GPU 显存 (GB)")
    
    # 预计执行时间（用于调度优化）
    estimated_time_seconds: int = Field(
        default=3600, 
        ge=1, 
        description="预计执行时间 (秒)"
    )
    
    def can_fit_in(self, available: "ResourceUsage") -> bool:
        """检查是否能在给定的可用资源中运行"""
        return (
            self.cpu_cores <= available.cpu_cores and
            self.memory_gb <= available.memory_gb and
            self.gpu_count <= available.gpu_count and
            self.gpu_memory_gb <= available.gpu_memory_gb
        )


class ResourceUsage(BaseModel):
    """
    资源使用/可用情况
    用于 Worker 上报当前资源状态
    """
    
    # CPU
    cpu_cores: int = Field(default=0, ge=0, description="CPU 核心数")
    cpu_percent: float = Field(default=0, ge=0, le=100, description="CPU 使用率 (%)")
    
    # 内存
    memory_gb: float = Field(default=0, ge=0, description="内存 (GB)")
    memory_percent: float = Field(default=0, ge=0, le=100, description="内存使用率 (%)")
    
    # GPU
    gpu_count: int = Field(default=0, ge=0, description="GPU 数量")
    gpu_memory_gb: float = Field(default=0, ge=0, description="GPU 显存 (GB)")
    gpu_utilization: float = Field(default=0, ge=0, le=100, description="GPU 利用率 (%)")
    
    def subtract(self, requirement: ResourceRequirement) -> "ResourceUsage":
        """减去资源需求，返回剩余资源"""
        return ResourceUsage(
            cpu_cores=max(0, self.cpu_cores - requirement.cpu_cores),
            memory_gb=max(0, self.memory_gb - requirement.memory_gb),
            gpu_count=max(0, self.gpu_count - requirement.gpu_count),
            gpu_memory_gb=max(0, self.gpu_memory_gb - requirement.gpu_memory_gb),
        )
    
    def add(self, requirement: ResourceRequirement) -> "ResourceUsage":
        """加上资源，返回总资源"""
        return ResourceUsage(
            cpu_cores=self.cpu_cores + requirement.cpu_cores,
            memory_gb=self.memory_gb + requirement.memory_gb,
            gpu_count=self.gpu_count + requirement.gpu_count,
            gpu_memory_gb=self.gpu_memory_gb + requirement.gpu_memory_gb,
        )


class WorkerCapacity(BaseModel):
    """
    Worker 容量信息
    包含总资源和已用资源
    """
    
    worker_id: str
    hostname: str
    
    # 总资源
    total: ResourceUsage
    
    # 已用资源
    used: ResourceUsage
    
    @property
    def available(self) -> ResourceUsage:
        """计算可用资源"""
        return ResourceUsage(
            cpu_cores=self.total.cpu_cores - self.used.cpu_cores,
            memory_gb=self.total.memory_gb - self.used.memory_gb,
            gpu_count=self.total.gpu_count - self.used.gpu_count,
            gpu_memory_gb=self.total.gpu_memory_gb - self.used.gpu_memory_gb,
        )
    
    def can_fit(self, requirement: ResourceRequirement) -> bool:
        """检查是否能容纳任务"""
        return requirement.can_fit_in(self.available)
