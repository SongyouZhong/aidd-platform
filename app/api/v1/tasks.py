"""
任务管理 API

支持两种模式：
1. 分布式模式（默认）：使用 Redis MQ
2. 本地模式：使用内存调度器
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import uuid4
import logging

from app.models import Task, TaskStatus, TaskPriority, ResourceRequirement
from app.api.deps import get_dispatcher, get_redis_task_queue
from app.scheduler import TaskDispatcher
from app.mq import RedisTaskQueue
from app.config import get_settings

router = APIRouter()


# =========================================================================
# 请求/响应模型
# =========================================================================

class ResourceRequirementSchema(BaseModel):
    """资源需求"""
    cpu_cores: int = Field(default=1, ge=1)
    memory_gb: float = Field(default=1.0, ge=0.1)
    gpu_count: int = Field(default=0, ge=0)
    gpu_memory_gb: float = Field(default=0.0, ge=0)


class TaskCreate(BaseModel):
    """创建任务请求"""
    service: str = Field(..., description="服务类型，如 admet, docking")
    task_type: str = Field(default="", description="具体任务类型，如 qikprop, vina")
    name: Optional[str] = Field(None, description="任务名称")
    priority: TaskPriority = Field(default=TaskPriority.NORMAL)
    input_params: Dict[str, Any] = Field(default_factory=dict, description="任务参数")
    input_files: List[str] = Field(default_factory=list, description="输入文件路径")
    resource_requirement: Optional[ResourceRequirementSchema] = None
    max_retries: int = Field(default=3, ge=0)
    timeout_seconds: Optional[int] = Field(None, ge=1)
    job_id: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "service": "admet",
                "task_type": "qikprop",
                "name": "ADMET预测任务",
                "priority": "normal",
                "input_params": {
                    "smiles": ["CCO", "CC(=O)OC1=CC=CC=C1C(=O)O"]
                },
                "input_files": [],
                "resource_requirement": {
                    "cpu_cores": 2,
                    "memory_gb": 4.0
                }
            }
        }


class TaskResponse(BaseModel):
    """任务响应"""
    id: str
    service: str
    name: Optional[str]
    status: TaskStatus
    priority: TaskPriority
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    worker_id: Optional[str]
    retry_count: int
    error_message: Optional[str]
    result: Optional[Dict[str, Any]]

    class Config:
        from_attributes = True


class TaskListResponse(BaseModel):
    """任务列表响应"""
    total: int
    items: List[TaskResponse]


class BatchTaskCreate(BaseModel):
    """批量创建任务请求"""
    tasks: List[TaskCreate]


class BatchTaskResponse(BaseModel):
    """批量创建任务响应"""
    submitted: int
    task_ids: List[str]


class QueueStatsResponse(BaseModel):
    """队列统计响应"""
    total_submitted: int
    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int
    retries: int


# =========================================================================
# API 端点（Redis MQ 模式）
# =========================================================================

@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(
    request: TaskCreate,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> TaskResponse:
    """
    创建新任务（提交到 Redis 队列）
    
    - **service**: 服务类型 (admet, docking 等)
    - **task_type**: 具体任务类型 (qikprop, vina 等)
    - **priority**: 任务优先级
    - **input_params**: 任务参数
    - **input_files**: 输入文件路径
    """
    # 构建任务参数，排除 None 值让 Task 模型使用默认值
    task_data = {
        "id": str(uuid4()),
        "service": request.service,
        "task_type": request.task_type,
        "name": request.name,
        "priority": request.priority,
        "input_params": request.input_params,
        "input_files": request.input_files,
        "max_retries": request.max_retries,
        "job_id": request.job_id,
        "created_at": datetime.utcnow()
    }
    # 仅当 timeout_seconds 有值时才传入
    if request.timeout_seconds is not None:
        task_data["timeout_seconds"] = request.timeout_seconds
    
    task = Task(**task_data)
    
    if request.resource_requirement:
        task.resources = ResourceRequirement(
            cpu_cores=request.resource_requirement.cpu_cores,
            memory_gb=request.resource_requirement.memory_gb,
            gpu_count=request.resource_requirement.gpu_count,
            gpu_memory_gb=request.resource_requirement.gpu_memory_gb
        )
    
    if not await task_queue.push(task):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to submit task (possibly duplicate)"
        )
    
    return TaskResponse(
        id=task.id,
        service=task.service,
        name=task.name,
        status=TaskStatus.PENDING,
        priority=task.priority,
        created_at=task.created_at,
        started_at=None,
        completed_at=None,
        worker_id=None,
        retry_count=0,
        error_message=None,
        result=None
    )


@router.post("/batch", response_model=BatchTaskResponse)
async def create_tasks_batch(
    request: BatchTaskCreate,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> BatchTaskResponse:
    """批量创建任务"""
    tasks = []
    
    for req in request.tasks:
        task = Task(
            id=str(uuid4()),
            service=req.service,
            task_type=req.task_type,
            name=req.name,
            priority=req.priority,
            input_params=req.input_params,
            input_files=req.input_files,
            max_retries=req.max_retries,
            timeout_seconds=req.timeout_seconds,
            job_id=req.job_id,
            created_at=datetime.utcnow()
        )
        
        if req.resource_requirement:
            task.resources = ResourceRequirement(
                cpu_cores=req.resource_requirement.cpu_cores,
                memory_gb=req.resource_requirement.memory_gb,
                gpu_count=req.resource_requirement.gpu_count,
                gpu_memory_gb=req.resource_requirement.gpu_memory_gb
            )
        
        tasks.append(task)
    
    submitted = await task_queue.push_batch(tasks)
    task_ids = [t.id for t in tasks[:submitted]]
    
    return BatchTaskResponse(
        submitted=submitted,
        task_ids=task_ids
    )


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    task_status: Optional[TaskStatus] = Query(None, alias="status", description="按状态筛选"),
    service: Optional[str] = Query(None, description="按服务类型筛选"),
    limit: int = Query(100, ge=1, le=1000),
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> TaskListResponse:
    """获取任务列表"""
    # 获取运行中的任务 ID
    running_task_ids = await task_queue.get_running_tasks()
    
    items = []
    for task_id in running_task_ids[:limit]:
        task = await task_queue.get_task(task_id)
        if task:
            # 筛选
            if task_status and task.status != task_status:
                continue
            if service and task.service != service:
                continue
            
            items.append(TaskResponse(
                id=task.id,
                service=task.service,
                name=task.name,
                status=task.status,
                priority=task.priority,
                created_at=task.created_at,
                started_at=task.started_at,
                completed_at=task.completed_at,
                worker_id=task.worker_id,
                retry_count=task.retry_count or 0,
                error_message=task.error_message,
                result=task.result
            ))
    
    return TaskListResponse(total=len(items), items=items)


@router.get("/stats", response_model=QueueStatsResponse)
async def get_queue_stats(
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> QueueStatsResponse:
    """获取队列统计信息"""
    stats = await task_queue.get_stats()
    return QueueStatsResponse(**stats)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: str,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> TaskResponse:
    """获取任务详情"""
    task = await task_queue.get_task(task_id)
    
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found"
        )
    
    return TaskResponse(
        id=task.id,
        service=task.service,
        name=task.name,
        status=task.status,
        priority=task.priority,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        worker_id=task.worker_id,
        retry_count=task.retry_count or 0,
        error_message=task.error_message,
        result=task.result
    )


@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_task(
    task_id: str,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> None:
    """取消任务"""
    if not await task_queue.cancel(task_id, reason="Cancelled via API"):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found or already completed"
        )
