"""
作业管理 API

作业（Job）是一组相关任务的集合
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import uuid4

from app.models import Task, TaskStatus, TaskPriority, ResourceRequirement
from app.api.deps import get_redis_task_queue
from app.mq import RedisTaskQueue

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


class TaskInJob(BaseModel):
    """作业中的任务定义"""
    name: Optional[str] = None
    input_data: Dict[str, Any] = Field(default_factory=dict)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    resource_requirement: Optional[ResourceRequirementSchema] = None


class JobCreate(BaseModel):
    """创建作业请求"""
    name: str = Field(..., description="作业名称")
    description: Optional[str] = None
    service: str = Field(..., description="服务类型")
    priority: TaskPriority = Field(default=TaskPriority.NORMAL)
    tasks: List[TaskInJob] = Field(..., min_length=1, description="任务列表")
    max_retries: int = Field(default=3, ge=0)
    timeout_seconds: Optional[int] = Field(None, ge=1)

    class Config:
        json_schema_extra = {
            "example": {
                "name": "ADMET 批量预测",
                "description": "处理 100 个分子的 ADMET 预测",
                "service": "admet",
                "priority": "normal",
                "tasks": [
                    {"name": "mol-1", "input_data": {"smiles": "CCO"}},
                    {"name": "mol-2", "input_data": {"smiles": "CC(=O)O"}}
                ]
            }
        }


class JobResponse(BaseModel):
    """作业响应"""
    id: str
    name: str
    description: Optional[str]
    service: str
    status: str
    priority: TaskPriority
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    progress: float
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]


class JobListResponse(BaseModel):
    """作业列表响应"""
    total: int
    items: List[JobResponse]


class JobTasksResponse(BaseModel):
    """作业任务列表响应"""
    job_id: str
    total: int
    task_ids: List[str]


# =========================================================================
# 内存存储（临时方案，生产环境应使用数据库）
# =========================================================================

_jobs: Dict[str, Dict[str, Any]] = {}


# =========================================================================
# API 端点
# =========================================================================

@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    request: JobCreate,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> JobResponse:
    """
    创建作业
    
    作业会自动拆分为多个任务并提交到队列
    """
    job_id = str(uuid4())
    now = datetime.utcnow()
    
    # 创建任务
    tasks = []
    for i, task_def in enumerate(request.tasks):
        task = Task(
            id=str(uuid4()),
            service=request.service,
            name=task_def.name or f"task-{i+1}",
            priority=request.priority,
            input_data=task_def.input_data,
            parameters=task_def.parameters,
            max_retries=request.max_retries,
            timeout_seconds=request.timeout_seconds,
            job_id=job_id,
            created_at=now
        )
        
        if task_def.resource_requirement:
            task.resource_requirement = ResourceRequirement(
                cpu_cores=task_def.resource_requirement.cpu_cores,
                memory_gb=task_def.resource_requirement.memory_gb,
                gpu_count=task_def.resource_requirement.gpu_count,
                gpu_memory_gb=task_def.resource_requirement.gpu_memory_gb
            )
        
        tasks.append(task)
    
    # 批量提交任务
    submitted = await task_queue.push_batch(tasks)
    
    # 保存作业信息
    job_data = {
        "id": job_id,
        "name": request.name,
        "description": request.description,
        "service": request.service,
        "status": "running" if submitted > 0 else "failed",
        "priority": request.priority,
        "total_tasks": len(tasks),
        "completed_tasks": 0,
        "failed_tasks": 0 if submitted == len(tasks) else len(tasks) - submitted,
        "task_ids": [t.id for t in tasks],
        "created_at": now,
        "started_at": now if submitted > 0 else None,
        "completed_at": None,
        "error_message": None if submitted > 0 else "Failed to submit tasks"
    }
    _jobs[job_id] = job_data
    
    progress = 0.0
    
    return JobResponse(
        id=job_id,
        name=request.name,
        description=request.description,
        service=request.service,
        status=job_data["status"],
        priority=request.priority,
        total_tasks=len(tasks),
        completed_tasks=0,
        failed_tasks=job_data["failed_tasks"],
        progress=progress,
        created_at=now,
        started_at=job_data["started_at"],
        completed_at=None,
        error_message=job_data["error_message"]
    )


@router.get("", response_model=JobListResponse)
async def list_jobs(
    job_status: Optional[str] = Query(None, alias="status", description="按状态筛选"),
    limit: int = Query(100, ge=1, le=1000)
) -> JobListResponse:
    """获取作业列表"""
    jobs = list(_jobs.values())
    
    if job_status:
        jobs = [j for j in jobs if j["status"] == job_status]
    
    jobs = sorted(jobs, key=lambda j: j["created_at"], reverse=True)[:limit]
    
    items = [
        JobResponse(
            id=j["id"],
            name=j["name"],
            description=j["description"],
            service=j["service"],
            status=j["status"],
            priority=j["priority"],
            total_tasks=j["total_tasks"],
            completed_tasks=j["completed_tasks"],
            failed_tasks=j["failed_tasks"],
            progress=j["completed_tasks"] / j["total_tasks"] if j["total_tasks"] > 0 else 0,
            created_at=j["created_at"],
            started_at=j["started_at"],
            completed_at=j["completed_at"],
            error_message=j["error_message"]
        )
        for j in jobs
    ]
    
    return JobListResponse(total=len(items), items=items)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> JobResponse:
    """获取作业详情"""
    job_data = _jobs.get(job_id)
    
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    # 更新任务状态统计
    completed = 0
    failed = 0
    running = 0
    
    for task_id in job_data["task_ids"]:
        task = await task_queue.get_task(task_id)
        if task:
            if task.status == TaskStatus.COMPLETED:
                completed += 1
            elif task.status == TaskStatus.FAILED:
                failed += 1
            elif task.status == TaskStatus.RUNNING:
                running += 1
    
    job_data["completed_tasks"] = completed
    job_data["failed_tasks"] = failed
    
    # 更新作业状态
    total = job_data["total_tasks"]
    if completed + failed == total:
        job_data["status"] = "completed" if failed == 0 else "partial_failed"
        if not job_data["completed_at"]:
            job_data["completed_at"] = datetime.utcnow()
    elif running > 0 or completed > 0:
        job_data["status"] = "running"
    
    progress = completed / total if total > 0 else 0
    
    return JobResponse(
        id=job_data["id"],
        name=job_data["name"],
        description=job_data["description"],
        service=job_data["service"],
        status=job_data["status"],
        priority=job_data["priority"],
        total_tasks=total,
        completed_tasks=completed,
        failed_tasks=failed,
        progress=progress,
        created_at=job_data["created_at"],
        started_at=job_data["started_at"],
        completed_at=job_data["completed_at"],
        error_message=job_data["error_message"]
    )


@router.get("/{job_id}/tasks", response_model=JobTasksResponse)
async def get_job_tasks(job_id: str) -> JobTasksResponse:
    """获取作业的任务列表"""
    job_data = _jobs.get(job_id)
    
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    return JobTasksResponse(
        job_id=job_id,
        total=len(job_data["task_ids"]),
        task_ids=job_data["task_ids"]
    )


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_job(
    job_id: str,
    task_queue: RedisTaskQueue = Depends(get_redis_task_queue)
) -> None:
    """取消作业（取消所有未完成的任务）"""
    job_data = _jobs.get(job_id)
    
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    # 取消所有任务
    for task_id in job_data["task_ids"]:
        await task_queue.cancel(task_id, reason=f"Job {job_id} cancelled")
    
    job_data["status"] = "cancelled"
    job_data["completed_at"] = datetime.utcnow()
    job_data["error_message"] = "Cancelled by user"
