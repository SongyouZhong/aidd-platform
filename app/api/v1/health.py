"""
健康检查 API
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from datetime import datetime

from app.api.deps import get_dispatcher
from app.scheduler import TaskDispatcher

router = APIRouter()


class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str
    timestamp: datetime
    version: str = "0.1.0"


class DetailedHealthResponse(HealthResponse):
    """详细健康检查响应"""
    scheduler_running: bool
    running_tasks: int
    pending_tasks: int
    online_workers: int


@router.get("", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """基础健康检查"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow()
    )


@router.get("/detailed", response_model=DetailedHealthResponse)
async def detailed_health_check(
    dispatcher: TaskDispatcher = Depends(get_dispatcher)
) -> DetailedHealthResponse:
    """详细健康检查"""
    stats = dispatcher.get_stats()
    cluster_stats = stats.get("cluster_stats", {})
    
    return DetailedHealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        scheduler_running=stats.get("is_running", False),
        running_tasks=stats.get("running_tasks", 0),
        pending_tasks=stats.get("pending_tasks", 0),
        online_workers=cluster_stats.get("online_workers", 0)
    )
