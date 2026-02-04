"""
API v1 路由
"""

from fastapi import APIRouter

from app.api.v1 import tasks, workers, health

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(tasks.router, prefix="/tasks", tags=["tasks"])
api_router.include_router(workers.router, prefix="/workers", tags=["workers"])
