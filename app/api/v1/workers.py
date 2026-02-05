"""
Worker 管理 API
"""

import logging
from typing import List, Optional, Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import uuid4

from app.models import Worker, WorkerStatus, ResourceUsage
from app.api.deps import get_resource_manager
from app.scheduler import ResourceManager

router = APIRouter()
logger = logging.getLogger(__name__)


# =========================================================================
# 请求/响应模型
# =========================================================================

class ResourceUsageSchema(BaseModel):
    """资源使用情况"""
    cpu_cores: int = 0
    memory_gb: float = 0.0
    gpu_count: int = 0
    gpu_memory_gb: float = 0.0


class WorkerRegister(BaseModel):
    """Worker 注册请求"""
    hostname: str = Field(..., description="主机名")
    ip_address: Optional[str] = None
    port: int = Field(default=8080)
    total_cpu: int = Field(..., ge=1, description="总 CPU 核心数")
    total_memory_gb: float = Field(..., ge=0.1, description="总内存(GB)")
    total_gpu: int = Field(default=0, ge=0, description="GPU 数量")
    total_gpu_memory_gb: float = Field(default=0, ge=0)
    supported_services: List[str] = Field(default_factory=list, description="支持的服务类型")
    max_concurrent_tasks: int = Field(default=4, ge=1)
    labels: Dict[str, str] = Field(default_factory=dict, description="标签")

    class Config:
        json_schema_extra = {
            "example": {
                "hostname": "worker-001",
                "ip_address": "192.168.1.100",
                "port": 8080,
                "total_cpu": 16,
                "total_memory_gb": 64.0,
                "total_gpu": 2,
                "total_gpu_memory_gb": 48.0,
                "supported_services": ["admet", "docking"],
                "max_concurrent_tasks": 8,
                "labels": {"zone": "gpu-cluster"}
            }
        }


class WorkerHeartbeat(BaseModel):
    """Worker 心跳请求"""
    used_cpu: int = 0
    used_memory_gb: float = 0.0
    used_gpu: int = 0
    used_gpu_memory_gb: float = 0.0
    current_tasks: List[str] = Field(default_factory=list)


class WorkerResponse(BaseModel):
    """Worker 响应"""
    id: str
    hostname: str
    ip_address: Optional[str]
    port: int
    status: WorkerStatus
    total_resources: ResourceUsageSchema
    used_resources: ResourceUsageSchema
    available_resources: ResourceUsageSchema
    supported_services: List[str]
    max_concurrent_tasks: int
    current_tasks: List[str]
    utilization: float
    registered_at: Optional[datetime]
    last_heartbeat: Optional[datetime]


class WorkerListResponse(BaseModel):
    """Worker 列表响应"""
    total: int
    online: int
    items: List[WorkerResponse]


class ClusterStatsResponse(BaseModel):
    """集群统计响应"""
    total_workers: int
    online_workers: int
    total_cpu: int
    used_cpu: int
    available_cpu: int
    total_memory_gb: float
    used_memory_gb: float
    available_memory_gb: float
    total_gpu: int
    used_gpu: int
    available_gpu: int


# =========================================================================
# API 端点
# =========================================================================

def _save_worker_to_db(worker: Worker, request: WorkerRegister) -> bool:
    """将 Worker 保存到 PostgreSQL"""
    try:
        import psycopg2
        import json
        from app.config import get_settings
        settings = get_settings()
        
        conn = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )
        cur = conn.cursor()
        
        # 使用 UPSERT
        sql = """
        INSERT INTO workers (
            id, hostname, ip_address, port, status,
            total_cpu_cores, total_memory_gb, total_gpu_count, total_gpu_memory_gb,
            used_cpu_cores, used_memory_gb, used_gpu_count, used_gpu_memory_gb,
            supported_services, max_concurrent_tasks, labels,
            registered_at, last_heartbeat
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            hostname = EXCLUDED.hostname,
            ip_address = EXCLUDED.ip_address,
            port = EXCLUDED.port,
            status = EXCLUDED.status,
            total_cpu_cores = EXCLUDED.total_cpu_cores,
            total_memory_gb = EXCLUDED.total_memory_gb,
            total_gpu_count = EXCLUDED.total_gpu_count,
            total_gpu_memory_gb = EXCLUDED.total_gpu_memory_gb,
            supported_services = EXCLUDED.supported_services,
            max_concurrent_tasks = EXCLUDED.max_concurrent_tasks,
            labels = EXCLUDED.labels,
            last_heartbeat = EXCLUDED.last_heartbeat,
            updated_at = CURRENT_TIMESTAMP
        """
        cur.execute(sql, (
            worker.id,
            worker.hostname,
            worker.ip_address,
            worker.port,
            worker.status.value,
            request.total_cpu,
            request.total_memory_gb,
            request.total_gpu,
            request.total_gpu_memory_gb,
            0, 0.0, 0, 0.0,  # used resources
            request.supported_services,
            request.max_concurrent_tasks,
            json.dumps(request.labels) if request.labels else None,
            worker.registered_at,
            worker.last_heartbeat
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"保存 Worker 到数据库失败: {e}")
        return False


def _update_worker_heartbeat_db(worker_id: str, used_cpu: int, used_memory_gb: float, 
                                  used_gpu: int, status: str) -> bool:
    """更新 Worker 心跳到数据库"""
    try:
        import psycopg2
        from datetime import datetime
        from app.config import get_settings
        settings = get_settings()
        
        conn = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )
        cur = conn.cursor()
        
        sql = """
        UPDATE workers SET
            used_cpu_cores = %s,
            used_memory_gb = %s,
            used_gpu_count = %s,
            status = %s,
            last_heartbeat = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        cur.execute(sql, (used_cpu, used_memory_gb, used_gpu, status, datetime.utcnow(), worker_id))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"更新 Worker 心跳到数据库失败: {e}")
        return False


def _set_worker_offline_db(worker_id: str) -> bool:
    """标记 Worker 下线"""
    try:
        import psycopg2
        from app.config import get_settings
        settings = get_settings()
        
        conn = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )
        cur = conn.cursor()
        cur.execute("UPDATE workers SET status = 'offline', updated_at = CURRENT_TIMESTAMP WHERE id = %s", (worker_id,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"标记 Worker 下线失败: {e}")
        return False


@router.post("/register", response_model=WorkerResponse, status_code=status.HTTP_201_CREATED)
async def register_worker(
    request: WorkerRegister,
    resource_manager: ResourceManager = Depends(get_resource_manager)
) -> WorkerResponse:
    """
    注册新 Worker
    
    Worker 启动时调用此接口注册自己，同时持久化到数据库
    """
    worker = Worker(
        id=str(uuid4()),
        hostname=request.hostname,
        ip_address=request.ip_address,
        port=request.port,
        total_resources=ResourceUsage(
            cpu_cores=request.total_cpu,
            memory_gb=request.total_memory_gb,
            gpu_count=request.total_gpu,
            gpu_memory_gb=request.total_gpu_memory_gb
        ),
        supported_services=request.supported_services,
        max_concurrent_tasks=request.max_concurrent_tasks,
        labels=request.labels
    )
    
    # 注册到内存
    resource_manager.register_worker(worker)
    
    # 持久化到数据库
    _save_worker_to_db(worker, request)
    
    logger.info(f"Worker 注册成功: {worker.id} ({worker.hostname})")
    return _worker_to_response(worker)


@router.post("/{worker_id}/heartbeat", response_model=WorkerResponse)
async def worker_heartbeat(
    worker_id: str,
    request: WorkerHeartbeat,
    resource_manager: ResourceManager = Depends(get_resource_manager)
) -> WorkerResponse:
    """
    Worker 心跳
    
    Worker 定期调用此接口报告状态，同时更新数据库
    """
    success = resource_manager.update_heartbeat(
        worker_id=worker_id,
        used_cpu=request.used_cpu,
        used_memory_gb=request.used_memory_gb,
        used_gpu=request.used_gpu,
        current_tasks=request.current_tasks
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker {worker_id} not found"
        )
    
    worker = resource_manager.get_worker(worker_id)
    
    # 同步到数据库
    _update_worker_heartbeat_db(worker_id, request.used_cpu, request.used_memory_gb, 
                                 request.used_gpu, worker.status.value)
    
    return _worker_to_response(worker)


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unregister_worker(
    worker_id: str,
    resource_manager: ResourceManager = Depends(get_resource_manager)
) -> None:
    """注销 Worker，同时更新数据库状态为 offline"""
    worker = resource_manager.unregister_worker(worker_id)
    
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker {worker_id} not found"
        )
    
    # 标记数据库中的 Worker 为 offline
    _set_worker_offline_db(worker_id)
    logger.info(f"Worker 已注销: {worker_id}")


@router.get("", response_model=WorkerListResponse)
async def list_workers(
    status: Optional[WorkerStatus] = Query(None, description="按状态筛选"),
    service: Optional[str] = Query(None, description="按支持的服务筛选"),
    resource_manager: ResourceManager = Depends(get_resource_manager)
) -> WorkerListResponse:
    """获取 Worker 列表"""
    workers = resource_manager.get_all_workers()
    
    # 筛选
    if status:
        workers = [w for w in workers if w.status == status]
    if service:
        workers = [w for w in workers if w.can_handle_service(service)]
    
    online = sum(1 for w in workers if w.status != WorkerStatus.OFFLINE)
    
    items = [_worker_to_response(w) for w in workers]
    
    return WorkerListResponse(
        total=len(items),
        online=online,
        items=items
    )


@router.get("/stats", response_model=ClusterStatsResponse)
async def get_cluster_stats(
    resource_manager: ResourceManager = Depends(get_resource_manager)
) -> ClusterStatsResponse:
    """获取集群资源统计"""
    stats = resource_manager.get_cluster_stats()
    return ClusterStatsResponse(**stats)


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id: str,
    resource_manager: ResourceManager = Depends(get_resource_manager)
) -> WorkerResponse:
    """获取 Worker 详情"""
    worker = resource_manager.get_worker(worker_id)
    
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker {worker_id} not found"
        )
    
    return _worker_to_response(worker)


# =========================================================================
# 辅助函数
# =========================================================================

def _worker_to_response(worker: Worker) -> WorkerResponse:
    """将 Worker 转换为响应模型"""
    available = worker.available_resources
    
    return WorkerResponse(
        id=worker.id,
        hostname=worker.hostname,
        ip_address=worker.ip_address,
        port=worker.port,
        status=worker.status,
        total_resources=ResourceUsageSchema(
            cpu_cores=worker.total_resources.cpu_cores,
            memory_gb=worker.total_resources.memory_gb,
            gpu_count=worker.total_resources.gpu_count,
            gpu_memory_gb=worker.total_resources.gpu_memory_gb
        ),
        used_resources=ResourceUsageSchema(
            cpu_cores=worker.used_resources.cpu_cores,
            memory_gb=worker.used_resources.memory_gb,
            gpu_count=worker.used_resources.gpu_count,
            gpu_memory_gb=worker.used_resources.gpu_memory_gb
        ),
        available_resources=ResourceUsageSchema(
            cpu_cores=available.cpu_cores,
            memory_gb=available.memory_gb,
            gpu_count=available.gpu_count,
            gpu_memory_gb=available.gpu_memory_gb
        ),
        supported_services=worker.supported_services,
        max_concurrent_tasks=worker.max_concurrent_tasks,
        current_tasks=worker.current_tasks,
        utilization=worker.utilization,
        registered_at=worker.registered_at,
        last_heartbeat=worker.last_heartbeat
    )
