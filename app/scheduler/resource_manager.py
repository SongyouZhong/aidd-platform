"""
资源管理器
负责追踪和分配计算资源
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from app.models import Worker, WorkerStatus, ResourceRequirement
from app.config import get_settings

logger = logging.getLogger(__name__)


class ResourceManager:
    """
    资源管理器
    
    职责：
    1. 维护所有 Worker 的资源状态
    2. 为任务查找合适的 Worker
    3. 分配和释放资源
    """
    
    def __init__(self):
        self._workers: Dict[str, Worker] = {}
        self._settings = get_settings()
    
    # =========================================================================
    # Worker 管理
    # =========================================================================
    
    def register_worker(self, worker: Worker) -> None:
        """注册 Worker"""
        worker.registered_at = datetime.utcnow()
        worker.last_heartbeat = datetime.utcnow()
        worker.status = WorkerStatus.ONLINE
        self._workers[worker.id] = worker
        logger.info(f"Worker registered: {worker.id} ({worker.hostname})")
    
    def unregister_worker(self, worker_id: str) -> Optional[Worker]:
        """注销 Worker"""
        worker = self._workers.pop(worker_id, None)
        if worker:
            logger.info(f"Worker unregistered: {worker_id}")
        return worker
    
    def get_worker(self, worker_id: str) -> Optional[Worker]:
        """获取 Worker"""
        return self._workers.get(worker_id)
    
    def get_all_workers(self) -> List[Worker]:
        """获取所有 Worker"""
        return list(self._workers.values())
    
    def get_online_workers(self) -> List[Worker]:
        """获取所有在线 Worker"""
        return [w for w in self._workers.values() if w.status != WorkerStatus.OFFLINE]
    
    # =========================================================================
    # 心跳处理
    # =========================================================================
    
    def update_heartbeat(
        self,
        worker_id: str,
        used_cpu: int = 0,
        used_memory_gb: float = 0,
        used_gpu: int = 0,
        current_tasks: Optional[List[str]] = None
    ) -> bool:
        """
        更新 Worker 心跳
        
        Returns:
            是否更新成功
        """
        worker = self._workers.get(worker_id)
        if not worker:
            return False
        
        worker.last_heartbeat = datetime.utcnow()
        worker.used_resources.cpu_cores = used_cpu
        worker.used_resources.memory_gb = used_memory_gb
        worker.used_resources.gpu_count = used_gpu
        
        if current_tasks is not None:
            worker.current_tasks = current_tasks
        
        # 更新状态
        if worker.status == WorkerStatus.OFFLINE:
            worker.status = WorkerStatus.ONLINE
        
        if len(worker.current_tasks) >= worker.max_concurrent_tasks:
            worker.status = WorkerStatus.BUSY
        elif worker.status == WorkerStatus.BUSY:
            worker.status = WorkerStatus.ONLINE
        
        return True
    
    def check_heartbeat_timeout(self) -> List[str]:
        """
        检查心跳超时的 Worker
        
        Returns:
            超时的 Worker ID 列表
        """
        timeout = timedelta(seconds=self._settings.heartbeat_timeout)
        now = datetime.utcnow()
        timed_out = []
        
        for worker_id, worker in self._workers.items():
            if worker.last_heartbeat and (now - worker.last_heartbeat) > timeout:
                if worker.status != WorkerStatus.OFFLINE:
                    worker.status = WorkerStatus.OFFLINE
                    timed_out.append(worker_id)
                    logger.warning(f"Worker heartbeat timeout: {worker_id}")
        
        return timed_out
    
    # =========================================================================
    # 资源分配
    # =========================================================================
    
    def find_available_worker(
        self,
        requirement: ResourceRequirement,
        service: str,
        preferred_worker_id: Optional[str] = None
    ) -> Optional[Worker]:
        """
        查找满足资源需求的 Worker
        
        Args:
            requirement: 资源需求
            service: 服务类型
            preferred_worker_id: 优先选择的 Worker（用于任务亲和性）
        
        Returns:
            合适的 Worker，如果没有则返回 None
        """
        # 如果指定了优先 Worker，先检查它
        if preferred_worker_id:
            worker = self._workers.get(preferred_worker_id)
            if worker and self._can_accept_task(worker, requirement, service):
                return worker
        
        # 查找所有满足条件的 Worker
        candidates = []
        for worker in self._workers.values():
            if self._can_accept_task(worker, requirement, service):
                candidates.append(worker)
        
        if not candidates:
            return None
        
        # 调度策略：选择负载最低的 Worker
        return min(candidates, key=lambda w: w.utilization)
    
    def _can_accept_task(
        self,
        worker: Worker,
        requirement: ResourceRequirement,
        service: str
    ) -> bool:
        """检查 Worker 是否能接受任务"""
        # 检查状态
        if not worker.is_available:
            return False
        
        # 检查服务支持
        if not worker.can_handle_service(service):
            return False
        
        # 检查资源
        available = worker.available_resources
        return requirement.can_fit_in(available)
    
    def allocate_resources(
        self,
        worker_id: str,
        task_id: str,
        requirement: ResourceRequirement
    ) -> bool:
        """
        分配资源
        
        Args:
            worker_id: Worker ID
            task_id: 任务 ID
            requirement: 资源需求
        
        Returns:
            是否分配成功
        """
        worker = self._workers.get(worker_id)
        if not worker:
            return False
        
        # 更新已用资源
        worker.used_resources.cpu_cores += requirement.cpu_cores
        worker.used_resources.memory_gb += requirement.memory_gb
        worker.used_resources.gpu_count += requirement.gpu_count
        worker.used_resources.gpu_memory_gb += requirement.gpu_memory_gb
        
        # 添加任务
        worker.add_task(task_id)
        
        logger.debug(
            f"Resources allocated: {task_id} -> {worker_id} "
            f"(CPU: {requirement.cpu_cores}, Mem: {requirement.memory_gb}GB)"
        )
        return True
    
    def release_resources(
        self,
        worker_id: str,
        task_id: str,
        requirement: ResourceRequirement
    ) -> bool:
        """
        释放资源
        
        Args:
            worker_id: Worker ID
            task_id: 任务 ID
            requirement: 资源需求
        
        Returns:
            是否释放成功
        """
        worker = self._workers.get(worker_id)
        if not worker:
            return False
        
        # 更新已用资源
        worker.used_resources.cpu_cores = max(
            0, worker.used_resources.cpu_cores - requirement.cpu_cores
        )
        worker.used_resources.memory_gb = max(
            0, worker.used_resources.memory_gb - requirement.memory_gb
        )
        worker.used_resources.gpu_count = max(
            0, worker.used_resources.gpu_count - requirement.gpu_count
        )
        worker.used_resources.gpu_memory_gb = max(
            0, worker.used_resources.gpu_memory_gb - requirement.gpu_memory_gb
        )
        
        # 移除任务
        worker.remove_task(task_id)
        
        logger.debug(f"Resources released: {task_id} from {worker_id}")
        return True
    
    # =========================================================================
    # 统计信息
    # =========================================================================
    
    def get_cluster_stats(self) -> Dict:
        """获取集群资源统计"""
        total_cpu = 0
        used_cpu = 0
        total_memory = 0.0
        used_memory = 0.0
        total_gpu = 0
        used_gpu = 0
        online_workers = 0
        
        for worker in self._workers.values():
            if worker.status != WorkerStatus.OFFLINE:
                online_workers += 1
                total_cpu += worker.total_resources.cpu_cores
                used_cpu += worker.used_resources.cpu_cores
                total_memory += worker.total_resources.memory_gb
                used_memory += worker.used_resources.memory_gb
                total_gpu += worker.total_resources.gpu_count
                used_gpu += worker.used_resources.gpu_count
        
        return {
            "total_workers": len(self._workers),
            "online_workers": online_workers,
            "total_cpu": total_cpu,
            "used_cpu": used_cpu,
            "available_cpu": total_cpu - used_cpu,
            "total_memory_gb": total_memory,
            "used_memory_gb": used_memory,
            "available_memory_gb": total_memory - used_memory,
            "total_gpu": total_gpu,
            "used_gpu": used_gpu,
            "available_gpu": total_gpu - used_gpu,
        }
