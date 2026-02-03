"""
优先级任务队列
支持多优先级、多服务类型的任务队列
"""

import heapq
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum

from app.models import Task, TaskPriority, TaskStatus

logger = logging.getLogger(__name__)


class QueuePriority(IntEnum):
    """
    队列优先级
    数值越小优先级越高
    """
    CRITICAL = 0    # 紧急任务
    HIGH = 1        # 高优先级
    NORMAL = 2      # 普通优先级
    LOW = 3         # 低优先级
    BATCH = 4       # 批处理任务


@dataclass(order=True)
class QueueItem:
    """
    队列项
    使用 dataclass 实现自动比较
    """
    priority: int                    # 优先级分数（越小越优先）
    created_at: datetime             # 创建时间
    task: Task = field(compare=False)  # 任务对象
    
    @classmethod
    def from_task(cls, task: Task) -> "QueueItem":
        """从 Task 创建队列项"""
        # 计算优先级分数
        priority_map = {
            TaskPriority.CRITICAL: QueuePriority.CRITICAL,
            TaskPriority.HIGH: QueuePriority.HIGH,
            TaskPriority.NORMAL: QueuePriority.NORMAL,
            TaskPriority.LOW: QueuePriority.LOW,
            TaskPriority.BATCH: QueuePriority.BATCH,
        }
        priority = priority_map.get(task.priority, QueuePriority.NORMAL)
        
        return cls(
            priority=priority,
            created_at=task.created_at or datetime.utcnow(),
            task=task
        )


class PriorityTaskQueue:
    """
    优先级任务队列
    
    特性：
    1. 多优先级支持
    2. 按服务类型分组
    3. 公平调度（防止低优先级任务饿死）
    4. 任务去重
    """
    
    def __init__(self, starvation_threshold: int = 100):
        """
        Args:
            starvation_threshold: 饥饿阈值，当低优先级任务等待超过该次数时提升优先级
        """
        # 主队列：按服务类型分组
        self._queues: Dict[str, List[QueueItem]] = {}
        
        # 任务ID集合：用于去重
        self._task_ids: Set[str] = set()
        
        # 等待计数器：用于防止饥饿
        self._wait_counts: Dict[str, int] = {}
        
        # 饥饿阈值
        self._starvation_threshold = starvation_threshold
    
    # =========================================================================
    # 基础操作
    # =========================================================================
    
    def push(self, task: Task) -> bool:
        """
        添加任务到队列
        
        Args:
            task: 任务对象
        
        Returns:
            是否添加成功（如果已存在则返回 False）
        """
        if task.id in self._task_ids:
            logger.warning(f"Task already in queue: {task.id}")
            return False
        
        # 获取或创建服务队列
        service = task.service
        if service not in self._queues:
            self._queues[service] = []
        
        # 创建队列项并加入
        item = QueueItem.from_task(task)
        heapq.heappush(self._queues[service], item)
        self._task_ids.add(task.id)
        self._wait_counts[task.id] = 0
        
        logger.debug(f"Task enqueued: {task.id} (service={service}, priority={task.priority})")
        return True
    
    def pop(self, service: Optional[str] = None) -> Optional[Task]:
        """
        从队列中取出优先级最高的任务
        
        Args:
            service: 指定服务类型，如果为 None 则从所有队列中选择
        
        Returns:
            任务对象，如果队列为空则返回 None
        """
        if service:
            return self._pop_from_service(service)
        else:
            return self._pop_global()
    
    def _pop_from_service(self, service: str) -> Optional[Task]:
        """从指定服务队列取任务"""
        queue = self._queues.get(service)
        if not queue:
            return None
        
        while queue:
            item = heapq.heappop(queue)
            task = item.task
            
            # 检查任务是否仍有效
            if task.id in self._task_ids:
                self._task_ids.remove(task.id)
                self._wait_counts.pop(task.id, None)
                return task
        
        return None
    
    def _pop_global(self) -> Optional[Task]:
        """从所有队列中选择优先级最高的任务"""
        best_item: Optional[QueueItem] = None
        best_service: Optional[str] = None
        
        for service, queue in self._queues.items():
            if not queue:
                continue
            
            # 查看队首但不弹出
            item = queue[0]
            
            # 应用饥饿补偿
            adjusted_priority = self._get_adjusted_priority(item)
            
            if best_item is None or adjusted_priority < self._get_adjusted_priority(best_item):
                best_item = item
                best_service = service
        
        if best_service:
            # 更新所有任务的等待计数
            self._increment_wait_counts(best_service)
            return self._pop_from_service(best_service)
        
        return None
    
    def _get_adjusted_priority(self, item: QueueItem) -> float:
        """获取调整后的优先级（考虑饥饿补偿）"""
        wait_count = self._wait_counts.get(item.task.id, 0)
        
        # 每等待 threshold 次，优先级提升一级
        boost = wait_count // self._starvation_threshold
        return item.priority - (boost * 0.5)
    
    def _increment_wait_counts(self, excluded_service: str) -> None:
        """增加非选中服务的等待计数"""
        for service, queue in self._queues.items():
            if service == excluded_service:
                continue
            for item in queue:
                if item.task.id in self._wait_counts:
                    self._wait_counts[item.task.id] += 1
    
    def peek(self, service: Optional[str] = None) -> Optional[Task]:
        """
        查看队首任务但不移除
        
        Args:
            service: 指定服务类型
        
        Returns:
            任务对象
        """
        if service:
            queue = self._queues.get(service)
            if queue:
                return queue[0].task
            return None
        else:
            best_item = None
            for queue in self._queues.values():
                if queue and (best_item is None or queue[0] < best_item):
                    best_item = queue[0]
            return best_item.task if best_item else None
    
    def remove(self, task_id: str) -> bool:
        """
        从队列中移除指定任务
        
        Args:
            task_id: 任务ID
        
        Returns:
            是否移除成功
        """
        if task_id not in self._task_ids:
            return False
        
        self._task_ids.remove(task_id)
        self._wait_counts.pop(task_id, None)
        
        # 注意：heapq 不支持高效删除中间元素
        # 我们只从 ID 集合中移除，实际弹出时会跳过
        logger.debug(f"Task marked for removal: {task_id}")
        return True
    
    def contains(self, task_id: str) -> bool:
        """检查任务是否在队列中"""
        return task_id in self._task_ids
    
    # =========================================================================
    # 批量操作
    # =========================================================================
    
    def push_batch(self, tasks: List[Task]) -> int:
        """
        批量添加任务
        
        Returns:
            成功添加的任务数量
        """
        count = 0
        for task in tasks:
            if self.push(task):
                count += 1
        return count
    
    def pop_batch(
        self,
        count: int,
        service: Optional[str] = None
    ) -> List[Task]:
        """
        批量取出任务
        
        Args:
            count: 最多取出的任务数量
            service: 指定服务类型
        
        Returns:
            任务列表
        """
        tasks = []
        for _ in range(count):
            task = self.pop(service)
            if task is None:
                break
            tasks.append(task)
        return tasks
    
    # =========================================================================
    # 统计信息
    # =========================================================================
    
    def size(self, service: Optional[str] = None) -> int:
        """获取队列大小"""
        if service:
            queue = self._queues.get(service)
            if queue:
                return sum(1 for item in queue if item.task.id in self._task_ids)
            return 0
        else:
            return len(self._task_ids)
    
    def is_empty(self, service: Optional[str] = None) -> bool:
        """检查队列是否为空"""
        return self.size(service) == 0
    
    def get_services(self) -> List[str]:
        """获取所有有任务的服务类型"""
        return [s for s, q in self._queues.items() if q and any(
            item.task.id in self._task_ids for item in q
        )]
    
    def get_stats(self) -> Dict:
        """获取队列统计信息"""
        stats = {
            "total_tasks": len(self._task_ids),
            "services": {},
        }
        
        for service in self._queues:
            count = self.size(service)
            if count > 0:
                stats["services"][service] = count
        
        # 按优先级统计
        priority_counts = {p.name: 0 for p in TaskPriority}
        for service, queue in self._queues.items():
            for item in queue:
                if item.task.id in self._task_ids:
                    priority_counts[item.task.priority.name] += 1
        
        stats["by_priority"] = priority_counts
        return stats
    
    def clear(self, service: Optional[str] = None) -> int:
        """
        清空队列
        
        Args:
            service: 指定服务类型，如果为 None 则清空所有
        
        Returns:
            清除的任务数量
        """
        if service:
            queue = self._queues.get(service)
            if not queue:
                return 0
            
            count = 0
            for item in queue:
                if item.task.id in self._task_ids:
                    self._task_ids.remove(item.task.id)
                    self._wait_counts.pop(item.task.id, None)
                    count += 1
            
            self._queues[service] = []
            return count
        else:
            count = len(self._task_ids)
            self._queues.clear()
            self._task_ids.clear()
            self._wait_counts.clear()
            return count
