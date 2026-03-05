"""
ADMET 自动同步检查器

定时扫描 project_compounds 表，与 admet_compute_result 对比，
对缺少 ADMET 结果的化合物自动提交 QikProp 计算任务。

以 project_compounds.id 为基准进行关联，不再使用 SMILES 去重。
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional

import psycopg2

from app.config import get_settings
from app.models import Task, TaskStatus, TaskPriority, ResourceRequirement
from app.mq.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class AdmetSyncChecker:
    """
    ADMET 自动同步检查器

    职责：
    1. 定时扫描 project_compounds 表，获取所有未删除的化合物
    2. 与 admet_compute_result 表对比（通过 id 关联），找出缺少 ADMET 结果的化合物
    3. 将缺少结果的化合物按批次提交为 ADMET 计算任务
    4. 任务通过 Redis 队列分发，由 ADMET Worker 执行计算并写入结果
    """

    def __init__(self):
        self._settings = get_settings()
        self._running = False
        self._scan_interval = self._settings.admet_sync_interval
        self._batch_size = self._settings.admet_sync_batch_size
        self._task_priority = self._settings.admet_sync_priority

        logger.info(
            f"AdmetSyncChecker 初始化: 扫描间隔={self._scan_interval}s, "
            f"批次大小={self._batch_size}, 任务优先级={self._task_priority}"
        )

    async def start(self) -> None:
        """启动定时扫描循环"""
        if self._running:
            logger.warning("AdmetSyncChecker 已在运行")
            return

        self._running = True
        logger.info("AdmetSyncChecker 已启动")

        while self._running:
            try:
                await self._scan_and_submit()
            except asyncio.CancelledError:
                logger.info("AdmetSyncChecker 收到取消信号")
                break
            except Exception as e:
                logger.error(f"AdmetSyncChecker 扫描异常: {e}", exc_info=True)

            # 等待下一次扫描
            try:
                await asyncio.sleep(self._scan_interval)
            except asyncio.CancelledError:
                break

        logger.info("AdmetSyncChecker 已停止")

    async def stop(self) -> None:
        """停止定时扫描"""
        self._running = False

    async def _scan_and_submit(self) -> None:
        """
        核心扫描逻辑：
        1. 查询 project_compounds 中未删除的化合物
        2. LEFT JOIN admet_compute_result 找出缺少结果的化合物
        3. 按批次提交 ADMET 计算任务
        """
        logger.info("开始 ADMET 同步扫描...")

        # Step 1: 查询缺少 ADMET 结果的化合物（在线程池中执行同步 DB 操作）
        loop = asyncio.get_event_loop()
        missing_compounds = await loop.run_in_executor(None, self._query_missing_compounds)

        if not missing_compounds:
            logger.info("ADMET 同步扫描完成: 所有化合物均已有 ADMET 结果，无需提交任务")
            return

        logger.info(f"发现 {len(missing_compounds)} 个化合物缺少 ADMET 结果，准备提交计算任务")

        # Step 2: 按批次分组并提交任务
        batches = [
            missing_compounds[i:i + self._batch_size]
            for i in range(0, len(missing_compounds), self._batch_size)
        ]

        submitted_count = 0
        for batch_idx, batch in enumerate(batches):
            try:
                task_id = await self._submit_admet_task(batch, batch_idx)
                submitted_count += 1
                logger.info(
                    f"已提交 ADMET 同步任务 {batch_idx + 1}/{len(batches)}: "
                    f"task_id={task_id}, 化合物数量={len(batch)}"
                )
            except Exception as e:
                logger.error(f"提交 ADMET 同步任务 {batch_idx + 1}/{len(batches)} 失败: {e}")

        logger.info(
            f"ADMET 同步扫描完成: 发现 {len(missing_compounds)} 个新化合物, "
            f"提交 {submitted_count}/{len(batches)} 个任务"
        )

    def _query_missing_compounds(self) -> List[dict]:
        """
        查询缺少 ADMET 结果的化合物

        使用 LEFT JOIN 通过 id 关联 admet_compute_result 表，
        找出 project_compounds 中尚无计算结果的记录。

        Returns:
            缺少 ADMET 结果的化合物列表，每项包含 id, smiles, name, project_id
        """
        conn = None
        try:
            settings = self._settings
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db
            )
            cur = conn.cursor()

            sql = """
            SELECT pc.id, pc.smiles, pc.name, pc.project_id
            FROM project_compounds pc
            LEFT JOIN admet_compute_result acr ON pc.id = acr.id
            WHERE pc.deleted = false
              AND acr.id IS NULL
              AND pc.smiles IS NOT NULL
              AND pc.smiles != ''
            """
            cur.execute(sql)
            rows = cur.fetchall()

            missing = [
                {"id": row[0], "smiles": row[1], "name": row[2], "project_id": row[3]}
                for row in rows
            ]
            logger.info(f"数据库查询完成: project_compounds 中有 {len(missing)} 个化合物缺少 ADMET 结果")

            cur.close()
            return missing

        except Exception as e:
            logger.error(f"查询缺少 ADMET 结果的 SMILES 失败: {e}")
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    async def _submit_admet_task(self, compounds: List[dict], batch_index: int) -> str:
        """
        提交一个 ADMET 计算任务到 Redis 队列

        同时将任务记录到 PostgreSQL tasks 表，保持与 API 提交任务一致的行为。

        Args:
            compounds: 本批次要计算的化合物列表，每项包含 id/smiles/name/project_id
            batch_index: 批次编号

        Returns:
            任务 ID
        """
        now = datetime.now()
        task_id = str(uuid.uuid4())
        date_str = now.strftime('%Y%m%d')

        # 提取 SMILES 列表供 QikProp 计算使用
        smiles_list = [c["smiles"] for c in compounds]

        # 构造 Task 对象
        task = Task(
            id=task_id,
            service="admet",
            task_type="qikprop",
            name=f"admet_sync_{date_str}_batch_{batch_index}",
            priority=TaskPriority(self._task_priority),
            status=TaskStatus.PENDING,
            input_params={
                "smiles": smiles_list,
                "compounds": compounds,      # 化合物元数据（id/smiles/name/project_id）
                "source": "admet_sync",
                "batch_index": batch_index,
            },
            resources=ResourceRequirement(
                cpu_cores=1,
                memory_gb=2.0,
                gpu_count=0,
                gpu_memory_gb=0.0,
            ),
            created_at=now,
        )

        # 1. 先保存到 PostgreSQL tasks 表（确保 Worker 消费时能查到任务记录）
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_task_to_db, task)

        # 2. 再推送到 Redis 队列（Worker 才能消费）
        await self._push_to_redis(task)

        return task_id

    async def _push_to_redis(self, task: Task) -> None:
        """将任务推送到 Redis 队列（复用现有的 Redis 数据结构）"""
        redis_client = await get_redis_client()
        redis = await redis_client.get_client()

        task_key = f"aidd:tasks:{task.id}"

        # 序列化任务数据（与 RedisTaskQueue._serialize_task 保持一致）
        task_data = {
            "id": task.id,
            "service": task.service,
            "task_type": task.task_type or "",
            "name": task.name or "",
            "status": task.status.value,
            "priority": str(task.priority.value),
            "input_params": json.dumps(task.input_params),
            "input_files": json.dumps(task.input_files),
            "output_files": json.dumps(task.output_files),
            "result": "",
            "worker_id": "",
            "retry_count": "0",
            "max_retries": str(task.max_retries),
            "timeout_seconds": str(task.timeout_seconds),
            "error_message": "",
            "resource_cpu_cores": str(task.resources.cpu_cores),
            "resource_memory_gb": str(task.resources.memory_gb),
            "resource_gpu_count": str(task.resources.gpu_count),
            "resource_gpu_memory_gb": str(task.resources.gpu_memory_gb),
            "created_at": task.created_at.isoformat() if task.created_at else "",
            "started_at": "",
            "completed_at": "",
        }

        # 计算优先级分数
        priority_base = {0: 0, 1: 100, 2: 200, 3: 300, 4: 400}
        score = priority_base.get(task.priority.value, 400) + (
            datetime.now().timestamp() / 10000000000
        )

        # 原子 Pipeline 操作
        service_queue = f"aidd:queue:service:{task.service}"
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(task_key, mapping=task_data)
            pipe.expire(task_key, 7 * 24 * 3600)
            pipe.zadd("aidd:queue:pending", {task.id: score})
            pipe.lpush(service_queue, task.id)
            pipe.hincrby("aidd:stats", "total_submitted", 1)
            pipe.hincrby("aidd:stats", f"submitted:{task.service}", 1)
            await pipe.execute()

        logger.debug(f"ADMET 同步任务已入队: {task.id}")

    def _save_task_to_db(self, task: Task) -> None:
        """将任务保存到 PostgreSQL tasks 表"""
        conn = None
        try:
            settings = self._settings
            conn = psycopg2.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db
            )
            cur = conn.cursor()

            sql = """
            INSERT INTO tasks (
                id, service, task_type, name, status, priority,
                input_params, input_files, max_retries, timeout_seconds,
                resource_cpu_cores, resource_memory_gb, resource_gpu_count, resource_gpu_memory_gb,
                created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cur.execute(sql, (
                task.id,
                task.service,
                task.task_type or "",
                task.name or "",
                task.status.value,
                task.priority.value,
                json.dumps(task.input_params),
                json.dumps(task.input_files),
                task.max_retries,
                task.timeout_seconds,
                task.resources.cpu_cores,
                task.resources.memory_gb,
                task.resources.gpu_count,
                task.resources.gpu_memory_gb,
                task.created_at or datetime.now()
            ))
            conn.commit()
            cur.close()
            logger.debug(f"ADMET 同步任务已保存到数据库: {task.id}")

        except Exception as e:
            logger.warning(f"ADMET 同步任务保存到数据库失败（不影响队列分发）: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


# =============================================================================
# 单例管理
# =============================================================================

_admet_sync_checker: Optional[AdmetSyncChecker] = None


def get_admet_sync_checker() -> AdmetSyncChecker:
    """获取 AdmetSyncChecker 单例"""
    global _admet_sync_checker
    if _admet_sync_checker is None:
        _admet_sync_checker = AdmetSyncChecker()
    return _admet_sync_checker
