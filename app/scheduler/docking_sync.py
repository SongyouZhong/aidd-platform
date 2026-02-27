"""
Docking 自动同步检查器

定时扫描 project_receptors 表中有活跃受体文件的项目，
对每个 (project_id, receptor_id) 组合，检查 project_compounds 中的化合物
是否都有对应的 docking_result 记录。对缺失的组合自动提交 Glide docking 任务。

工作原理（与 AdmetSyncChecker 类似）:
1. 查询 project_receptors 中 is_active=true 的受体文件
2. 对每个受体，LEFT JOIN docking_result 找出缺少结果的化合物 SMILES
3. 对缺失的 (smiles, receptor_id) 组合，预插入 status='pending' 的 docking_result 记录
4. 按批次提交 docking 任务到 Redis 队列
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Tuple, Dict

import psycopg2

from app.config import get_settings
from app.models import Task, TaskStatus, TaskPriority, ResourceRequirement
from app.mq.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class DockingSyncChecker:
    """
    Docking 自动同步检查器

    职责：
    1. 定时扫描有活跃受体文件的项目
    2. 对每个受体，找出缺少 docking 结果的化合物
    3. 预插入 pending 记录防止重复提交
    4. 将缺失的化合物按批次提交为 Glide docking 任务
    """

    def __init__(self):
        self._settings = get_settings()
        self._running = False
        self._scan_interval = self._settings.docking_sync_interval
        self._batch_size = self._settings.docking_sync_batch_size
        self._task_priority = self._settings.docking_sync_priority

        logger.info(
            f"DockingSyncChecker 初始化: 扫描间隔={self._scan_interval}s, "
            f"批次大小={self._batch_size}, 任务优先级={self._task_priority}"
        )

    async def start(self) -> None:
        """启动定时扫描循环"""
        if self._running:
            logger.warning("DockingSyncChecker 已在运行")
            return

        self._running = True
        logger.info("DockingSyncChecker 已启动")

        while self._running:
            try:
                await self._scan_and_submit()
            except asyncio.CancelledError:
                logger.info("DockingSyncChecker 收到取消信号")
                break
            except Exception as e:
                logger.error(f"DockingSyncChecker 扫描异常: {e}", exc_info=True)

            try:
                await asyncio.sleep(self._scan_interval)
            except asyncio.CancelledError:
                break

        logger.info("DockingSyncChecker 已停止")

    async def stop(self) -> None:
        """停止定时扫描"""
        self._running = False

    async def _scan_and_submit(self) -> None:
        """
        核心扫描逻辑：
        1. 查询所有活跃的受体文件
        2. 对每个受体，找出缺少 docking 结果的化合物 SMILES
        3. 预插入 pending 记录
        4. 按批次提交 docking 任务
        """
        logger.info("开始 Docking 同步扫描...")

        loop = asyncio.get_event_loop()

        # Step 1: 获取所有活跃受体
        active_receptors = await loop.run_in_executor(None, self._query_active_receptors)
        if not active_receptors:
            logger.info("Docking 同步扫描完成: 无活跃受体文件，跳过")
            return

        logger.info(f"发现 {len(active_receptors)} 个活跃受体文件")

        total_submitted = 0
        for receptor in active_receptors:
            receptor_id = receptor["id"]
            project_id = receptor["project_id"]
            minio_path = receptor["minio_path"]
            receptor_name = receptor["name"]

            # Step 2: 查询该受体缺失 docking 结果的化合物
            missing_smiles = await loop.run_in_executor(
                None, self._query_missing_smiles, project_id, receptor_id
            )

            if not missing_smiles:
                logger.debug(
                    f"受体 {receptor_name}({receptor_id}): 所有化合物均已有 docking 结果"
                )
                continue

            logger.info(
                f"受体 {receptor_name}({receptor_id}): "
                f"发现 {len(missing_smiles)} 个化合物缺少 docking 结果"
            )

            # Step 3: 预插入 pending 记录（防止下次扫描重复提交）
            await loop.run_in_executor(
                None, self._insert_pending_records, project_id, receptor_id, missing_smiles
            )

            # Step 4: 按批次提交任务
            batches = [
                missing_smiles[i:i + self._batch_size]
                for i in range(0, len(missing_smiles), self._batch_size)
            ]

            for batch_idx, batch in enumerate(batches):
                try:
                    task_id = await self._submit_docking_task(
                        batch, project_id, receptor_id, minio_path, batch_idx
                    )
                    total_submitted += 1
                    logger.info(
                        f"已提交 Docking 任务 {batch_idx + 1}/{len(batches)}: "
                        f"task_id={task_id}, 受体={receptor_name}, SMILES={len(batch)}"
                    )

                    # 更新 pending 记录中的 task_id
                    await loop.run_in_executor(
                        None, self._update_pending_task_id,
                        receptor_id, batch, task_id
                    )
                except Exception as e:
                    logger.error(
                        f"提交 Docking 任务失败 (受体={receptor_name}, batch={batch_idx}): {e}"
                    )
                    # 回滚该 batch 的 pending 记录，避免孤儿记录阻塞后续扫描
                    await loop.run_in_executor(
                        None, self._rollback_pending_records,
                        receptor_id, batch
                    )

        logger.info(f"Docking 同步扫描完成: 共提交 {total_submitted} 个任务")

    # =========================================================================
    # 数据库查询方法（同步，在线程池中执行）
    # =========================================================================

    def _get_connection(self):
        """获取数据库连接"""
        settings = self._settings
        return psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

    def _query_active_receptors(self) -> List[Dict]:
        """查询所有活跃的受体文件"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, project_id, name, minio_path
                FROM project_receptors
                WHERE is_active = true
            """)
            rows = cur.fetchall()
            cur.close()

            receptors = [
                {"id": row[0], "project_id": row[1], "name": row[2], "minio_path": row[3]}
                for row in rows
            ]
            logger.debug(f"查询到 {len(receptors)} 个活跃受体文件")
            return receptors
        except Exception as e:
            logger.error(f"查询活跃受体文件失败: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def _query_missing_smiles(self, project_id: str, receptor_id: str) -> List[str]:
        """
        查询指定项目+受体组合中缺少 docking 结果的化合物 SMILES

        使用 LEFT JOIN 在一条 SQL 中完成差集计算。
        排除已有 pending/computing/success 状态记录的 SMILES。
        """
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            sql = """
            SELECT DISTINCT pc.smiles
            FROM project_compounds pc
            LEFT JOIN docking_result dr
                ON pc.smiles = dr.smiles
                AND dr.receptor_id = %s
                AND dr.status IN ('pending', 'computing', 'success')
            WHERE pc.project_id = %s
              AND pc.deleted = false
              AND dr.smiles IS NULL
              AND pc.smiles IS NOT NULL
              AND pc.smiles != ''
            """
            cur.execute(sql, (receptor_id, project_id))
            rows = cur.fetchall()
            cur.close()

            missing = [row[0] for row in rows]
            return missing
        except Exception as e:
            logger.error(f"查询缺失 docking 结果失败 (project={project_id}, receptor={receptor_id}): {e}")
            return []
        finally:
            if conn:
                conn.close()

    def _insert_pending_records(
        self, project_id: str, receptor_id: str, smiles_list: List[str]
    ) -> None:
        """
        预插入 status='pending' 的 docking_result 记录

        使用 ON CONFLICT DO NOTHING 避免重复插入（唯一约束: receptor_id + smiles）。
        """
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            sql = """
            INSERT INTO docking_result (id, project_id, receptor_id, smiles, status, created_at)
            VALUES (gen_random_uuid(), %s, %s, %s, 'pending', NOW())
            ON CONFLICT (receptor_id, smiles) DO NOTHING
            """
            for smiles in smiles_list:
                cur.execute(sql, (project_id, receptor_id, smiles))

            conn.commit()
            cur.close()
            logger.debug(f"已预插入 {len(smiles_list)} 条 pending docking_result 记录")
        except Exception as e:
            logger.error(f"预插入 pending 记录失败: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def _update_pending_task_id(
        self, receptor_id: str, smiles_list: List[str], task_id: str
    ) -> None:
        """更新 pending 记录的 task_id，方便追踪"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            # 使用 IN 子句批量更新
            placeholders = ",".join(["%s"] * len(smiles_list))
            sql = f"""
            UPDATE docking_result
            SET task_id = %s, status = 'pending', updated_at = NOW()
            WHERE receptor_id = %s
              AND smiles IN ({placeholders})
              AND status = 'pending'
            """
            cur.execute(sql, [task_id, receptor_id] + smiles_list)
            conn.commit()
            cur.close()
        except Exception as e:
            logger.warning(f"更新 pending 记录的 task_id 失败: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def _rollback_pending_records(
        self, receptor_id: str, smiles_list: List[str]
    ) -> None:
        """
        回滚孤儿 pending 记录：删除无 task_id 的 pending 记录，
        避免 Redis 提交失败后阻塞后续扫描重新检测。
        """
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            placeholders = ",".join(["%s"] * len(smiles_list))
            sql = f"""
            DELETE FROM docking_result
            WHERE receptor_id = %s
              AND smiles IN ({placeholders})
              AND status = 'pending'
              AND task_id IS NULL
            """
            cur.execute(sql, [receptor_id] + smiles_list)
            deleted = cur.rowcount
            conn.commit()
            cur.close()
            logger.info(f"已回滚 {deleted} 条孤儿 pending 记录 (receptor={receptor_id})")
        except Exception as e:
            logger.warning(f"回滚 pending 记录失败: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    # =========================================================================
    # 任务提交
    # =========================================================================

    async def _submit_docking_task(
        self,
        smiles_list: List[str],
        project_id: str,
        receptor_id: str,
        receptor_minio_path: str,
        batch_index: int,
    ) -> str:
        """
        提交一个 Glide docking 任务到 Redis 队列

        同时将任务记录到 PostgreSQL tasks 表。
        """
        now = datetime.now()
        task_id = str(uuid.uuid4())
        date_str = now.strftime("%Y%m%d")

        task = Task(
            id=task_id,
            service="docking",
            task_type="glide",
            name=f"docking_sync_{date_str}_receptor_{receptor_id[:8]}_batch_{batch_index}",
            priority=TaskPriority(self._task_priority),
            status=TaskStatus.PENDING,
            input_params={
                "smiles_list": smiles_list,
                "receptor_id": receptor_id,
                "receptor_minio_path": receptor_minio_path,
                "project_id": project_id,
                "source": "docking_sync",
                "batch_index": batch_index,
            },
            input_files=[receptor_minio_path],
            resources=ResourceRequirement(
                cpu_cores=4,
                memory_gb=8.0,
                gpu_count=0,
                gpu_memory_gb=0.0,
                estimated_time_seconds=7200,
            ),
            timeout_seconds=7200,  # Glide 计算可能较慢，2小时超时
            created_at=now,
        )

        # 1. 推送到 Redis 队列
        await self._push_to_redis(task)

        # 2. 保存到 PostgreSQL tasks 表
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_task_to_db, task)

        return task_id

    async def _push_to_redis(self, task: Task) -> None:
        """将任务推送到 Redis 队列（与 AdmetSyncChecker 相同的逻辑）"""
        redis_client = await get_redis_client()
        redis = await redis_client.get_client()

        task_key = f"aidd:tasks:{task.id}"

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

        # 计算优先级分数（与 AdmetSyncChecker 一致）
        priority_base = {0: 0, 1: 100, 2: 200, 3: 300, 4: 400}
        score = priority_base.get(task.priority.value, 400) + (
            datetime.now().timestamp() / 10000000000
        )

        service_queue = f"aidd:queue:service:{task.service}"
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(task_key, mapping=task_data)
            pipe.expire(task_key, 7 * 24 * 3600)
            pipe.zadd("aidd:queue:pending", {task.id: score})
            pipe.lpush(service_queue, task.id)
            pipe.hincrby("aidd:stats", "total_submitted", 1)
            pipe.hincrby("aidd:stats", f"submitted:{task.service}", 1)
            await pipe.execute()

        logger.debug(f"Docking 同步任务已入队: {task.id}")

    def _save_task_to_db(self, task: Task) -> None:
        """将任务保存到 PostgreSQL tasks 表"""
        conn = None
        try:
            conn = self._get_connection()
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
            logger.debug(f"Docking 同步任务已保存到数据库: {task.id}")

        except Exception as e:
            logger.warning(f"Docking 同步任务保存到数据库失败（不影响队列分发）: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


# =============================================================================
# 单例管理
# =============================================================================

_docking_sync_checker: Optional[DockingSyncChecker] = None


def get_docking_sync_checker() -> DockingSyncChecker:
    """获取 DockingSyncChecker 单例"""
    global _docking_sync_checker
    if _docking_sync_checker is None:
        _docking_sync_checker = DockingSyncChecker()
    return _docking_sync_checker
