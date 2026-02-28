"""
Docking 结果后处理器

定时扫描 Redis 中已完成/失败的 docking 任务，
从 MinIO 下载打分 CSV，解析每个分子的对接分数，
并写入 docking_result 表。

设计原则:
  - Worker（aidd-toolkit）只负责计算和生成结果文件
  - 平台（aidd-platform）负责解析结果并写入业务表
  - 通过 Redis Set 跟踪已处理的 task_id，避免重复处理

工作流程:
  1. 从 Redis completed/failed 队列获取 docking 任务 ID
  2. 跳过已处理的任务（通过 aidd:docking:processed 集合判断）
  3. 对已完成的任务:
     a. 读取 task hash 中的 output_files 和 input_params
     b. 从 MinIO 下载打分 CSV（_dock.csv）
     c. 解析每行 SMILES -> docking_score 映射
     d. 批量 UPDATE docking_result 表
  4. 对已失败的任务:
     a. 将对应的 pending 记录更新为 status='failed'
  5. 将 task_id 加入 processed 集合
"""

import asyncio
import csv
import io
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2

from app.config import get_settings
from app.mq.redis_client import get_redis_client
from app.storage.storage import get_storage

logger = logging.getLogger(__name__)

# Redis key: 已处理的 docking 任务集合（避免重复处理）
PROCESSED_SET_KEY = "aidd:docking:processed"


class DockingResultProcessor:
    """
    Docking 结果后处理器

    职责：
    1. 定时扫描 Redis 中已完成/失败的 docking 任务
    2. 从 MinIO 下载结果 CSV，解析打分数据
    3. 批量更新 docking_result 表（pending → success/failed）
    """

    def __init__(self):
        self._settings = get_settings()
        self._running = False
        # 扫描间隔（秒），使用与 docking_sync 相同的配置前缀
        self._scan_interval = self._settings.docking_result_process_interval
        logger.info(
            f"DockingResultProcessor 初始化: 扫描间隔={self._scan_interval}s"
        )

    async def start(self) -> None:
        """启动定时扫描循环"""
        if self._running:
            logger.warning("DockingResultProcessor 已在运行")
            return

        self._running = True
        logger.info("DockingResultProcessor 已启动")

        while self._running:
            try:
                await self._process_completed_tasks()
                await self._process_failed_tasks()
            except asyncio.CancelledError:
                logger.info("DockingResultProcessor 收到取消信号")
                break
            except Exception as e:
                logger.error(
                    f"DockingResultProcessor 扫描异常: {e}", exc_info=True
                )

            try:
                await asyncio.sleep(self._scan_interval)
            except asyncio.CancelledError:
                break

        logger.info("DockingResultProcessor 已停止")

    async def stop(self) -> None:
        """停止定时扫描"""
        self._running = False

    # =========================================================================
    # 核心处理逻辑
    # =========================================================================

    async def _process_completed_tasks(self) -> None:
        """扫描已完成的 docking 任务并写入结果"""
        redis_client = await get_redis_client()
        redis = await redis_client.get_client()

        # 获取 completed 列表中所有任务 ID（最多 1000 条）
        completed_ids = await redis.lrange("aidd:queue:completed", 0, -1)
        if not completed_ids:
            return

        processed_count = 0
        for task_id in completed_ids:
            # 如果是 bytes，解码为 str
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")

            # 检查是否已处理
            if await redis.sismember(PROCESSED_SET_KEY, task_id):
                continue

            # 获取任务数据
            task_data = await redis.hgetall(f"aidd:tasks:{task_id}")
            if not task_data:
                # 任务数据已过期，标记为已处理
                await redis.sadd(PROCESSED_SET_KEY, task_id)
                continue

            # 解码 task_data（Redis 可能返回 bytes）
            task_data = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): (
                    v.decode("utf-8") if isinstance(v, bytes) else v
                )
                for k, v in task_data.items()
            }

            # 仅处理 docking 服务的任务
            if task_data.get("service") != "docking":
                continue

            # 仅处理已完成的任务
            # 注意: Worker 通过 PlatformMQClient.complete() 设置 status="success"
            if task_data.get("status") not in ("completed", "success"):
                continue

            logger.info(f"开始处理已完成的 docking 任务: {task_id}")

            try:
                await self._handle_completed_task(task_id, task_data)
                processed_count += 1
            except Exception as e:
                logger.error(
                    f"处理 docking 任务结果失败 (task={task_id}): {e}",
                    exc_info=True,
                )

            # 标记为已处理（即使失败也标记，避免反复重试导致循环）
            await redis.sadd(PROCESSED_SET_KEY, task_id)

        if processed_count > 0:
            logger.info(f"本轮处理了 {processed_count} 个已完成的 docking 任务")

    async def _process_failed_tasks(self) -> None:
        """扫描已失败的 docking 任务并更新 docking_result 状态"""
        redis_client = await get_redis_client()
        redis = await redis_client.get_client()

        # 获取 failed 列表中所有任务 ID
        failed_ids = await redis.lrange("aidd:queue:failed", 0, -1)
        if not failed_ids:
            return

        processed_count = 0
        for task_id in failed_ids:
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")

            if await redis.sismember(PROCESSED_SET_KEY, task_id):
                continue

            task_data = await redis.hgetall(f"aidd:tasks:{task_id}")
            if not task_data:
                await redis.sadd(PROCESSED_SET_KEY, task_id)
                continue

            task_data = {
                (k.decode("utf-8") if isinstance(k, bytes) else k): (
                    v.decode("utf-8") if isinstance(v, bytes) else v
                )
                for k, v in task_data.items()
            }

            if task_data.get("service") != "docking":
                continue

            # Worker 通过 PlatformMQClient.fail() 设置 status="failed"
            if task_data.get("status") != "failed":
                continue

            logger.info(f"处理失败的 docking 任务: {task_id}")

            try:
                await self._handle_failed_task(task_id, task_data)
                processed_count += 1
            except Exception as e:
                logger.error(
                    f"处理失败 docking 任务状态更新失败 (task={task_id}): {e}",
                    exc_info=True,
                )

            await redis.sadd(PROCESSED_SET_KEY, task_id)

        if processed_count > 0:
            logger.info(f"本轮处理了 {processed_count} 个失败的 docking 任务")

    # =========================================================================
    # 具体任务处理
    # =========================================================================

    async def _handle_completed_task(
        self, task_id: str, task_data: Dict[str, str]
    ) -> None:
        """
        处理一个已完成的 docking 任务:
        1. 从 input_params 提取 receptor_id, project_id
        2. 从 output_files 找到打分 CSV 路径
        3. 下载 CSV 并解析每行分数
        4. 批量 UPDATE docking_result 表
        """
        # 解析 input_params
        input_params = json.loads(task_data.get("input_params", "{}"))
        receptor_id = input_params.get("receptor_id")
        project_id = input_params.get("project_id")

        if not receptor_id or not project_id:
            logger.warning(
                f"任务 {task_id} 缺少 receptor_id 或 project_id，跳过"
            )
            return

        # 解析 output_files： 找到打分 CSV（以 _dock.csv 结尾的文件）
        output_files = json.loads(task_data.get("output_files", "[]"))
        score_csv_path = self._find_score_csv(output_files)

        # 同时找到 maegz 结果文件路径（供前端下载）
        maegz_path = self._find_maegz_file(output_files)

        # 找到 manifest.json 路径（pose 拆分结果映射）
        manifest_path = self._find_manifest_file(output_files)

        if not score_csv_path:
            logger.warning(
                f"任务 {task_id} 无打分 CSV 输出文件，"
                f"将所有 pending 记录标记为 failed"
            )
            await self._mark_task_records_failed(
                task_id, "对接计算完成但未生成打分 CSV"
            )
            return

        # 从 MinIO 下载 CSV
        loop = asyncio.get_event_loop()
        score_map = await loop.run_in_executor(
            None, self._download_and_parse_csv, score_csv_path
        )

        if not score_map:
            logger.warning(
                f"任务 {task_id} 打分 CSV 为空或解析失败"
            )
            await self._mark_task_records_failed(
                task_id, "打分 CSV 为空或解析失败"
            )
            return

        logger.info(
            f"任务 {task_id}: 解析到 {len(score_map)} 个分子的对接分数"
        )

        # 解析 manifest.json（如果存在），获取 title → SDF 文件名映射
        title_to_sdf = {}
        poses_base_dir = ""
        if manifest_path:
            manifest_data = await loop.run_in_executor(
                None, self._download_and_parse_manifest, manifest_path
            )
            if manifest_data:
                title_to_sdf = manifest_data
                # 推算 poses 目录的 MinIO 基础路径
                # manifest 路径格式: minio://docking/glide/{date}/{task_id}/poses/manifest.json
                manifest_obj = manifest_path
                if manifest_obj.startswith("minio://"):
                    manifest_obj = manifest_obj[len("minio://"):]
                poses_base_dir = manifest_obj.rsplit('manifest.json', 1)[0]
                logger.info(
                    f"任务 {task_id}: manifest 包含 {len(title_to_sdf)} 个 pose 文件映射"
                )

        # 更新 docking_result 表
        await loop.run_in_executor(
            None,
            self._update_docking_results,
            task_id,
            receptor_id,
            score_map,
            maegz_path,
            title_to_sdf,
            poses_base_dir,
        )

    async def _handle_failed_task(
        self, task_id: str, task_data: Dict[str, str]
    ) -> None:
        """处理一个失败的 docking 任务：将对应 pending 记录标记为 failed"""
        error_message = task_data.get("error_message", "任务执行失败（未知原因）")
        await self._mark_task_records_failed(task_id, error_message)

    async def _mark_task_records_failed(
        self, task_id: str, error_message: str
    ) -> None:
        """将某个 task_id 关联的所有 pending/computing 记录标记为 failed"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, self._update_records_to_failed, task_id, error_message
        )

    # =========================================================================
    # 文件解析（同步，在线程池中执行）
    # =========================================================================

    @staticmethod
    def _find_score_csv(output_files: List[str]) -> Optional[str]:
        """
        从 output_files 列表中找到打分 CSV 文件

        Worker 上传的打分 CSV 路径格式: minio://docking/glide/{date}/{task_id}_dock.csv
        """
        for f in output_files:
            if f.endswith("_dock.csv") or f.endswith("_dock.csv"):
                return f
        # 回退: 第一个 .csv 文件
        for f in output_files:
            if f.endswith(".csv"):
                return f
        return None

    @staticmethod
    def _find_maegz_file(output_files: List[str]) -> Optional[str]:
        """从 output_files 找到 maegz 结构文件（top-1 pose 优先）"""
        # 优先找 top-1 pose 文件（_pv_1p.maegz）
        for f in output_files:
            if "_pv_1p.maegz" in f:
                return f
        # 回退: 找任意 maegz
        for f in output_files:
            if f.endswith(".maegz"):
                return f
        return None

    @staticmethod
    def _find_manifest_file(output_files: List[str]) -> Optional[str]:
        """从 output_files 找到 pose 拆分的 manifest.json 文件"""
        for f in output_files:
            if f.endswith("manifest.json"):
                return f
        return None

    def _download_and_parse_csv(self, csv_minio_path: str) -> Dict[str, Dict]:
        """
        从 MinIO 下载打分 CSV 并解析为 SMILES -> 分数映射

        CSV 格式（由 GlideRunner._merge_results 生成）：
        - 索引列: title
        - smiles: 原始 SMILES
        - PK_{target}: docking score（越负越好）
        - 其他可能的列: EpikAlert_{target} 等

        Returns:
            {smiles: {"docking_score": float, "glide_gscore": float, ...}}
        """
        try:
            # 去掉 minio:// 前缀
            object_name = csv_minio_path
            if object_name.startswith("minio://"):
                object_name = object_name[len("minio://"):]

            storage = get_storage()
            csv_bytes = storage.download(object_name)
            csv_text = csv_bytes.decode("utf-8")

            reader = csv.DictReader(io.StringIO(csv_text))
            fieldnames = reader.fieldnames or []

            # 自动查找 PK_{target} 列名（docking score 列）
            score_col = None
            for col in fieldnames:
                if col.startswith("PK_"):
                    score_col = col
                    break

            # 查找 Glide_score 列（如果存在）
            glide_score_col = "Glide_score" if "Glide_score" in fieldnames else None
            glide_emodel_col = "Emodel" if "Emodel" in fieldnames else None
            glide_energy_col = "GlideEnergy" if "GlideEnergy" in fieldnames else None

            result = {}
            for row in reader:
                smiles = row.get("smiles", "").strip()
                if not smiles:
                    continue

                scores = {}

                # 主打分（使用 PK_{target} 列或 Glide_score 列）
                raw_score = None
                if score_col and row.get(score_col):
                    raw_score = row[score_col]
                elif glide_score_col and row.get(glide_score_col):
                    raw_score = row[glide_score_col]

                if raw_score:
                    try:
                        scores["docking_score"] = float(raw_score)
                    except (ValueError, TypeError):
                        pass

                # 额外打分字段（如果 CSV 中包含）
                if glide_score_col and row.get(glide_score_col):
                    try:
                        scores["glide_gscore"] = float(row[glide_score_col])
                    except (ValueError, TypeError):
                        pass

                if glide_emodel_col and row.get(glide_emodel_col):
                    try:
                        scores["glide_emodel"] = float(row[glide_emodel_col])
                    except (ValueError, TypeError):
                        pass

                if glide_energy_col and row.get(glide_energy_col):
                    try:
                        scores["glide_energy"] = float(row[glide_energy_col])
                    except (ValueError, TypeError):
                        pass

                if scores:
                    # 保留 title 信息，用于关联 manifest.json 中的 SDF 文件
                    title = row.get("title", "").strip()
                    if title:
                        scores["_title"] = title
                    result[smiles] = scores

            logger.debug(f"CSV 解析完成: {len(result)} 个分子有打分数据")
            return result

        except Exception as e:
            logger.error(f"下载/解析打分 CSV 失败 ({csv_minio_path}): {e}")
            return {}

    def _download_and_parse_manifest(
        self, manifest_minio_path: str
    ) -> Optional[Dict[str, str]]:
        """
        从 MinIO 下载 manifest.json 并解析为 title → SDF 文件名映射

        Returns:
            {title: filename} 或 None（下载/解析失败时）
        """
        try:
            object_name = manifest_minio_path
            if object_name.startswith("minio://"):
                object_name = object_name[len("minio://"):]

            storage = get_storage()
            manifest_bytes = storage.download(object_name)
            manifest_data = json.loads(manifest_bytes.decode("utf-8"))

            if isinstance(manifest_data, dict):
                logger.debug(
                    f"manifest.json 解析完成: {len(manifest_data)} 个 pose 映射"
                )
                return manifest_data
            else:
                logger.warning(f"manifest.json 格式异常: 期望 dict，得到 {type(manifest_data)}")
                return None
        except Exception as e:
            logger.warning(f"下载/解析 manifest.json 失败 ({manifest_minio_path}): {e}")
            return None

    # =========================================================================
    # 数据库更新（同步，在线程池中执行）
    # =========================================================================

    def _get_connection(self):
        """获取数据库连接"""
        settings = self._settings
        return psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db,
        )

    def _update_docking_results(
        self,
        task_id: str,
        receptor_id: str,
        score_map: Dict[str, Dict],
        maegz_path: Optional[str],
        title_to_sdf: Optional[Dict[str, str]] = None,
        poses_base_dir: str = "",
    ) -> None:
        """
        批量更新 docking_result 表：
        - 有打分的 SMILES → status='success', 写入分数
        - 属于该 task 但不在 score_map 中的 SMILES → status='failed'（对接失败）

        如果有 pose 拆分信息（title_to_sdf + poses_base_dir），
        同时写入 pose_sdf_minio_path 和 receptor_pdb_minio_path。
        """
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            success_count = 0
            failed_count = 0

            # 构建 receptor PDB 路径（所有化合物共用）
            receptor_pdb_path = None
            if poses_base_dir:
                receptor_pdb_path = f"minio://{poses_base_dir}receptor.pdb"

            # 构建 SMILES → SDF 路径映射
            # score_map CSV 中包含 title 列，通过 title 关联 manifest 中的 SDF 文件名
            # 由于 score_map 的 key 是 smiles，需要先从 CSV 建立 smiles → title 映射
            smiles_to_sdf_path: Dict[str, str] = {}
            if title_to_sdf and poses_base_dir:
                # 从打分 CSV 重新建立 smiles → title 映射
                # score_map 已经在 _download_and_parse_csv 中构建
                # 但 score_map 不包含 title，需要重新遍历 CSV
                # 此处使用一个近似方案：从 score_map 中的 _title 字段取值
                for smiles, scores in score_map.items():
                    title = scores.get("_title", "")
                    if title and title in title_to_sdf:
                        sdf_filename = title_to_sdf[title]
                        smiles_to_sdf_path[smiles] = f"minio://{poses_base_dir}{sdf_filename}"

            # 先查出该 task 关联的所有 pending/computing 记录的 SMILES
            cur.execute(
                """
                SELECT smiles FROM docking_result
                WHERE task_id = %s AND status IN ('pending', 'computing')
                """,
                (task_id,),
            )
            task_smiles_rows = cur.fetchall()
            task_smiles_set = {row[0] for row in task_smiles_rows}

            # 逐条更新有打分的记录
            for smiles in task_smiles_set:
                if smiles in score_map:
                    scores = score_map[smiles]
                    # 查找该化合物的单独 SDF 文件路径
                    sdf_path = smiles_to_sdf_path.get(smiles)
                    cur.execute(
                        """
                        UPDATE docking_result
                        SET status = 'success',
                            docking_score = %s,
                            glide_gscore = %s,
                            glide_emodel = %s,
                            glide_energy = %s,
                            result_minio_path = %s,
                            pose_sdf_minio_path = %s,
                            receptor_pdb_minio_path = %s,
                            error_message = NULL,
                            updated_at = NOW()
                        WHERE task_id = %s
                          AND receptor_id = %s
                          AND smiles = %s
                          AND status IN ('pending', 'computing')
                        """,
                        (
                            scores.get("docking_score"),
                            scores.get("glide_gscore"),
                            scores.get("glide_emodel"),
                            scores.get("glide_energy"),
                            maegz_path,
                            sdf_path,
                            receptor_pdb_path,
                            task_id,
                            receptor_id,
                            smiles,
                        ),
                    )
                    success_count += cur.rowcount
                else:
                    # 该 SMILES 不在打分结果中（对接失败的分子）
                    cur.execute(
                        """
                        UPDATE docking_result
                        SET status = 'failed',
                            error_message = '对接计算未产生该分子的打分结果',
                            updated_at = NOW()
                        WHERE task_id = %s
                          AND receptor_id = %s
                          AND smiles = %s
                          AND status IN ('pending', 'computing')
                        """,
                        (task_id, receptor_id, smiles),
                    )
                    failed_count += cur.rowcount

            conn.commit()
            cur.close()

            logger.info(
                f"任务 {task_id} 结果已写入: "
                f"success={success_count}, failed={failed_count}"
            )

        except Exception as e:
            logger.error(f"更新 docking_result 失败 (task={task_id}): {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def _update_records_to_failed(
        self, task_id: str, error_message: str
    ) -> None:
        """将某个 task_id 关联的所有 pending/computing 记录标记为 failed"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            cur.execute(
                """
                UPDATE docking_result
                SET status = 'failed',
                    error_message = %s,
                    updated_at = NOW()
                WHERE task_id = %s
                  AND status IN ('pending', 'computing')
                """,
                (error_message, task_id),
            )
            updated = cur.rowcount
            conn.commit()
            cur.close()

            if updated > 0:
                logger.info(
                    f"任务 {task_id} 的 {updated} 条记录已标记为 failed: {error_message}"
                )

        except Exception as e:
            logger.error(
                f"更新 docking_result 为 failed 失败 (task={task_id}): {e}"
            )
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()


# =============================================================================
# 单例管理
# =============================================================================

_docking_result_processor: Optional[DockingResultProcessor] = None


def get_docking_result_processor() -> DockingResultProcessor:
    """获取 DockingResultProcessor 单例"""
    global _docking_result_processor
    if _docking_result_processor is None:
        _docking_result_processor = DockingResultProcessor()
    return _docking_result_processor
