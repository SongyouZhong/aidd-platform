"""
Docking 结果后处理 DAG

替代: DockingResultProcessor
调度: */2 * * * *（每 2 分钟）
重试: 1 次，间隔 1 分钟

流程:
  [process_completed_tasks] → [process_failed_tasks]

功能:
  1. 扫描 Redis 中已完成的 docking 任务
  2. 从 MinIO 下载打分 CSV，解析每个分子的对接分数
  3. 批量 UPDATE docking_result 表（pending → success）
  4. 对失败任务将 pending 记录标记为 failed
  5. 通过 aidd:docking:processed 集合避免重复处理
"""

import csv
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'aidd',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='docking_result_process',
    default_args=default_args,
    description='扫描已完成/失败的 docking 任务，解析结果写入 docking_result 表',
    schedule='*/2 * * * *',  # 每2分钟
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['docking', 'result', 'process'],
) as dag:

    def process_completed(**context):
        """
        扫描已完成的 docking 任务并写入结果
        逻辑复用自 DockingResultProcessor._process_completed_tasks
        """
        from common.redis_utils import (
            get_completed_task_ids, get_task_data,
            is_task_processed, mark_task_processed,
        )

        completed_ids = get_completed_task_ids()
        if not completed_ids:
            logger.info("无已完成的 docking 任务需要处理")
            return 0

        processed = 0
        for task_id in completed_ids:
            # 跳过已处理的
            if is_task_processed(task_id):
                continue

            task_data = get_task_data(task_id)
            if not task_data:
                mark_task_processed(task_id)
                continue

            # 仅处理 docking 服务的已完成任务
            if task_data.get("service") != "docking":
                continue
            if task_data.get("status") not in ("completed", "success"):
                continue

            logger.info(f"开始处理已完成的 docking 任务: {task_id}")

            try:
                _handle_completed_task(task_id, task_data)
                processed += 1
            except Exception as e:
                logger.error(f"处理 docking 任务结果失败 (task={task_id}): {e}", exc_info=True)

            # 标记为已处理（即使失败也标记，避免反复重试）
            mark_task_processed(task_id)

        if processed > 0:
            logger.info(f"本轮处理了 {processed} 个已完成的 docking 任务")
        return processed

    def process_failed(**context):
        """
        扫描已失败的 docking 任务并更新 docking_result 状态
        逻辑复用自 DockingResultProcessor._process_failed_tasks
        """
        from common.redis_utils import (
            get_failed_task_ids, get_task_data,
            is_task_processed, mark_task_processed,
        )

        failed_ids = get_failed_task_ids()
        if not failed_ids:
            return 0

        processed = 0
        for task_id in failed_ids:
            if is_task_processed(task_id):
                continue

            task_data = get_task_data(task_id)
            if not task_data:
                mark_task_processed(task_id)
                continue

            if task_data.get("service") != "docking":
                continue
            if task_data.get("status") != "failed":
                continue

            logger.info(f"处理失败的 docking 任务: {task_id}")

            try:
                error_message = task_data.get("error_message", "任务执行失败（未知原因）")
                _update_records_to_failed(task_id, error_message)
                processed += 1
            except Exception as e:
                logger.error(f"处理失败 docking 任务状态更新失败 (task={task_id}): {e}", exc_info=True)

            mark_task_processed(task_id)

        if processed > 0:
            logger.info(f"本轮处理了 {processed} 个失败的 docking 任务")
        return processed

    # =========================================================================
    # 核心处理函数（复用自 DockingResultProcessor）
    # =========================================================================

    def _handle_completed_task(task_id: str, task_data: Dict[str, str]) -> None:
        """
        处理一个已完成的 docking 任务:
        1. 从 input_params 提取 receptor_id, project_id
        2. 从 output_files 找到打分 CSV 路径
        3. 下载 CSV 并解析每行分数
        4. 批量 UPDATE docking_result 表
        """
        input_params = json.loads(task_data.get("input_params", "{}"))
        receptor_id = input_params.get("receptor_id")
        project_id = input_params.get("project_id")

        if not receptor_id or not project_id:
            logger.warning(f"任务 {task_id} 缺少 receptor_id 或 project_id，跳过")
            return

        output_files = json.loads(task_data.get("output_files", "[]"))
        score_csv_path = _find_score_csv(output_files)
        maegz_path = _find_maegz_file(output_files)
        manifest_path = _find_manifest_file(output_files)

        if not score_csv_path:
            logger.warning(f"任务 {task_id} 无打分 CSV 输出文件，将所有 pending 记录标记为 failed")
            _update_records_to_failed(task_id, "对接计算完成但未生成打分 CSV")
            return

        # 从 MinIO 下载并解析 CSV
        score_map = _download_and_parse_csv(score_csv_path)
        if not score_map:
            logger.warning(f"任务 {task_id} 打分 CSV 为空或解析失败")
            _update_records_to_failed(task_id, "打分 CSV 为空或解析失败")
            return

        logger.info(f"任务 {task_id}: 解析到 {len(score_map)} 个分子的对接分数")

        # 解析 manifest.json（如果存在）
        title_to_sdf = {}
        poses_base_dir = ""
        if manifest_path:
            title_to_sdf = _download_and_parse_manifest(manifest_path) or {}
            if title_to_sdf:
                manifest_obj = manifest_path
                if manifest_obj.startswith("minio://"):
                    manifest_obj = manifest_obj[len("minio://"):]
                poses_base_dir = manifest_obj.rsplit('manifest.json', 1)[0]

        # 更新 docking_result 表
        _update_docking_results(task_id, receptor_id, score_map, maegz_path, title_to_sdf, poses_base_dir)

    def _find_score_csv(output_files: List[str]) -> Optional[str]:
        """从 output_files 列表中找到打分 CSV 文件"""
        for f in output_files:
            if f.endswith("_dock.csv"):
                return f
        for f in output_files:
            if f.endswith(".csv"):
                return f
        return None

    def _find_maegz_file(output_files: List[str]) -> Optional[str]:
        """从 output_files 找到 maegz 结构文件"""
        for f in output_files:
            if "_pv_1p.maegz" in f:
                return f
        for f in output_files:
            if f.endswith(".maegz"):
                return f
        return None

    def _find_manifest_file(output_files: List[str]) -> Optional[str]:
        """从 output_files 找到 pose 拆分的 manifest.json 文件"""
        for f in output_files:
            if f.endswith("manifest.json"):
                return f
        return None

    def _download_and_parse_csv(csv_minio_path: str) -> Dict[str, Dict]:
        """
        从 MinIO 下载打分 CSV 并解析为 SMILES -> 分数映射
        逻辑复用自 DockingResultProcessor._download_and_parse_csv
        """
        from common.minio_utils import download_file_as_text

        try:
            csv_text = download_file_as_text(csv_minio_path)
            reader = csv.DictReader(io.StringIO(csv_text))
            fieldnames = reader.fieldnames or []

            # 自动查找打分列
            score_col = None
            for col in fieldnames:
                if col.startswith("PK_"):
                    score_col = col
                    break

            glide_score_col = "Glide_score" if "Glide_score" in fieldnames else None
            glide_emodel_col = "Emodel" if "Emodel" in fieldnames else None
            glide_energy_col = "GlideEnergy" if "GlideEnergy" in fieldnames else None

            result = {}
            for row in reader:
                smiles = row.get("smiles", "").strip()
                if not smiles:
                    continue

                scores = {}

                # 主打分
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

                # 额外打分字段
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
                    title = row.get("title", "").strip()
                    if title:
                        scores["_title"] = title
                    result[smiles] = scores

            return result

        except Exception as e:
            logger.error(f"下载/解析打分 CSV 失败 ({csv_minio_path}): {e}")
            return {}

    def _download_and_parse_manifest(manifest_minio_path: str) -> Optional[Dict[str, str]]:
        """从 MinIO 下载 manifest.json 并解析为 title → SDF 文件名映射"""
        from common.minio_utils import download_file_as_text

        try:
            manifest_text = download_file_as_text(manifest_minio_path)
            manifest_data = json.loads(manifest_text)
            if isinstance(manifest_data, dict):
                return manifest_data
            logger.warning(f"manifest.json 格式异常: 期望 dict，得到 {type(manifest_data)}")
            return None
        except Exception as e:
            logger.warning(f"下载/解析 manifest.json 失败 ({manifest_minio_path}): {e}")
            return None

    def _update_docking_results(
        task_id: str,
        receptor_id: str,
        score_map: Dict[str, Dict],
        maegz_path: Optional[str],
        title_to_sdf: Optional[Dict[str, str]] = None,
        poses_base_dir: str = "",
    ) -> None:
        """
        批量更新 docking_result 表
        逻辑复用自 DockingResultProcessor._update_docking_results
        """
        from common.db_utils import get_sync_connection

        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            success_count = 0
            failed_count = 0

            # 构建 receptor PDB 路径
            receptor_pdb_path = None
            if poses_base_dir:
                receptor_pdb_path = f"minio://{poses_base_dir}receptor.pdb"

            # SMILES → SDF 路径映射
            smiles_to_sdf_path: Dict[str, str] = {}
            if title_to_sdf and poses_base_dir:
                for smiles, scores in score_map.items():
                    title = scores.get("_title", "")
                    if title and title in title_to_sdf:
                        sdf_filename = title_to_sdf[title]
                        smiles_to_sdf_path[smiles] = f"minio://{poses_base_dir}{sdf_filename}"

            # 查出该 task 关联的所有 pending/computing 记录的 SMILES
            cur.execute(
                "SELECT smiles FROM docking_result WHERE task_id = %s AND status IN ('pending', 'computing')",
                (task_id,),
            )
            task_smiles_set = {row[0] for row in cur.fetchall()}

            for smiles in task_smiles_set:
                if smiles in score_map:
                    scores = score_map[smiles]
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
                        WHERE task_id = %s AND receptor_id = %s AND smiles = %s
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
                            task_id, receptor_id, smiles,
                        ),
                    )
                    success_count += cur.rowcount
                else:
                    cur.execute(
                        """
                        UPDATE docking_result
                        SET status = 'failed',
                            error_message = '对接计算未产生该分子的打分结果',
                            updated_at = NOW()
                        WHERE task_id = %s AND receptor_id = %s AND smiles = %s
                          AND status IN ('pending', 'computing')
                        """,
                        (task_id, receptor_id, smiles),
                    )
                    failed_count += cur.rowcount

            conn.commit()
            cur.close()
            logger.info(f"任务 {task_id} 结果已写入: success={success_count}, failed={failed_count}")

        except Exception as e:
            logger.error(f"更新 docking_result 失败 (task={task_id}): {e}")
            conn.rollback()
        finally:
            conn.close()

    def _update_records_to_failed(task_id: str, error_message: str) -> None:
        """将某个 task_id 关联的所有 pending/computing 记录标记为 failed"""
        from common.db_utils import get_sync_connection

        conn = get_sync_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE docking_result
                SET status = 'failed', error_message = %s, updated_at = NOW()
                WHERE task_id = %s AND status IN ('pending', 'computing')
                """,
                (error_message, task_id),
            )
            updated = cur.rowcount
            conn.commit()
            cur.close()
            if updated > 0:
                logger.info(f"任务 {task_id} 的 {updated} 条记录已标记为 failed: {error_message}")
        except Exception as e:
            logger.error(f"更新 docking_result 为 failed 失败 (task={task_id}): {e}")
            conn.rollback()
        finally:
            conn.close()

    # =========================================================================
    # DAG 任务编排
    # =========================================================================

    t_completed = PythonOperator(
        task_id='process_completed_tasks',
        python_callable=process_completed,
    )
    t_failed = PythonOperator(
        task_id='process_failed_tasks',
        python_callable=process_failed,
    )

    t_completed >> t_failed
