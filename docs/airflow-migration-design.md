# AIDD Platform — Airflow 迁移总体设计方案

## 1. 设计目标

- **引入 Apache Airflow** 替代 aidd-platform 中的自研调度逻辑
- **使用 Airflow UI** 替代 aidd-platform-front，不再维护自定义前端
- **使用 Grafana + Prometheus** 补充 Airflow UI 缺失的集群资源监控
- **保留 aidd-toolkit** 作为计算执行层，最小化改动

---

## 2. 架构总览

### 2.1 现有架构

```
aidd-platform-front (Vue 3)
        │ HTTP
        ▼
aidd-platform (FastAPI)
  ├── API 层（任务CRUD、Worker管理）
  ├── Scheduler（Dispatcher、PriorityQueue、ResourceManager）
  ├── 自动同步（AdmetSync、DockingSync、ResultProcessor）
  ├── 心跳检查（HeartbeatChecker）
  └── Redis MQ + MinIO + PostgreSQL
        │
        ▼
aidd-toolkit (Worker 节点)
  ├── ADMET Worker (QikProp)
  └── Docking Worker (Glide)
```

### 2.2 目标架构

```
┌──────────────────────────────────────────────────────────────────┐
│                        用户界面层                                 │
│                                                                   │
│   Airflow Web UI                    Grafana                      │
│   ┌─────────────────────┐          ┌────────────────────┐       │
│   │ DAG 管理/监控        │          │ Worker 资源监控     │       │
│   │ 任务历史/日志        │          │ CPU/Mem/GPU 面板   │       │
│   │ 手动触发任务         │          │ 任务耗时趋势       │       │
│   │ 重试/SLA 告警       │          │ 队列深度告警       │       │
│   └─────────────────────┘          └────────────────────┘       │
└────────────┬─────────────────────────────────┬──────────────────┘
             │                                  │
             ▼                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                        编排调度层 (Airflow)                       │
│                                                                   │
│   ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐      │
│   │ Scheduler   │  │ CeleryExec   │  │ PostgreSQL(meta)  │      │
│   │ DAG 解析    │  │ Redis Broker │  │ DAG Run 记录      │      │
│   └─────────────┘  └──────────────┘  └───────────────────┘      │
│                                                                   │
│   DAGs:                                                           │
│   ├── admet_sync_dag          (每日扫描 → 批量提交)               │
│   ├── docking_sync_dag        (每小时扫描 → 批量提交)             │
│   ├── admet_compute_dag       (单次 ADMET 计算流水线)             │
│   ├── docking_compute_dag     (单次 Docking 计算流水线)           │
│   ├── docking_result_dag      (结果后处理 → 写DB)                 │
│   ├── worker_heartbeat_dag    (Worker 健康检查)                   │
│   └── manual_submit_dag       (手动触发 → 接收参数 → 提交计算)    │
└──────────────────────────────────────────────────────────────────┘
             │                    │                  │
             │ Redis              │ MinIO             │ PostgreSQL
             ▼                    ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                        基础设施层                                 │
│                                                                   │
│   ┌─────────┐  ┌─────────┐  ┌────────────┐  ┌───────────────┐  │
│   │ Redis   │  │ MinIO   │  │ PostgreSQL │  │ Prometheus    │  │
│   │ (Broker │  │ (文件   │  │ (aichemol  │  │ (指标采集)    │  │
│   │ +队列)  │  │  存储)  │  │  +Airflow) │  │               │  │
│   └─────────┘  └─────────┘  └────────────┘  └───────────────┘  │
└──────────────────────────────────────────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────────────────────────────┐
│                        执行层 (aidd-toolkit)                      │
│                                                                   │
│   ┌───────────────────┐      ┌───────────────────┐              │
│   │  ADMET Worker     │      │  Docking Worker    │              │
│   │  - QikProp 计算   │      │  - Glide 对接      │              │
│   │  - Prometheus暴露 │      │  - Prometheus暴露  │              │
│   │  - 心跳上报       │      │  - 心跳上报        │              │
│   └───────────────────┘      └───────────────────┘              │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. 组件变更清单

### 3.1 废弃的组件

| 组件 | 原位置 | 废弃原因 |
|---|---|---|
| **aidd-platform-front** | 整个项目 | Airflow UI + Grafana 替代 |
| **TaskDispatcher** | `scheduler/dispatcher.py` | Airflow Scheduler 替代 |
| **PriorityTaskQueue** | `scheduler/priority_queue.py` | Airflow 任务优先级 + Pool 替代 |
| **AdmetSyncChecker** | `scheduler/admet_sync.py` | 迁移为 `admet_sync_dag` |
| **DockingSyncChecker** | `scheduler/docking_sync.py` | 迁移为 `docking_sync_dag` |
| **DockingResultProcessor** | `scheduler/docking_result_processor.py` | 迁移为 `docking_result_dag` |
| **HeartbeatChecker** | `scheduler/heartbeat_checker.py` | 迁移为 `worker_heartbeat_dag` |
| **FastAPI API 层** | `api/v1/tasks.py`, `workers.py`, `health.py` | Airflow REST API 替代 |
| **FastAPI main.py** | `app/main.py` | 不再需要 FastAPI 进程 |

### 3.2 保留的组件

| 组件 | 位置 | 说明 |
|---|---|---|
| **ResourceManager** | `scheduler/resource_manager.py` | 改造为 Airflow DAG 内调用的纯函数模块 |
| **RedisTaskQueue** | `mq/task_queue.py` | 保留 Redis 数据结构，Airflow 任务通过它与 Worker 交互 |
| **RedisClient** | `mq/redis_client.py` | Redis 连接管理 |
| **Storage (MinIO)** | `storage/` | 文件上传下载不变 |
| **Models** | `models/` | 数据模型保留 |
| **aidd-toolkit 整体** | 独立项目 | Worker 端几乎不改，继续从 Redis 消费任务 |

### 3.3 新增的组件

| 组件 | 说明 |
|---|---|
| **Airflow DAGs** | 7 个 DAG 文件（见第4节详细设计） |
| **Airflow Operators** | 自定义 Operator 封装现有逻辑 |
| **Airflow Sensors** | 等待 Worker 完成任务的 Sensor |
| **Grafana Dashboard** | Worker 资源监控看板 |
| **Prometheus 配置** | 抓取 Worker 节点指标 |

---

## 4. Airflow DAG 详细设计

### 4.1 DAG 总览

```
dags/
├── __init__.py
├── common/
│   ├── __init__.py
│   ├── db_utils.py            # PostgreSQL 查询工具
│   ├── redis_utils.py         # Redis 队列操作工具
│   ├── minio_utils.py         # MinIO 文件操作工具
│   └── config.py              # 共享配置（连接串、常量）
├── admet_sync_dag.py          # ADMET 自动同步
├── docking_sync_dag.py        # Docking 自动同步
├── admet_compute_dag.py       # ADMET 计算流水线
├── docking_compute_dag.py     # Docking 计算流水线
├── docking_result_dag.py      # Docking 结果后处理
├── worker_heartbeat_dag.py    # Worker 心跳检查
├── operators/
│   ├── __init__.py
│   ├── redis_task_operator.py # 提交任务到 Redis
│   └── minio_operator.py      # MinIO 文件操作
└── sensors/
    ├── __init__.py
    └── redis_task_sensor.py   # 等待 Redis 中任务完成
```

### 4.2 admet_sync_dag — ADMET 自动同步

**替代**: `AdmetSyncChecker`  
**调度**: `@daily`（每天凌晨 2:00）  
**重试**: 2 次，间隔 10 分钟  

```
[scan_missing_compounds] → [split_batches] → [submit_batch_tasks] → [log_summary]
```

```python
# dags/admet_sync_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'aidd',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='admet_sync',
    default_args=default_args,
    description='扫描缺少 ADMET 结果的化合物，自动提交计算任务',
    schedule_interval='0 2 * * *',  # 每天凌晨2点
    start_date=days_ago(1),
    catchup=False,
    tags=['admet', 'sync', 'batch'],
) as dag:

    def scan_missing_compounds(**context):
        """
        查询 project_compounds LEFT JOIN admet_compute_result，
        找出缺少 ADMET 结果的化合物。
        逻辑复用自 AdmetSyncChecker._query_missing_compounds()
        """
        from common.db_utils import get_sync_connection
        from common.config import ADMET_BATCH_SIZE

        conn = get_sync_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pc.id, pc.smiles, pc.project_id
                    FROM project_compounds pc
                    LEFT JOIN admet_compute_result acr
                        ON pc.id = acr.compound_id
                    WHERE pc.is_deleted = false
                      AND acr.id IS NULL
                    ORDER BY pc.created_at
                """)
                missing = cur.fetchall()
        finally:
            conn.close()

        # 通过 XCom 传递结果
        context['ti'].xcom_push(key='missing_compounds', value=missing)
        return len(missing)

    def split_batches(**context):
        """将缺失化合物按批次分组"""
        from common.config import ADMET_BATCH_SIZE

        missing = context['ti'].xcom_pull(
            task_ids='scan_missing_compounds',
            key='missing_compounds'
        )
        if not missing:
            return []

        batches = [
            missing[i:i + ADMET_BATCH_SIZE]
            for i in range(0, len(missing), ADMET_BATCH_SIZE)
        ]
        context['ti'].xcom_push(key='batches', value=batches)
        return len(batches)

    def submit_batch_tasks(**context):
        """
        为每个批次创建 Redis 任务。
        逻辑复用自 AdmetSyncChecker._submit_admet_task()
        """
        from common.redis_utils import push_task_to_redis
        from common.config import ADMET_PRIORITY
        import uuid

        batches = context['ti'].xcom_pull(
            task_ids='split_batches', key='batches'
        )
        if not batches:
            return 0

        task_ids = []
        for batch in batches:
            task_id = str(uuid.uuid4())
            compounds = [
                {'id': str(row[0]), 'smiles': row[1], 'project_id': str(row[2])}
                for row in batch
            ]
            push_task_to_redis(
                task_id=task_id,
                service='admet',
                task_type='qikprop',
                priority=ADMET_PRIORITY,
                input_params={
                    'compounds': compounds,
                    'smiles': [c['smiles'] for c in compounds],
                },
                resource_cpu_cores=2,
                resource_memory_gb=4.0,
            )
            task_ids.append(task_id)

        context['ti'].xcom_push(key='submitted_task_ids', value=task_ids)
        return len(task_ids)

    t_scan = PythonOperator(
        task_id='scan_missing_compounds',
        python_callable=scan_missing_compounds,
    )
    t_split = PythonOperator(
        task_id='split_batches',
        python_callable=split_batches,
    )
    t_submit = PythonOperator(
        task_id='submit_batch_tasks',
        python_callable=submit_batch_tasks,
    )

    t_scan >> t_split >> t_submit
```

### 4.3 docking_sync_dag — Docking 自动同步

**替代**: `DockingSyncChecker`  
**调度**: `0 */1 * * *`（每小时）  
**重试**: 2 次，间隔 5 分钟  

```
[query_active_receptors]
        │
        ▼
[for each receptor] → [find_missing_compounds] → [pre_insert_pending]
        │                                               │
        ▼                                               ▼
[generate_smiles_csv] → [upload_to_minio] → [submit_docking_task]
        │
        ▼
[log_summary]
```

```python
# dags/docking_sync_dag.py（核心结构）

with DAG(
    dag_id='docking_sync',
    schedule_interval='0 */1 * * *',  # 每小时
    catchup=False,
    tags=['docking', 'sync', 'batch'],
) as dag:

    def scan_and_submit(**context):
        """
        完整的 Docking 同步逻辑（单个任务内完成）。
        
        因为 receptor 数量动态变化，且逻辑内含 DB 写入（pre_insert_pending）
        + MinIO 上传 + Redis 入队，不适合拆成多个静态 task。
        复用 DockingSyncChecker._scan_and_submit() 的核心逻辑。
        """
        from common.db_utils import get_sync_connection
        from common.redis_utils import push_task_to_redis
        from common.minio_utils import upload_csv_string
        from common.config import DOCKING_BATCH_SIZE, DOCKING_PRIORITY
        import uuid, csv, io

        conn = get_sync_connection()
        try:
            # 1. 查询活跃受体
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, project_id, file_path, target_name
                    FROM project_receptors
                    WHERE is_active = true
                """)
                receptors = cur.fetchall()

            submitted = 0
            for receptor in receptors:
                receptor_id, project_id, grid_path, target = receptor

                # 2. 找缺少 docking 结果的化合物
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT pc.smiles
                        FROM project_compounds pc
                        LEFT JOIN docking_result dr
                            ON pc.smiles = dr.smiles
                            AND dr.receptor_id = %s
                        WHERE pc.project_id = %s
                          AND pc.is_deleted = false
                          AND dr.id IS NULL
                    """, (str(receptor_id), str(project_id)))
                    missing = [row[0] for row in cur.fetchall()]

                if not missing:
                    continue

                # 3. 预插入 pending 记录
                with conn.cursor() as cur:
                    for smiles in missing:
                        cur.execute("""
                            INSERT INTO docking_result
                                (smiles, receptor_id, project_id, status)
                            VALUES (%s, %s, %s, 'pending')
                            ON CONFLICT DO NOTHING
                        """, (smiles, str(receptor_id), str(project_id)))
                    conn.commit()

                # 4. 按批次提交
                for i in range(0, len(missing), DOCKING_BATCH_SIZE):
                    batch = missing[i:i + DOCKING_BATCH_SIZE]
                    task_id = str(uuid.uuid4())

                    # 生成 CSV 并上传 MinIO
                    buf = io.StringIO()
                    writer = csv.writer(buf)
                    writer.writerow(['smiles', 'title'])
                    for idx, smi in enumerate(batch):
                        writer.writerow([smi, f'mol_{idx}'])
                    csv_path = f"docking/input/{task_id}.csv"
                    upload_csv_string(buf.getvalue(), csv_path)

                    push_task_to_redis(
                        task_id=task_id,
                        service='docking',
                        task_type='glide',
                        priority=DOCKING_PRIORITY,
                        input_params={
                            'input_file': f'minio://{csv_path}',
                            'grid_file': f'minio://{grid_path}',
                            'target': target,
                            'receptor_id': str(receptor_id),
                            'project_id': str(project_id),
                        },
                        resource_cpu_cores=4,
                        resource_memory_gb=8.0,
                    )
                    submitted += 1
        finally:
            conn.close()

        return submitted

    PythonOperator(
        task_id='scan_and_submit',
        python_callable=scan_and_submit,
    )
```

### 4.4 docking_result_dag — Docking 结果后处理

**替代**: `DockingResultProcessor`  
**调度**: `*/2 * * * *`（每 2 分钟）  
**说明**: 扫描 Redis 中已完成的 docking 任务，下载 CSV，解析分数，写入 `docking_result` 表。

```
[scan_completed_tasks] → [process_results] → [scan_failed_tasks] → [mark_failed]
```

```python
# dags/docking_result_dag.py（核心结构）

with DAG(
    dag_id='docking_result_process',
    schedule_interval='*/2 * * * *',  # 每2分钟
    catchup=False,
    tags=['docking', 'result', 'process'],
) as dag:

    def process_completed(**context):
        """
        扫描 Redis completed 队列中的 docking 任务：
        1. 下载 MinIO 结果 CSV
        2. 解析 SMILES → docking_score 映射
        3. 批量 UPDATE docking_result 表
        复用 DockingResultProcessor 的核心逻辑。
        """
        from common.redis_utils import get_redis, PROCESSED_SET_KEY
        from common.minio_utils import download_file_content
        from common.db_utils import get_sync_connection
        import csv, io, json

        r = get_redis()
        completed_ids = r.lrange('aidd:queue:completed', 0, -1)
        processed = 0

        for tid in completed_ids:
            task_id = tid.decode() if isinstance(tid, bytes) else tid
            if r.sismember(PROCESSED_SET_KEY, task_id):
                continue

            task_data = r.hgetall(f'aidd:tasks:{task_id}')
            task_data = {
                (k.decode() if isinstance(k, bytes) else k):
                (v.decode() if isinstance(v, bytes) else v)
                for k, v in task_data.items()
            }

            if task_data.get('service') != 'docking':
                continue
            if task_data.get('status') not in ('completed', 'success'):
                continue

            # 解析 output_files 获取 _dock.csv 路径
            output_files = json.loads(task_data.get('output_files', '[]'))
            csv_file = next(
                (f for f in output_files if f.endswith('_dock.csv')), None
            )
            if not csv_file:
                r.sadd(PROCESSED_SET_KEY, task_id)
                continue

            # 下载并解析 CSV
            csv_content = download_file_content(
                csv_file.replace('minio://', '')
            )
            reader = csv.DictReader(io.StringIO(csv_content))
            scores = {
                row['smiles']: float(row.get('docking_score', 0))
                for row in reader if row.get('smiles')
            }

            # 写入 DB
            input_params = json.loads(task_data.get('input_params', '{}'))
            receptor_id = input_params.get('receptor_id')
            project_id = input_params.get('project_id')

            conn = get_sync_connection()
            try:
                with conn.cursor() as cur:
                    for smiles, score in scores.items():
                        cur.execute("""
                            UPDATE docking_result
                            SET docking_score = %s,
                                status = 'success',
                                updated_at = NOW()
                            WHERE smiles = %s
                              AND receptor_id = %s
                              AND project_id = %s
                              AND status = 'pending'
                        """, (score, smiles, receptor_id, project_id))
                conn.commit()
            finally:
                conn.close()

            r.sadd(PROCESSED_SET_KEY, task_id)
            processed += 1

        return processed

    def process_failed(**context):
        """将失败任务对应的 pending 记录标记为 failed"""
        from common.redis_utils import get_redis, PROCESSED_SET_KEY
        from common.db_utils import get_sync_connection
        import json

        r = get_redis()
        failed_ids = r.lrange('aidd:queue:failed', 0, -1)
        processed = 0

        for tid in failed_ids:
            task_id = tid.decode() if isinstance(tid, bytes) else tid
            if r.sismember(PROCESSED_SET_KEY, task_id):
                continue

            task_data = r.hgetall(f'aidd:tasks:{task_id}')
            task_data = {
                (k.decode() if isinstance(k, bytes) else k):
                (v.decode() if isinstance(v, bytes) else v)
                for k, v in task_data.items()
            }

            if task_data.get('service') != 'docking':
                continue

            input_params = json.loads(task_data.get('input_params', '{}'))
            receptor_id = input_params.get('receptor_id')
            project_id = input_params.get('project_id')
            error_msg = task_data.get('error_message', 'Unknown error')

            if receptor_id and project_id:
                conn = get_sync_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE docking_result
                            SET status = 'failed',
                                error_message = %s,
                                updated_at = NOW()
                            WHERE receptor_id = %s
                              AND project_id = %s
                              AND status = 'pending'
                        """, (error_msg, receptor_id, project_id))
                    conn.commit()
                finally:
                    conn.close()

            r.sadd(PROCESSED_SET_KEY, task_id)
            processed += 1

        return processed

    t_completed = PythonOperator(
        task_id='process_completed_tasks',
        python_callable=process_completed,
    )
    t_failed = PythonOperator(
        task_id='process_failed_tasks',
        python_callable=process_failed,
    )

    t_completed >> t_failed
```

### 4.5 admet_compute_dag — ADMET 单次计算流水线

**用途**: 用户手动触发（通过 Airflow UI "Trigger DAG w/ config"）  
**调度**: 无自动调度（`schedule_interval=None`）  
**参数**: 通过 `dag_run.conf` 传入

```
[validate_input] → [push_to_redis] → [wait_completion] → [log_result]
```

```python
# dags/admet_compute_dag.py

with DAG(
    dag_id='admet_compute',
    schedule_interval=None,  # 仅手动触发
    catchup=False,
    tags=['admet', 'compute', 'manual'],
    params={
        'smiles': Param(type='array', description='SMILES 列表'),
        'input_file': Param(type='string', default='', description='MinIO 输入文件路径'),
        'priority': Param(type='integer', default=2, description='优先级 0-4'),
        'timeout': Param(type='integer', default=3600, description='超时时间（秒）'),
    },
    render_template_as_native_obj=True,
) as dag:

    def validate_and_submit(**context):
        """验证参数并提交任务到 Redis"""
        from common.redis_utils import push_task_to_redis
        import uuid

        conf = context['dag_run'].conf or {}
        smiles = conf.get('smiles', [])
        input_file = conf.get('input_file', '')
        priority = conf.get('priority', 2)

        if not smiles and not input_file:
            raise ValueError("必须提供 smiles 列表或 input_file 路径")

        task_id = str(uuid.uuid4())
        input_params = {}
        if smiles:
            input_params['smiles'] = smiles
        if input_file:
            input_params['input_file'] = input_file

        push_task_to_redis(
            task_id=task_id,
            service='admet',
            task_type='qikprop',
            priority=priority,
            input_params=input_params,
            resource_cpu_cores=2,
            resource_memory_gb=4.0,
        )
        context['ti'].xcom_push(key='task_id', value=task_id)
        return task_id

    def wait_for_completion(**context):
        """轮询 Redis 等待任务完成（Deferrable 替代方案）"""
        from common.redis_utils import get_redis
        import time

        task_id = context['ti'].xcom_pull(
            task_ids='validate_and_submit', key='task_id'
        )
        r = get_redis()
        timeout = context['dag_run'].conf.get('timeout', 3600)
        start = time.time()

        while time.time() - start < timeout:
            task_data = r.hgetall(f'aidd:tasks:{task_id}')
            status = (task_data.get(b'status') or task_data.get('status', b'')).decode() \
                if isinstance(task_data.get(b'status', task_data.get('status', '')), bytes) \
                else task_data.get('status', '')

            if status == 'success':
                return {'status': 'success', 'task_id': task_id}
            elif status in ('failed', 'cancelled', 'timeout'):
                error = task_data.get('error_message', b'').decode() \
                    if isinstance(task_data.get('error_message', ''), bytes) \
                    else task_data.get('error_message', '')
                raise Exception(f"任务失败: {error}")

            time.sleep(10)  # 每10秒检查一次

        raise TimeoutError(f"任务 {task_id} 超时 ({timeout}s)")

    t_submit = PythonOperator(
        task_id='validate_and_submit',
        python_callable=validate_and_submit,
    )
    t_wait = PythonOperator(
        task_id='wait_for_completion',
        python_callable=wait_for_completion,
        execution_timeout=timedelta(hours=2),
    )

    t_submit >> t_wait
```

### 4.6 docking_compute_dag — Docking 单次计算流水线

**用途**: 手动触发  
**参数**: `dag_run.conf` 包含 `input_file`, `grid_file`, `target`, `precision`, `n_poses`, `project` 等

```
[validate_input] → [push_to_redis] → [wait_completion] → [trigger_result_process]
```

结构与 `admet_compute_dag` 类似，额外增加：
- `grid_file` 和 `target` 为必填参数
- 完成后自动触发 `docking_result_process` DAG 立即处理结果

### 4.7 worker_heartbeat_dag — Worker 心跳检查

**替代**: `HeartbeatChecker`  
**调度**: `*/1 * * * *`（每分钟）  
**说明**: 从 Redis/DB 检查 Worker 最后心跳时间，标记超时的节点为 OFFLINE

```python
# dags/worker_heartbeat_dag.py

with DAG(
    dag_id='worker_heartbeat_check',
    schedule_interval='*/1 * * * *',  # 每分钟
    catchup=False,
    tags=['worker', 'heartbeat', 'monitor'],
) as dag:

    def check_heartbeats(**context):
        """检查 Worker 心跳，标记超时节点"""
        from common.db_utils import get_sync_connection
        from datetime import datetime, timedelta

        TIMEOUT_SECONDS = 120  # 生产环境适当放宽超时
        conn = get_sync_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE workers
                    SET status = 'offline'
                    WHERE status != 'offline'
                      AND last_heartbeat < %s
                    RETURNING id, hostname
                """, (datetime.now() - timedelta(seconds=TIMEOUT_SECONDS),))
                timed_out = cur.fetchall()
                conn.commit()
        finally:
            conn.close()

        if timed_out:
            for wid, hostname in timed_out:
                print(f"Worker 心跳超时: {wid} ({hostname})")
        return len(timed_out)

    PythonOperator(
        task_id='check_heartbeats',
        python_callable=check_heartbeats,
    )
```

---

## 5. 公共模块设计（dags/common/）

### 5.1 config.py

```python
"""
Airflow DAG 共享配置
从 Airflow Variables 或环境变量读取
"""
from airflow.models import Variable

# 数据库连接
POSTGRES_HOST = Variable.get('POSTGRES_HOST', default_var='10.18.85.10')
POSTGRES_PORT = int(Variable.get('POSTGRES_PORT', default_var='30684'))
POSTGRES_USER = Variable.get('POSTGRES_USER', default_var='appuser')
POSTGRES_PASSWORD = Variable.get('POSTGRES_PASSWORD', default_var='strongpassword')
POSTGRES_DB = Variable.get('POSTGRES_DB', default_var='aichemol')

# Redis
REDIS_HOST = Variable.get('REDIS_HOST', default_var='10.18.85.10')
REDIS_PORT = int(Variable.get('REDIS_PORT', default_var='6379'))

# MinIO
MINIO_ENDPOINT = Variable.get('MINIO_ENDPOINT', default_var='172.19.80.100:9090')
MINIO_ACCESS_KEY = Variable.get('MINIO_ACCESS_KEY', default_var='admin')
MINIO_SECRET_KEY = Variable.get('MINIO_SECRET_KEY', default_var='minio_test_password_2025')
MINIO_BUCKET = Variable.get('MINIO_BUCKET', default_var='aidd-files')

# 业务参数
ADMET_BATCH_SIZE = 50
ADMET_PRIORITY = 4   # BATCH
DOCKING_BATCH_SIZE = 10
DOCKING_PRIORITY = 3  # LOW
```

### 5.2 redis_utils.py

```python
"""Redis 工具函数，封装与 aidd-platform 兼容的队列操作"""
import json
import redis
from datetime import datetime
from .config import REDIS_HOST, REDIS_PORT

PROCESSED_SET_KEY = "aidd:docking:processed"

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

def push_task_to_redis(
    task_id: str,
    service: str,
    task_type: str,
    priority: int,
    input_params: dict,
    resource_cpu_cores: int = 1,
    resource_memory_gb: float = 1.0,
    resource_gpu_count: int = 0,
    timeout_seconds: int = 3600,
):
    """
    提交任务到 Redis，保持与 aidd-platform RedisTaskQueue 兼容的数据结构。
    Worker 端 (aidd-toolkit) 无需任何改动即可消费此任务。
    """
    r = get_redis()
    now = datetime.now().isoformat()
    task_data = {
        'id': task_id,
        'service': service,
        'task_type': task_type,
        'status': 'pending',
        'priority': str(priority),
        'input_params': json.dumps(input_params),
        'input_files': json.dumps([]),
        'output_files': json.dumps([]),
        'result': '',
        'worker_id': '',
        'retry_count': '0',
        'max_retries': '3',
        'timeout_seconds': str(timeout_seconds),
        'error_message': '',
        'resource_cpu_cores': str(resource_cpu_cores),
        'resource_memory_gb': str(resource_memory_gb),
        'resource_gpu_count': str(resource_gpu_count),
        'resource_gpu_memory_gb': '0.0',
        'created_at': now,
        'started_at': '',
        'completed_at': '',
    }

    pipe = r.pipeline()
    # Hash: 任务详情
    pipe.hset(f'aidd:tasks:{task_id}', mapping=task_data)
    # ZSet: 全局优先级队列（score = priority * 100 + timestamp 小数）
    score = priority * 100 + datetime.now().timestamp() / 1e10
    pipe.zadd('aidd:queue:pending', {task_id: score})
    # List: 服务专用队列
    pipe.rpush(f'aidd:queue:service:{service}', task_id)
    # Stats
    pipe.hincrby('aidd:stats', 'total_submitted', 1)
    pipe.execute()
```

### 5.3 db_utils.py

```python
"""PostgreSQL 同步连接工具"""
import psycopg2
from .config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

def get_sync_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
    )
```

### 5.4 minio_utils.py

```python
"""MinIO 文件操作工具"""
from minio import Minio
import io
from .config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

def upload_csv_string(content: str, object_name: str):
    """上传 CSV 字符串到 MinIO"""
    client = get_minio_client()
    data = content.encode('utf-8')
    client.put_object(
        MINIO_BUCKET, object_name,
        io.BytesIO(data), len(data),
        content_type='text/csv',
    )
    return f'minio://{object_name}'

def download_file_content(object_name: str) -> str:
    """从 MinIO 下载文件内容为字符串"""
    client = get_minio_client()
    response = client.get_object(MINIO_BUCKET, object_name)
    return response.read().decode('utf-8')
```

---

## 6. aidd-toolkit 改动（极小）

**核心原则：Worker 端几乎不改。** Redis 数据结构保持兼容，Worker 继续从 `aidd:queue:service:{service}` 消费任务。

唯一建议的改动：

### 6.1 增强 Prometheus 指标暴露

当前 `telemetry.py` 已有指标定义，需确保：

```python
# common/telemetry.py 补充指标（用于 Grafana 展示）

# Worker 资源指标
WORKER_CPU_USAGE = Gauge(
    'aidd_worker_cpu_usage_cores', '已使用 CPU 核数', ['worker_id']
)
WORKER_MEMORY_USAGE = Gauge(
    'aidd_worker_memory_usage_gb', '已使用内存 GB', ['worker_id']
)
WORKER_GPU_USAGE = Gauge(
    'aidd_worker_gpu_usage_count', '已使用 GPU 数', ['worker_id']
)
WORKER_TASK_QUEUE_DEPTH = Gauge(
    'aidd_worker_task_queue_depth', '待处理任务数', ['worker_id', 'service']
)
```

### 6.2 心跳上报保持不变

Worker 继续向 PostgreSQL `workers` 表写心跳。Airflow 的 `worker_heartbeat_dag` 读取此表进行超时检测。

---

## 7. 监控方案：Grafana + Prometheus

### 7.1 Prometheus 配置

```yaml
# monitor/prometheus.yml（已有，需补充）

scrape_configs:
  - job_name: 'aidd-workers'
    scrape_interval: 15s
    static_configs:
      # ADMET Worker
      - targets: ['admet-worker-host:9090']
        labels:
          service: admet
      # Docking Worker
      - targets: ['docking-worker-host:9090']
        labels:
          service: docking

  - job_name: 'airflow'
    scrape_interval: 30s
    static_configs:
      - targets: ['airflow-host:8080']
```

### 7.2 Grafana Dashboard

| 面板 | 数据源 | 指标 |
|---|---|---|
| Worker 在线状态 | PostgreSQL | `SELECT id, hostname, status FROM workers` |
| CPU 使用率 | Prometheus | `aidd_worker_cpu_usage_cores` |
| 内存使用率 | Prometheus | `aidd_worker_memory_usage_gb` |
| GPU 使用率 | Prometheus | `aidd_worker_gpu_usage_count` |
| 任务完成趋势 | Prometheus | `rate(aidd_tasks_total[5m])` |
| 任务耗时分布 | Prometheus | `aidd_task_duration_seconds` |
| 队列深度 | Prometheus / Redis | `aidd_worker_task_queue_depth` |
| Airflow DAG 状态 | Airflow API | DAG Run 成功/失败统计 |

---

## 8. 部署架构

### 8.1 Docker Compose 新增服务

```yaml
# docker-compose.yml 新增

services:
  # Airflow 组件
  airflow-webserver:
    image: apache/airflow:2.9-python3.11
    command: webserver
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/1
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres:5432/airflow
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
    depends_on:
      - airflow-init

  airflow-scheduler:
    image: apache/airflow:2.9-python3.11
    command: scheduler
    environment: *airflow-env  # 同上
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

  airflow-worker:
    image: apache/airflow:2.9-python3.11
    command: celery worker
    environment: *airflow-env
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

  airflow-init:
    image: apache/airflow:2.9-python3.11
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create --username admin --password admin
          --firstname Admin --lastname User --role Admin --email admin@local
      "
    environment: *airflow-env

  # 监控组件
  prometheus:
    image: prom/prometheus:v2.51.0
    ports:
      - "9191:9090"
    volumes:
      - ./monitor/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:10.4.0
    ports:
      - "3000:3000"
    volumes:
      - ./monitor/grafana_dashboards:/etc/grafana/provisioning/dashboards
      - grafana-data:/var/lib/grafana
```

### 8.2 服务端口规划

| 服务 | 端口 | 说明 |
|---|---|---|
| Airflow Web UI | 8080 | **用户主入口**（替代原 platform-front） |
| Grafana | 3000 | 资源监控面板 |
| Prometheus | 9191 | 指标采集 |
| Redis | 6379 | 任务队列 (db0=业务, db1=Airflow Celery Broker) |
| PostgreSQL | 30684 | aichemol(业务) + airflow(元数据) |
| MinIO | 9090 | 文件存储 |

---

## 9. 迁移步骤

### Phase 1：基础设施搭建（1周）

1. 部署 Airflow (WebServer + Scheduler + Worker + PostgreSQL metadata DB)
2. 部署 Prometheus + Grafana
3. 编写 `dags/common/` 公共模块
4. 配置 Airflow Variables（DB/Redis/MinIO 连接信息）
5. 验证 Airflow 能正常连接 Redis 和 PostgreSQL

### Phase 2：定时任务迁移（1周）

1. 编写 `admet_sync_dag` 并测试
2. 编写 `docking_sync_dag` 并测试
3. 编写 `docking_result_dag` 并测试
4. 编写 `worker_heartbeat_dag` 并测试
5. **双跑验证**：Airflow DAG 与现有 Platform 同时运行，对比结果一致性
6. 关闭 Platform 中对应的 `asyncio.create_task()` 后台任务

### Phase 3：手动任务迁移（1周）

1. 编写 `admet_compute_dag`（手动触发）
2. 编写 `docking_compute_dag`（手动触发）
3. 测试 Airflow UI "Trigger DAG w/ config" 提交任务
4. 验证端到端流程：提交 → Worker 执行 → 结果写入 DB

### Phase 4：监控面板搭建（1周）

1. 补充 aidd-toolkit Prometheus 指标
2. 配置 Grafana Dashboard
3. 设置告警规则（Worker 离线、队列堆积、任务失败率）
4. 验证监控覆盖度

### Phase 5：下线旧组件

1. 停止 aidd-platform FastAPI 进程
2. 归档 aidd-platform-front 代码仓库
3. 清理 Redis 中残留的旧状态数据
4. 更新文档

---

## 10. 风险与应对

| 风险 | 影响 | 应对措施 |
|---|---|---|
| Airflow 调度延迟（5-30s） | 手动提交的任务响应变慢 | 用户可接受；若需更快，可配置 `scheduler_heartbeat_sec=5` |
| XCom 传输大数据 | ADMET 扫描可能返回数千条化合物 | 大批量数据写入临时表或 MinIO，XCom 仅传 ID/路径 |
| Airflow Worker 与计算 Worker 混淆 | 概念冲突 | Airflow Worker 仅做编排（推任务到 Redis），不做计算 |
| Redis db 冲突 | Airflow Celery Broker 与业务队列共用 Redis | 使用不同 db（业务=db0, Airflow=db1） |
| 丢失细粒度资源管理 | 无法精确匹配 Worker 资源 | 短期：依赖 Worker 端并发控制（`max_concurrent=4`）；长期：自定义 Airflow Plugin 读取 Worker 资源 |
| 优先级退化 | Airflow priority_weight 不如自研灵活 | 通过 Pool + priority_weight 组合近似实现；BATCH 任务放入独立 Pool |
