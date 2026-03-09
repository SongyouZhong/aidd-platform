# AIDD Platform — Airflow 迁移指南

> 本文档说明如何从原 FastAPI + Vue 3 架构迁移到 Airflow + Grafana 架构。

## 架构变更

| 原组件 | 新组件 | 说明 |
|--------|--------|------|
| aidd-platform (FastAPI) | Airflow DAGs | 任务编排和调度 |
| aidd-platform-front (Vue 3) | Airflow Web UI | 任务管理和监控 |
| 自研 Scheduler/Dispatcher | Airflow Scheduler | DAG 调度 |
| 自研监控 | Grafana + Prometheus | Worker 资源监控 |

**不变的组件：** Redis、PostgreSQL、MinIO、aidd-toolkit（Worker 端几乎不改）

---

## 新增文件结构

```
aidd-platform/
├── dags/                              # Airflow DAG 目录
│   ├── common/                        # 公共模块
│   │   ├── config.py                  # 共享配置
│   │   ├── db_utils.py                # PostgreSQL 工具
│   │   ├── redis_utils.py             # Redis 队列操作
│   │   └── minio_utils.py             # MinIO 文件操作
│   ├── operators/                     # 自定义 Operator
│   │   ├── redis_task_operator.py     # Redis 任务提交
│   │   └── minio_operator.py          # MinIO 文件操作
│   ├── sensors/                       # 自定义 Sensor
│   │   └── redis_task_sensor.py       # 等待任务完成
│   ├── admet_sync_dag.py              # ADMET 自动同步（替代 AdmetSyncChecker）
│   ├── docking_sync_dag.py            # Docking 自动同步（替代 DockingSyncChecker）
│   ├── docking_result_dag.py          # Docking 结果后处理（替代 DockingResultProcessor）
│   ├── admet_compute_dag.py           # ADMET 手动计算
│   ├── docking_compute_dag.py         # Docking 手动计算
│   └── worker_heartbeat_dag.py        # Worker 心跳检查（替代 HeartbeatChecker）
├── monitor/                           # 监控配置
│   ├── prometheus.yml                 # Prometheus 采集配置
│   └── grafana/                       # Grafana 配置
│       └── provisioning/
│           ├── datasources/           # 数据源自动配置
│           └── dashboards/            # Dashboard 自动配置
├── docker-compose-airflow.yml         # Airflow 架构 Docker Compose
└── requirements-airflow.txt           # Airflow DAG 依赖
```

---

## DAG 对照表

| DAG ID | 替代的原组件 | 调度周期 | 说明 |
|--------|-------------|---------|------|
| `admet_sync` | `AdmetSyncChecker` | 每天 02:00 | 扫描缺 ADMET 结果的化合物并提交计算 |
| `docking_sync` | `DockingSyncChecker` | 每小时 | 扫描缺 docking 结果的化合物并提交计算 |
| `docking_result_process` | `DockingResultProcessor` | 每 2 分钟 | 解析已完成的 docking 结果写入 DB |
| `admet_compute` | API 手动提交 | 手动触发 | 用户通过 Airflow UI 提交 ADMET 计算 |
| `docking_compute` | API 手动提交 | 手动触发 | 用户通过 Airflow UI 提交 Docking 计算 |
| `worker_heartbeat_check` | `HeartbeatChecker` | 每分钟 | 检查 Worker 心跳超时 |

---

## 服务端口规划

| 服务 | 端口 | 说明 |
|------|------|------|
| Airflow Web UI | 8080 | **用户主入口**（替代原 platform-front） |
| Grafana | 3000 | 资源监控面板 |
| Prometheus | 9191 | 指标采集 |
| Redis | 6379 | 任务队列 (db0=业务, db1=Airflow Celery Broker) |
| PostgreSQL | 30684 | aichemol(业务) + airflow(元数据) |
| MinIO | 9090 | 文件存储 |

---

## 部署步骤

### 1. 准备工作

```bash
# 在 PostgreSQL 中创建 Airflow 元数据库
PGPASSWORD=strongpassword psql -h 10.18.85.10 -p 30684 -U appuser -d postgres -c "CREATE DATABASE airflow;"
```

### 2. 初始化 Airflow

```bash
cd aidd-platform

# 初始化 Airflow 元数据库并创建管理员用户
docker-compose -f docker-compose-airflow.yml up airflow-init
```

### 3. 启动所有服务

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

### 4. 安装 DAG 依赖

```bash
# 进入 Airflow Worker 容器安装依赖
docker exec -it aidd-airflow-worker pip install -r /opt/airflow/dags/../requirements-airflow.txt
docker exec -it aidd-airflow-scheduler pip install -r /opt/airflow/dags/../requirements-airflow.txt
```

### 5. 验证

- 访问 Airflow UI: http://localhost:8080 (admin/admin)
- 检查 7 个 DAG 是否正常加载
- 访问 Grafana: http://localhost:3000 (admin/admin)
- 检查 Prometheus 数据源是否连通

---

## 手动触发任务

### 通过 Airflow UI

1. 打开 Airflow UI → DAGs 页面
2. 找到 `admet_compute` 或 `docking_compute`
3. 点击 "Trigger DAG w/ config"
4. 输入参数 JSON，例如：

**ADMET 计算：**
```json
{
  "smiles": ["CCO", "c1ccccc1", "CC(=O)O"],
  "project_id": "your-project-id",
  "priority": 2,
  "timeout": 3600
}
```

**Docking 计算：**
```json
{
  "input_file": "minio://tasks/input/xxx.csv",
  "grid_file": "minio://receptors/xxx.zip",
  "receptor_id": "receptor-uuid",
  "project_id": "project-uuid",
  "target": "target_name",
  "precision": "SP",
  "n_poses": 3,
  "timeout": 7200
}
```

### 通过 Airflow REST API

```bash
curl -X POST "http://localhost:8080/api/v1/dags/admet_compute/dagRuns" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "conf": {
      "smiles": ["CCO", "c1ccccc1"],
      "project_id": "test-project"
    }
  }'
```

---

## 双跑验证（Phase 2）

在正式切换之前，建议同时运行 Airflow DAG 和原 Platform 后台任务，对比结果一致性：

1. 两者共享同一 Redis 和 PostgreSQL
2. 确保不会重复提交任务（可通过标记 source 字段区分）
3. 对比 ADMET 和 Docking 结果数量是否一致

确认无误后，关闭 Platform 中对应的 `asyncio.create_task()` 后台任务。

---

## 废弃的组件

以下组件在迁移完成后可以废弃：

| 组件 | 原位置 | 替代方案 |
|------|--------|---------|
| aidd-platform-front | 整个前端项目 | Airflow UI + Grafana |
| TaskDispatcher | `scheduler/dispatcher.py` | Airflow Scheduler |
| PriorityTaskQueue | `scheduler/priority_queue.py` | Airflow 任务优先级 + Pool |
| AdmetSyncChecker | `scheduler/admet_sync.py` | `admet_sync_dag` |
| DockingSyncChecker | `scheduler/docking_sync.py` | `docking_sync_dag` |
| DockingResultProcessor | `scheduler/docking_result_processor.py` | `docking_result_dag` |
| HeartbeatChecker | `scheduler/heartbeat_checker.py` | `worker_heartbeat_dag` |
| FastAPI API 层 | `api/v1/*.py` | Airflow REST API |
| FastAPI main.py | `app/main.py` | 不再需要 |
