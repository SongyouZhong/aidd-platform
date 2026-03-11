# AIDD Platform

> AI 药物设计计算任务调度平台，基于 Apache Airflow 管理计算任务的调度、编排与监控。

与 [aidd-toolkit](../aidd-toolkit) 配合使用：
- **aidd-toolkit**: 提供封装好的计算工具（QikProp、Docking 等）
- **aidd-platform**: 负责任务调度、工作流编排、状态监控

**核心组件**：
- **Apache Airflow 2.9.3**: 工作流调度引擎（CeleryExecutor）
- **Redis**: Celery Broker（任务队列 db1）+ 业务数据（db0）
- **PostgreSQL**: Airflow 元数据库（`airflow` DB）+ 业务数据库
- **MinIO**: 文件对象存储（输入/输出文件）
- **Prometheus**: 本地指标采集
- **Fluentd**: 日志采集，转发至 Elasticsearch / Kibana

---

## 🏗️ 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                      AIDD Platform (本项目)                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                  Apache Airflow                              │    │
│  │  ┌────────────┐  ┌────────────┐  ┌───────────────────────┐  │    │
│  │  │ Webserver  │  │ Scheduler  │  │   Celery Worker       │  │    │
│  │  │  :8080     │  │ (DAG解析)  │  │  (编排，不做计算)      │  │    │
│  │  └────────────┘  └────────────┘  └───────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │         监控栈                                               │    │
│  │  Prometheus :${PROMETHEUS_PORT}  →  Grafana                 │    │
│  │  Fluentd                         →  Kibana / Elasticsearch  │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  外部依赖（独立管理）                                                  │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────────┐      │
│  │  aidd-redis  │  │    PostgreSQL     │  │       MinIO       │      │
│  │  (aidd-net)  │  │  <POSTGRES_HOST>  │  │  <MINIO_ENDPOINT> │      │
│  └──────────────┘  └──────────────────┘  └───────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
   │ ADMET Worker│    │ ADMET Worker│    │Docking Worker│
   │(aidd-toolkit)│   │(aidd-toolkit)│   │(aidd-toolkit)│
   │  Node 1     │    │  Node 2     │    │  Node 3 GPU  │
   └─────────────┘    └─────────────┘    └──────────────┘
```

---

## 📋 DAG 列表

| DAG | 说明 | 触发方式 |
|-----|------|----------|
| `admet_compute_dag` | ADMET 属性预测任务调度 | 外部触发 |
| `admet_sync_dag` | ADMET 结果同步回数据库 | 定时/外部触发 |
| `docking_compute_dag` | 分子对接计算任务调度 | 外部触发 |
| `docking_result_dag` | 对接结果解析与入库 | 外部触发 |
| `docking_sync_dag` | 对接状态同步 | 定时/外部触发 |
| `worker_heartbeat_dag` | Worker 心跳检测与状态维护 | 定时（cron） |

---

## 📁 项目结构

```
/aidd-platform
├── app/                        # FastAPI 服务（Worker 注册/心跳 API）
│   ├── main.py                 # FastAPI 入口（run.py 启动）
│   ├── config.py               # 配置管理
│   ├── api/v1/                 # REST API（tasks / workers / health）
│   ├── models/                 # 数据模型（task / worker / resource）
│   ├── mq/                     # Redis 消息队列客户端
│   ├── scheduler/              # 调度器（dispatcher / resource_manager）
│   ├── storage/                # MinIO 存储客户端
│   ├── worker/                 # Worker 通信客户端
│   └── db/                     # 数据库会话
│
├── dags/                       # Airflow DAG 定义
│   ├── .airflowignore          # 排除 common/ operators/ sensors/ 目录
│   ├── admet_compute_dag.py
│   ├── admet_sync_dag.py
│   ├── docking_compute_dag.py
│   ├── docking_result_dag.py
│   ├── docking_sync_dag.py
│   ├── worker_heartbeat_dag.py
│   ├── common/                 # 公共工具（config / db / minio 等）
│   ├── operators/              # 自定义 Operator
│   └── sensors/                # 自定义 Sensor
│
├── monitor/
│   ├── prometheus.yml          # Prometheus 采集配置
│   └── fluent.conf             # Fluentd 日志采集配置（→ 公司 ES）
│
├── config/
│   └── config.yml              # 业务配置（YAML）
│
├── scripts/
│   └── init-db.sql             # 业务库初始化脚本
│
├── .env                        # 所有环境变量（不提交到 git）
├── docker-compose-airflow.yml  # 生产部署（Airflow + 监控栈）
├── docker-compose.yml          # 旧版 FastAPI 部署（已弃用）
├── requirements-airflow.txt    # DAG 依赖包
└── environment.yml             # Mamba/Conda 本地开发环境
```

---

## 🚀 快速开始

### 前置条件

| 依赖 | 说明 |
|------|------|
| Docker + docker-compose | 容器运行时 |
| `aidd-network` Docker 网络 | `docker network create aidd-network` |
| `aidd-redis` 容器 | 运行在 `aidd-network` 上，DNS 别名 `redis` |
| PostgreSQL `airflow` 数据库 | UTF-8 编码，见下方初始化步骤 |
| PostgreSQL 业务数据库 | 业务库，已存在 |

### 1. 配置 `.env`

```bash
# .env 文件已包含所有配置，首次使用请核实以下关键项：
# POSTGRES_HOST / POSTGRES_PORT / POSTGRES_USER / POSTGRES_PASSWORD
# REDIS_HOST（容器名或 IP）
# MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
# AIRFLOW_FERNET_KEY（生成: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"）
# AIRFLOW_ADMIN_USERNAME / AIRFLOW_ADMIN_PASSWORD
```

### 2. 创建 Airflow 元数据库（仅首次）

```bash
PGPASSWORD=<POSTGRES_PASSWORD> psql -h <POSTGRES_HOST> -p <POSTGRES_PORT> -U <POSTGRES_USER> -d postgres \
  -c "CREATE DATABASE airflow ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0;"
```

### 3. 初始化 Airflow（仅首次）

```bash
docker-compose -f docker-compose-airflow.yml up airflow-init
```

初始化完成后日志应出现 `Admin user admin created`。

### 4. 启动所有服务

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

### 5. 安装 DAG 依赖（首次或依赖更新后）

```bash
# 将 requirements 复制到容器并安装
docker cp requirements-airflow.txt aidd-airflow-worker:/tmp/
docker exec aidd-airflow-worker python -m pip install -r /tmp/requirements-airflow.txt

docker cp requirements-airflow.txt aidd-airflow-scheduler:/tmp/
docker exec aidd-airflow-scheduler python -m pip install -r /tmp/requirements-airflow.txt

docker cp requirements-airflow.txt aidd-airflow-webserver:/tmp/
docker exec aidd-airflow-webserver python -m pip install -r /tmp/requirements-airflow.txt
```

### 6. 验证服务状态

```bash
docker ps --filter name=aidd --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

| 容器 | 说明 | 端口 |
|------|------|------|
| `aidd-airflow-webserver` | Airflow Web UI | 8080 |
| `aidd-airflow-scheduler` | DAG 调度器 | — |
| `aidd-airflow-worker` | Celery Worker（编排层） | — |
| `aidd-airflow-init` | 初始化容器（常驻） | — |
| `aidd-prometheus` | 指标采集 | 9191 |
| `aidd-fluentd` | 日志采集 | — |

### 7. 访问服务

| 服务 | 地址 |
|------|------|
| Airflow Web UI | http://localhost:8080 |
| Prometheus | http://localhost:${PROMETHEUS_PORT} |
| Grafana | 见 `.env` `GRAFANA_URL` |
| Kibana | 见 `.env` `KIBANA_URL` |

---

## 📦 MinIO 对象存储

平台使用 MinIO 作为文件存储后端，存储任务的输入/输出文件。

### 配置

在 `.env` 中设置：

```env
MINIO_ENDPOINT=<host>:<port>
MINIO_ACCESS_KEY=<access_key>
MINIO_SECRET_KEY=<secret_key>
MINIO_BUCKET=<bucket_name>
```

### 使用示例

```python
from app.storage import get_storage

storage = get_storage()

# 上传/下载
storage.upload('tasks/input.csv', csv_data)
data = storage.download('tasks/input.csv')

# 预签名 URL（供客户端直接访问）
url = storage.get_presigned_url('tasks/result.csv', expires_hours=24)

# 临时文件
temp_path = storage.save_temp('temp.csv', data)
storage.download_to_temp('input.csv')
storage.clean_temp()
```

---

## 🔗 与 aidd-toolkit 集成

aidd-toolkit 中的 Worker 通过 HTTP 调用本平台的 FastAPI（`run.py`）进行注册/心跳：

```
Worker 启动  →  POST /api/v1/workers/register  →  返回 worker_id
Worker 运行  →  POST /api/v1/workers/{id}/heartbeat  (每 10 秒)
Worker 停止  →  DELETE /api/v1/workers/{worker_id}
```

**超时处理**：Platform 每 10 秒检查所有 Worker 的 `last_heartbeat`，超过 30 秒无心跳则自动标记为 `offline`，并将该 Worker 上的任务重新入队。

### 启动 Worker

```bash
cd /path/to/aidd-toolkit
mamba activate aidd-toolkit-admet
python -m services.admet_predict.wrapper
```

### FastAPI 服务（本地开发）

```bash
# 激活环境
mamba activate aidd-platform

# 启动
uvicorn app.main:app --reload --host 0.0.0.0 --port 8333
# Swagger UI: http://localhost:8333/docs
```

---

## 📡 FastAPI 接口概览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/tasks` | 创建任务（入队 Redis） |
| GET | `/api/v1/tasks/{id}` | 获取任务详情 |
| GET | `/api/v1/tasks/stats` | 获取队列统计 |
| DELETE | `/api/v1/tasks/{id}` | 取消任务 |
| POST | `/api/v1/workers/register` | 注册 Worker |
| POST | `/api/v1/workers/{id}/heartbeat` | Worker 心跳 |
| GET | `/api/v1/workers/stats` | 集群资源统计 |
| GET | `/api/v1/health` | 健康检查 |

---

## 📊 监控

### Grafana

Prometheus 在本机 `:${PROMETHEUS_PORT}` 暴露指标。若使用外部 Grafana，需手动添加 Prometheus Datasource：

> Grafana → Configuration → Data Sources → Add → Prometheus
> URL: `http://<本机IP>:<PROMETHEUS_PORT>`

### Kibana

Fluentd 采集 Airflow 任务日志，推送至 Elasticsearch，索引前缀由 `.env` 中 `ELASTICSEARCH_INDEX` 控制（默认 `aidd-airflow-logs-*`）：

> Kibana → Discover → 选择对应索引模式

> 具体环境的 Grafana / Kibana 地址见 [docs/deployment-internal.md](docs/deployment-internal.md)。

---

## 🔧 常见问题

**重启服务时报 `ContainerConfig` 错误**

docker-compose v1 的已知 bug，需先手动删除旧容器再重启：

```bash
docker rm -f aidd-airflow-webserver aidd-airflow-scheduler aidd-airflow-worker aidd-airflow-init
docker-compose -f docker-compose-airflow.yml up -d
```

**DAG Import Error**

检查 `.airflowignore` 是否存在于 `dags/` 目录，确保 `common/`、`operators/`、`sensors/` 已被排除：

```bash
docker exec aidd-airflow-scheduler airflow dags list-import-errors
```

**Fernet Key 错误**

若 Airflow 报加密相关错误，请确认 `.env` 中的 `AIRFLOW_FERNET_KEY` 与初始化时使用的值一致。
