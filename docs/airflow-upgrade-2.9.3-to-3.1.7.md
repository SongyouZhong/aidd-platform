# Airflow 升级记录：2.9.3 → 3.1.7

**日期**：2026-03-11  
**项目**：aidd-platform  

---

## 背景

aidd-platform 使用 Airflow 替代原有的后台调度服务（AdmetSyncChecker、DockingSyncChecker、HeartbeatChecker 等），原版本为 **2.9.3**，本次升级至 **3.1.7**。

---

## 一、文件变更清单

### 1. `docker-compose-airflow.yml`

#### 1.1 镜像版本升级

所有 Airflow 服务镜像从 `2.9.3` 升级至 `3.1.7`：

```yaml
# 旧版
image: apache/airflow:2.9.3-python3.11

# 新版
image: apache/airflow:3.1.7-python3.11
```

涉及服务：`airflow-init`、`airflow-webserver`、`airflow-scheduler`、`airflow-worker`

#### 1.2 API Server 改名

Airflow 3.x 将 `webserver` 命令重命名为 `api-server`：

```yaml
# 旧版
command: webserver

# 新版
command: api-server
```

#### 1.3 Web UI / API 配置迁移

Airflow 3.x 将 webserver workers 配置迁移至 `[api]` section，并引入 `SimpleAuthManager` 替代旧的用户管理命令：

```yaml
# 旧版
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: ${AIRFLOW_TIMEZONE}
AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
AIRFLOW__WEBSERVER__WORKERS: ${AIRFLOW_WEBSERVER_WORKERS}

# 新版
AIRFLOW__API__WORKERS: ${AIRFLOW_WEBSERVER_WORKERS}
AIRFLOW__SIMPLE_AUTH_MANAGER__ADMIN_USERNAME: ${AIRFLOW_ADMIN_USERNAME}
AIRFLOW__SIMPLE_AUTH_MANAGER__ADMIN_PASSWORD: ${AIRFLOW_ADMIN_PASSWORD}
```

#### 1.4 airflow-init 命令简化

Airflow 3.x 移除了 `airflow users create` CLI 命令，改为通过 `SimpleAuthManager` 环境变量配置管理员账号：

```yaml
# 旧版
airflow db migrate &&
airflow users create \
  --username ${AIRFLOW_ADMIN_USERNAME} \
  --password ${AIRFLOW_ADMIN_PASSWORD} \
  --firstname Admin --lastname User \
  --role Admin \
  --email ${AIRFLOW_ADMIN_EMAIL} || true

# 新版
airflow db migrate
```

#### 1.5 新增 airflow-dag-processor 服务

Airflow 3.x 将 DAG 文件解析器从 Scheduler 进程中独立拆分为专用服务：

```yaml
airflow-dag-processor:
  image: apache/airflow:3.1.7-python3.11
  container_name: aidd-airflow-dag-processor
  command: dag-processor
  environment:
    <<: *airflow-env
  volumes: *airflow-volumes
  networks:
    - aidd-network
  restart: unless-stopped
```

#### 1.6 Healthcheck 端点更新

Airflow 3.x 将健康检查端点从 `/health` 移至 `/api/v2/monitor/health`：

```yaml
# 旧版
test: ["CMD", "curl", "-f", "http://localhost:8080/health"]

# 新版
test: ["CMD", "curl", "-f", "http://localhost:8080/api/v2/monitor/health"]
```

---

### 2. `requirements-airflow.txt`

Airflow 3.x 将 `CeleryExecutor` 从核心包中移出，需要单独安装 provider：

```txt
# 新增
apache-airflow-providers-celery>=1.3.0
```

---

### 3. DAG 文件（共 5 个）

涉及文件：`admet_compute_dag.py`、`admet_sync_dag.py`、`docking_compute_dag.py`、`docking_result_dag.py`、`docking_sync_dag.py`、`worker_heartbeat_dag.py`

#### 3.1 删除 `days_ago` 导入（已从 Airflow 3.x 移除）

```python
# 旧版
from airflow.utils.dates import days_ago

# 新版（删除此行，改用标准库）
from datetime import datetime, timedelta, timezone
```

#### 3.2 `start_date` 改为时区感知日期

```python
# 旧版
start_date=days_ago(1),

# 新版
start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
```

#### 3.3 `schedule_interval` 改为 `schedule`

```python
# 旧版
schedule_interval=None,
schedule_interval='0 2 * * *',
schedule_interval='*/1 * * * *',

# 新版
schedule=None,
schedule='0 2 * * *',
schedule='*/1 * * * *',
```

---

### 4. Operator / Sensor 文件（共 3 个）

涉及文件：`operators/redis_task_operator.py`、`operators/minio_operator.py`、`sensors/redis_task_sensor.py`

#### 4.1 删除 `apply_defaults` 装饰器（已从 Airflow 3.x 移除）

```python
# 旧版
from airflow.utils.decorators import apply_defaults

class MyOperator(BaseOperator):
    @apply_defaults
    def __init__(self, ...):
        ...

# 新版（删除 import 和 @apply_defaults，BaseOperator 已默认包含该行为）
class MyOperator(BaseOperator):
    def __init__(self, ...):
        ...
```

---

## 二、升级步骤

```bash
# Step 1：拉取新镜像
docker pull apache/airflow:3.1.7-python3.11

# Step 2：停止旧版所有服务
docker-compose -f docker-compose-airflow.yml down

# Step 3：执行数据库 Schema 迁移（Airflow 3.x 有大量 Schema 变更）
docker-compose -f docker-compose-airflow.yml run --rm airflow-init

# Step 4：启动所有新版服务
docker-compose -f docker-compose-airflow.yml up -d
```

---

## 三、验证结果

### 容器状态

| 容器 | 状态 |
|------|------|
| `aidd-airflow-init` | Exit 0（正常退出） |
| `aidd-airflow-webserver` | Up (healthy) |
| `aidd-airflow-scheduler` | Up |
| `aidd-airflow-dag-processor` | Up |
| `aidd-airflow-worker` | Up |
| `aidd-fluentd` | Up |
| `aidd-prometheus` | Up |

### 版本确认

```
$ docker exec aidd-airflow-webserver airflow version
3.1.7
```

### Health API

```
GET http://localhost:8080/api/v2/monitor/health

{
  "metadatabase": {"status": "healthy"},
  "scheduler":    {"status": "healthy", "latest_scheduler_heartbeat": "..."},
  "dag_processor":{"status": "healthy", "latest_dag_processor_heartbeat": "..."},
  "triggerer":    {"status": null}
}
```

### DAG 注册（全部 6 个成功）

```
dag_id                  | is_paused | bundle_name
========================+===========+============
admet_compute           | True      | dags-folder
admet_sync              | False     | dags-folder
docking_compute         | True      | dags-folder
docking_result_process  | False     | dags-folder
docking_sync            | False     | dags-folder
worker_heartbeat_check  | False     | dags-folder
```

---

## 四、Airflow 2.x → 3.x 关键破坏性变更速查

| 变更项 | 2.x 旧用法 | 3.x 新用法 |
|--------|-----------|-----------|
| Web UI 启动命令 | `airflow webserver` | `airflow api-server` |
| DAG 解析器 | 内嵌于 Scheduler | 独立进程 `airflow dag-processor` |
| 用户管理 | `airflow users create` | `SimpleAuthManager` 环境变量 |
| CeleryExecutor | 内置 | 需安装 `apache-airflow-providers-celery` |
| `schedule_interval` | `DAG(schedule_interval=...)` | `DAG(schedule=...)` |
| `days_ago()` | `from airflow.utils.dates import days_ago` | 已删除，用 `datetime(..., tzinfo=timezone.utc)` |
| `@apply_defaults` | `from airflow.utils.decorators import apply_defaults` | 已删除，BaseOperator 默认包含 |
| Healthcheck 端点 | `/health` | `/api/v2/monitor/health` |
| workers 配置 | `AIRFLOW__WEBSERVER__WORKERS` | `AIRFLOW__API__WORKERS` |
