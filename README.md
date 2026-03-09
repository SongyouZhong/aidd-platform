# AIDD Platform

> AI 药物设计计算任务调度平台，负责管理计算资源、调度任务、监控 Worker 状态。

与 [aidd-toolkit](../aidd-toolkit) 配合使用：
- **aidd-toolkit**: 提供封装好的计算工具（QikProp、Docking 等）
- **aidd-platform**: 负责任务调度、资源管理、Worker 编排

**核心组件**：
- **Redis**: 任务队列（消息中间件）
- **PostgreSQL**: 任务持久化存储
- **MinIO**: 文件对象存储（输入/输出文件）

---

## 🏗️ 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                      AIDD Platform (本项目)                          │
├─────────────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐        │
│  │  REST API │  │ Scheduler │  │  Worker   │  │  Monitor  │        │
│  │  (FastAPI)│  │  (调度器)  │  │  Manager  │  │  (监控)   │        │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘        │
│        └──────────────┼──────────────┼──────────────┘              │
│                       ▼              ▼                              │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │       Redis MQ (任务队列) + PostgreSQL (持久化)               │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
   ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
   │ ADMET Worker│          │ ADMET Worker│          │Docking Worker│
   │(aidd-toolkit)│          │(aidd-toolkit)│          │(aidd-toolkit)│
   │  Node 1     │          │  Node 2     │          │  Node 3 GPU │
   └─────────────┘          └─────────────┘          └─────────────┘
```

---

## 🔄 Redis MQ 任务队列

### 数据结构

| Key 模式 | 类型 | 说明 |
|----------|------|------|
| `aidd:tasks:{id}` | Hash | 任务详情 |
| `aidd:queue:pending` | Sorted Set | 全局待处理队列（按优先级排序） |
| `aidd:queue:service:{name}` | List | 服务专用队列 |
| `aidd:queue:running` | Set | 运行中的任务 |
| `aidd:queue:completed` | List | 已完成任务（最近1000条） |
| `aidd:queue:failed` | List | 失败任务 |
| `aidd:stats:*` | Hash | 统计信息 |

### 任务流转

```
                    ┌─────────────────┐
                    │   API 提交任务   │
                    └────────┬────────┘
                             ▼
    ┌────────────────────────────────────────────────┐
    │          aidd:queue:pending (ZSet)              │
    │    Score = 优先级 + 时间戳（实现优先级+FIFO）     │
    └────────────────────────┬───────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
    ┌──────────┐       ┌──────────┐       ┌──────────┐
    │ admet 队列│       │docking队列│       │  其他    │
    └────┬─────┘       └────┬─────┘       └────┬─────┘
         │                  │                  │
         ▼                  ▼                  ▼
    ┌─────────────────────────────────────────────┐
    │      Worker 消费 (BRPOP/BZPOPMIN)           │
    └────────────────────────┬────────────────────┘
                             ▼
                    ┌────────────────┐
                    │ aidd:queue:    │
                    │    running     │
                    └────────┬───────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ completed │  │  failed  │  │ 重试入队  │
        └──────────┘  └──────────┘  └──────────┘
```

---

## 📁 项目结构

```
/aidd-platform
├── app/                        # 应用主目录
│   ├── __init__.py
│   ├── main.py                 # FastAPI 入口
│   ├── config.py               # 配置管理
│   │
│   ├── api/                    # REST API
│   │   ├── __init__.py
│   │   ├── deps.py             # 依赖注入
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── health.py       # 健康检查 API
│   │       ├── tasks.py        # 任务 API
│   │       └── workers.py      # Worker 管理 API
│   │
│   ├── models/                 # 数据模型
│   │   ├── __init__.py
│   │   ├── task.py             # 任务模型
│   │   ├── worker.py           # Worker 模型
│   │   └── resource.py         # 资源模型
│   │
│   ├── mq/                     # 消息队列
│   │   ├── __init__.py
│   │   ├── redis_client.py     # Redis 客户端
│   │   ├── task_queue.py       # 任务队列
│   │   └── task_consumer.py    # 任务消费者
│   │
│   ├── scheduler/              # 调度器核心
│   │   ├── __init__.py
│   │   ├── dispatcher.py       # 任务分发器
│   │   ├── resource_manager.py # 资源管理器
│   │   └── priority_queue.py   # 优先级队列
│   │
│   ├── storage/                # 存储模块
│   │   ├── __init__.py
│   │   ├── minio_client.py     # MinIO 对象存储客户端
│   │   └── storage.py          # 统一存储接口
│   │
│   ├── worker/                 # Worker 客户端
│   │   ├── __init__.py
│   │   └── client.py           # Worker 通信客户端
│   │
│   └── db/                     # 数据库
│       ├── __init__.py
│       └── session.py          # 数据库会话
│
├── config/
│   └── config.yml              # 配置文件 (YAML)
│
├── scripts/
│   └── init-db.sql             # 数据库初始化脚本
│
├── docker-compose.yml
├── Dockerfile
└── environment.yml             # Mamba/Conda 环境配置
```

---

## 🚀 快速开始

### 1. 创建虚拟环境（使用 mamba）

```bash
# 使用 mamba 创建环境
mamba env create -f environment.yml

# 激活环境
mamba activate aidd-platform

# 更新环境（如果依赖有变化）
mamba env update -f environment.yml --prune
```

> **注意**: 如果没有安装 mamba，可以使用以下命令安装:
> ```bash
> # 使用 conda 安装 mamba
> conda install -c conda-forge mamba
> 
> # 或使用 miniforge（推荐，已内置 mamba）
> # https://github.com/conda-forge/miniforge
> ```

### 2. 配置环境变量

```bash
export POSTGRES_HOST=10.18.85.10
export POSTGRES_PORT=30684
export REDIS_HOST=localhost
```

或者复制并编辑 `.env` 文件:
```bash
cp .env.example .env
# 编辑 .env 文件配置相关参数
```

### 3. 初始化数据库

```bash
# 使用 psql 执行初始化脚本
PGPASSWORD=strongpassword psql -h 10.18.85.10 -p 30684 -U appuser -d aichemol -f scripts/init-db.sql

# 或使用 Python
python -c "
import psycopg2
with open('scripts/init-db.sql', 'r') as f:
    sql = f.read()
conn = psycopg2.connect(
    host='10.18.85.10', port=30684,
    user='appuser', password='strongpassword',
    database='aichemol'
)
conn.autocommit = True
conn.cursor().execute(sql)
conn.close()
print('数据库初始化完成')
"
```

### 4. 启动服务

```bash
# 开发模式
uvicorn app.main:app --reload --host 0.0.0.0 --port 8333

# 生产模式
docker-compose up -d
```

### 5. 访问 API 文档

- Swagger UI: http://localhost:8333/docs
- ReDoc: http://localhost:8333/redoc

---

## 📡 API 概览

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

## 📦 MinIO 对象存储

平台使用 MinIO 作为文件存储后端，用于存储任务的输入/输出文件。

### 配置

```yaml
# config/config.yml
storage:
  minio:
    endpoint: 172.19.80.100:9090
    access_key: admin
    secret_key: minio_test_password_2025
    bucket: aidd-files
    secure: false
```

### 使用示例

```python
from app.storage import get_storage

storage = get_storage()

# 上传文件到 MinIO
storage.upload('tasks/input.csv', csv_data)
storage.upload_file('tasks/result.csv', '/local/path/result.csv')

# 下载文件
data = storage.download('tasks/input.csv')
storage.download_to_file('tasks/result.csv', '/local/path/result.csv')

# 获取预签名 URL（用于客户端直接访问）
url = storage.get_presigned_url('tasks/result.csv', expires_hours=24)

# 临时文件（本地磁盘，计算过程使用）
temp_path = storage.save_temp('temp.csv', data)
storage.download_to_temp('input.csv')  # 从 MinIO 下载到临时目录
storage.clean_temp()  # 清理临时文件
```

---

## 🔗 与 aidd-toolkit 集成

### Worker 生命周期

aidd-toolkit 中的 Worker 通过 HTTP 调用本平台进行注册/注销：

```
┌─────────────────────────────────────────────────────────────┐
│                    Worker 生命周期                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. 启动                                                     │
│     └──▶ POST /api/v1/workers/register                       │
│          返回 worker_id，状态 = online                        │
│          同时持久化到 PostgreSQL workers 表                   │
│                                                              │
│  2. 心跳 (每 10 秒)                                           │
│     └──▶ POST /api/v1/workers/{id}/heartbeat                 │
│          报告 CPU/内存使用、当前任务列表                       │
│          Platform 更新 last_heartbeat 时间戳                  │
│                                                              │
│  3. 运行中                                                   │
│     └──▶ 从 Redis 消费任务 (BRPOP aidd:queue:service:admet)  │
│     └──▶ 执行计算，更新任务状态到 Redis + PostgreSQL          │
│                                                              │
│  4. 停止 (Ctrl+C)                                            │
│     └──▶ DELETE /api/v1/workers/{worker_id}                  │
│          状态 = offline，数据库同步更新                        │
│                                                              │
│  5. 心跳超时 (Platform 自动检测)                              │
│     └──▶ 超过 30 秒无心跳，自动标记 Worker 为 offline         │
│     └──▶ 该 Worker 上的任务重新入队等待调度                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 心跳机制

Worker 与 Platform 之间通过心跳保持连接状态：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `worker.heartbeat_interval` | 10 秒 | Worker 发送心跳间隔 |
| `heartbeat_timeout` | 30 秒 | Platform 判定超时阈值 |
| `heartbeat_check_interval` | 10 秒 | Platform 检查心跳间隔 |

**心跳请求示例**：
```bash
POST /api/v1/workers/{worker_id}/heartbeat
Content-Type: application/json

{
  "used_cpu": 4,
  "used_memory_gb": 8.5,
  "used_gpu": 0,
  "used_gpu_memory_gb": 0,
  "current_tasks": ["task-uuid-1", "task-uuid-2"]
}
```

**超时处理流程**：
```
Platform HeartbeatChecker (后台任务)
    │
    ├──▶ 每 10 秒检查所有 Worker 的 last_heartbeat
    │
    ├──▶ 发现超时 Worker (now - last_heartbeat > 30s)
    │       │
    │       ├──▶ 内存中标记 status = offline
    │       │
    │       └──▶ 数据库同步更新 status = offline
    │
    └──▶ 重新调度该 Worker 上的任务
```

### 启动 Worker

```bash
# 在 aidd-toolkit 目录下
cd /path/to/aidd-toolkit
mamba activate aidd-toolkit-admet
python -m services.admet_predict.wrapper
```

启动后会看到：
```
INFO | Worker 注册成功: <worker-id> (hostname)
INFO | ADMET Worker 已初始化，服务: admet, worker_id: <worker-id>
INFO | Worker 已启动，服务: admet
```

### 提交任务示例

```bash
# 通过 curl 提交任务
curl -X POST "http://localhost:8333/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "admet",
    "task_type": "qikprop",
    "name": "ADMET预测",
    "input_params": {
      "smiles": ["CCO", "CC(=O)OC1=CC=CC=C1C(=O)O"]
    }
  }'
```

```python
# 通过 Python 提交任务
import requests

response = requests.post(
    "http://localhost:8333/api/v1/tasks",
    json={
        "service": "admet",
        "task_type": "qikprop",
        "name": "ADMET 预测",
        "input_params": {"smiles": ["CCO", "CC(=O)O"]}
    }
)
task = response.json()
print(f"Task ID: {task['id']}")
```
