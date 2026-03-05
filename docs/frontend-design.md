# AIDD Platform 前端管理界面设计方案

## 1. 项目背景与目标

### 1.1 背景

AIDD Platform 是一个 AI 药物发现任务调度平台，后端基于 FastAPI 构建，负责管理分布式计算任务（ADMET 预测、分子对接等）的调度、Worker 节点的注册和监控、以及 Redis/PostgreSQL 的任务持久化。

目前所有操作依赖 API 调用或直接查数据库，缺少可视化管理界面，不便于管理人员查看系统状态、排查问题和操作任务。

### 1.2 目标

构建一个**轻量级单页管理前端**，供运维和管理人员使用，核心能力：

- 实时查看平台运行状态（集群健康度、资源使用率）
- 管理和监控计算任务的全生命周期
- 管理和监控 Worker 节点状态
- 查看队列统计，辅助容量规划

### 1.3 技术选型

| 层面       | 方案                    | 版本     | 说明                                                    |
| ---------- | ----------------------- | -------- | ------------------------------------------------------- |
| 框架       | Vue 3 + TypeScript      | 3.5 / TS 5.9 | Composition API + `<script setup>` 语法，类型安全   |
| UI 组件库  | PrimeVue                | 4.5      | 企业级组件库，DataTable/Chart/Dialog 开箱即用            |
| 状态管理   | Pinia                   | 3.0      | Vue 官方推荐，DevTools 集成，TS 类型推导良好             |
| 路由       | Vue Router              | 4.6      | 官方路由，支持动态路由和导航守卫                         |
| 构建工具   | Vite                    | 7.2      | 极速 HMR，原生 ESM 开发体验                              |
| HTTP 客户端| Axios                   | 1.13     | 拦截器、请求取消、错误处理统一                           |
| 工具库     | VueUse                  | 14.1     | `useIntervalFn`(轮询)、`useDateFormat`、`useTitle` 等    |
| 日期处理   | date-fns                | 4.1      | Tree-shakable，用于时间格式化和相对时间计算               |
| 图表       | PrimeVue Charts (Chart.js) | —     | PrimeVue 内置 Chart 组件，无需额外引入图表库             |
| 包管理器   | pnpm                    | —        | 磁盘空间高效，严格依赖管理                               |
| 部署       | Nginx 静态托管 或 FastAPI 挂载 | — | 简单项目可直接由后端 serve 静态文件，减少运维复杂度      |

---

## 2. 已有后端 API 分析

前端基于现有 REST API (`/api/v1`) 进行数据获取和操作。

### 2.1 API 接口清单

#### 健康检查

| 方法 | 路径                       | 说明           | 响应核心字段                                                                  |
| ---- | -------------------------- | -------------- | ----------------------------------------------------------------------------- |
| GET  | `/api/v1/health`           | 基础健康检查   | `status`, `timestamp`, `version`                                              |
| GET  | `/api/v1/health/detailed`  | 详细健康检查   | `scheduler_running`, `running_tasks`, `pending_tasks`, `online_workers`        |

#### 任务管理

| 方法   | 路径                       | 说明             | 请求/响应核心字段                                                             |
| ------ | -------------------------- | ---------------- | ----------------------------------------------------------------------------- |
| POST   | `/api/v1/tasks`            | 创建单个任务     | 请求: `service`, `task_type`, `priority`, `input_params`, `input_files` 等     |
| POST   | `/api/v1/tasks/batch`      | 批量创建任务     | 请求: `tasks[]` 任务数组                                                      |
| GET    | `/api/v1/tasks`            | 获取任务列表     | 查询参数: `status`, `service`, `limit`                                        |
| GET    | `/api/v1/tasks/stats`      | 队列统计         | `total_submitted`, `pending`, `running`, `completed`, `failed`, `cancelled`    |
| GET    | `/api/v1/tasks/{task_id}`  | 获取任务详情     | 完整任务信息                                                                  |
| DELETE | `/api/v1/tasks/{task_id}`  | 取消任务         | 204 No Content                                                                |

#### Worker 管理

| 方法   | 路径                                  | 说明           | 关键字段                                                                      |
| ------ | ------------------------------------- | -------------- | ----------------------------------------------------------------------------- |
| POST   | `/api/v1/workers/register`            | 注册 Worker    | `hostname`, `total_cpu`, `total_memory_gb`, `supported_services` 等           |
| POST   | `/api/v1/workers/{id}/heartbeat`      | Worker 心跳    | `used_cpu`, `used_memory_gb`, `current_tasks` 等                              |
| GET    | `/api/v1/workers`                     | Worker 列表    | 查询参数: `status`, `service`                                                 |
| GET    | `/api/v1/workers/stats`               | 集群统计       | `total_workers`, `online_workers`, `total/used/available` CPU/内存/GPU         |
| GET    | `/api/v1/workers/{id}`                | Worker 详情    | 完整 Worker 信息                                                              |
| DELETE | `/api/v1/workers/{id}`                | 注销 Worker    | 204 No Content                                                                |

### 2.2 数据模型摘要

**任务状态流转 (`TaskStatus`)**：

```
PENDING → QUEUED → RUNNING → SUCCESS
                           → FAILED  (可重试 → PENDING)
                           → TIMEOUT
              → CANCELLED
```

**任务优先级 (`TaskPriority`)**：

| 值 | 名称     | 说明               |
| -- | -------- | ------------------ |
| 0  | CRITICAL | 紧急任务           |
| 1  | HIGH     | 高优先级           |
| 2  | NORMAL   | 正常（默认）       |
| 3  | LOW      | 低优先级           |
| 4  | BATCH    | 批量/自动同步任务  |

**Worker 状态 (`WorkerStatus`)**：

| 状态     | 说明           |
| -------- | -------------- |
| ONLINE   | 在线，可接任务 |
| BUSY     | 忙碌，资源已满 |
| OFFLINE  | 离线           |
| DRAINING | 排空中         |

**服务类型**：`admet`（ADMET 预测）、`docking`（分子对接）、`md`（分子动力学）、`qsar`（定量构效关系）

---

## 3. 页面规划

### 3.1 整体布局

```
┌─────────────────────────────────────────────────────────┐
│  AIDD Platform          Dashboard | Tasks | Workers     │  ← 顶部导航栏
├─────────┬───────────────────────────────────────────────┤
│         │                                               │
│  侧边栏  │              主内容区域                        │
│ (可选)   │                                               │
│         │                                               │
├─────────┴───────────────────────────────────────────────┤
│  © 2025 AIDD Platform v0.1.0                            │  ← 底部状态栏
└─────────────────────────────────────────────────────────┘
```

### 3.2 页面路由

| 路由              | 页面名称       | 说明                   |
| ----------------- | -------------- | ---------------------- |
| `/`               | 仪表盘         | 系统总览               |
| `/tasks`          | 任务列表       | 任务搜索、筛选、操作   |
| `/tasks/:id`      | 任务详情       | 单个任务完整信息       |
| `/workers`        | Worker 列表    | 节点状态与资源查看     |
| `/workers/:id`    | Worker 详情    | 单个节点详细信息       |

---

## 4. 各页面功能详细设计

### 4.1 仪表盘 (Dashboard)

**目的**：一屏掌握平台全局状态，是管理人员打开页面后的第一视角。

#### 4.1.1 平台状态卡片（顶部一行）

```
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  🟢 平台状态  │ │  在线 Worker  │ │  运行中任务   │ │  待处理任务   │
│   healthy    │ │      3       │ │      12      │ │      45      │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

- 数据来源：`GET /api/v1/health/detailed`
- 刷新频率：每 10 秒自动轮询

#### 4.1.2 集群资源概览（中部）

```
┌─────────────────────────────────────────────────────────┐
│  集群资源                                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐       │
│  │ CPU          │ │ 内存         │ │ GPU          │       │
│  │ [进度条 60%] │ │ [进度条 45%] │ │ [进度条 75%] │       │
│  │ 24/40 cores  │ │ 96/210 GB   │ │ 3/4 cards    │       │
│  └─────────────┘ └─────────────┘ └─────────────┘       │
└─────────────────────────────────────────────────────────┘
```

- 数据来源：`GET /api/v1/workers/stats`
- 展示：环形进度图或条形进度条，同时显示已用/总量数值

#### 4.1.3 任务状态分布（中部左）

```
┌───────────────────────────┐
│  任务队列统计               │
│                           │
│   ┌─────────────────┐     │
│   │   环形图/饼图     │     │
│   │  pending: 45     │     │
│   │  running: 12     │     │
│   │  completed: 230  │     │
│   │  failed: 8       │     │
│   └─────────────────┘     │
│  总提交: 295              │
└───────────────────────────┘
```

- 数据来源：`GET /api/v1/tasks/stats`

#### 4.1.4 Worker 状态列表（中部右）

快速展示每个 Worker 的状态和利用率缩略信息：

```
┌───────────────────────────────────┐
│  Worker 节点状态                    │
│  ┌──────────────────────────────┐ │
│  │ 🟢 worker-001  CPU 60%  2任务│ │
│  │ 🟡 worker-002  CPU 95%  4任务│ │
│  │ 🔴 worker-003  离线          │ │
│  └──────────────────────────────┘ │
└───────────────────────────────────┘
```

- 数据来源：`GET /api/v1/workers`
- 点击跳转到 Worker 详情页

---

### 4.2 任务管理页 (Tasks)

**目的**：查看所有任务、筛选、搜索、执行操作。

#### 4.2.1 筛选栏

```
┌─────────────────────────────────────────────────────────┐
│  状态: [全部 ▾]  服务: [全部 ▾]  优先级: [全部 ▾]  [搜索🔍]│
│                                               [刷新] [创建任务] │
└─────────────────────────────────────────────────────────┘
```

- 状态筛选：全部 / pending / queued / running / success / failed / cancelled / timeout
- 服务类型筛选：全部 / admet / docking / md / qsar
- 优先级筛选：全部 / critical / high / normal / low / batch
- 搜索：按任务 ID 或名称  

#### 4.2.2 任务表格

| 任务 ID (简短) | 名称           | 服务    | 状态         | 优先级  | Worker         | 创建时间            | 耗时    | 操作     |
| -------------- | -------------- | ------- | ------------ | ------- | -------------- | ------------------- | ------- | -------- |
| a1b2c3d4       | ADMET预测-批次1| admet   | 🟢 success   | normal  | worker-001     | 2025-03-05 10:30    | 45s     | 详情     |
| e5f6g7h8       | Docking-受体A  | docking | 🔵 running   | high    | worker-002     | 2025-03-05 10:35    | -       | 详情 取消|
| i9j0k1l2       | ADMET预测-批次2| admet   | 🟡 pending   | batch   | -              | 2025-03-05 10:40    | -       | 详情 取消|
| m3n4o5p6       | Docking-受体B  | docking | 🔴 failed    | normal  | worker-001     | 2025-03-05 10:20    | 120s    | 详情 重试|

- 状态列使用颜色标签：
  - 🟡 pending/queued → 黄色
  - 🔵 running → 蓝色
  - 🟢 success → 绿色
  - 🔴 failed/timeout → 红色
  - ⚫ cancelled → 灰色
- 操作列：
  - **详情**：跳转任务详情页
  - **取消**：调用 `DELETE /api/v1/tasks/{task_id}`（pending/running 状态可用）
- 分页：默认每页 20 条
- 自动刷新：每 15 秒轮询列表（可手动关闭）

#### 4.2.3 创建任务对话框

点击「创建任务」按钮弹出 Modal 表单：

```
┌─────────────────────────────────────┐
│  创建计算任务                        │
│                                     │
│  服务类型*:  [admet    ▾]            │
│  任务类型:   [qikprop  ▾]            │
│  任务名称:   [__________________]    │
│  优先级:     [normal   ▾]            │
│                                     │
│  ── 输入参数 ──                      │
│  SMILES:     [JSON 编辑器]           │
│  输入文件:   [文件路径列表]           │
│                                     │
│  ── 资源需求 ──                      │
│  CPU 核心:   [2    ]                 │
│  内存 (GB):  [4.0  ]                 │
│  GPU 数量:   [0    ]                 │
│                                     │
│  超时 (秒):  [3600 ]                 │
│  最大重试:   [3    ]                 │
│                                     │
│            [取消]  [提交]            │
└─────────────────────────────────────┘
```

- 调用 `POST /api/v1/tasks` 提交
- 表单验证：service 必填，CPU >= 1，内存 >= 0.1

---

### 4.3 任务详情页 (Task Detail)

**目的**：展示单个任务的完整信息，便于排查问题。

#### 布局

```
┌─────────────────────────────────────────────────────────┐
│  ← 返回任务列表          任务: a1b2c3d4-xxxx-xxxx       │
│                                                         │
│  ┌─ 基本信息 ────────────────────────────────────────┐  │
│  │ ID:       a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx    │  │
│  │ 名称:     ADMET预测-批次1                          │  │
│  │ 服务:     admet                                   │  │
│  │ 任务类型:  qikprop                                 │  │
│  │ 状态:     🟢 success                              │  │
│  │ 优先级:   normal (2)                              │  │
│  │ Job ID:   batch-20250305-001                      │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 时间线 ──────────────────────────────────────────┐  │
│  │ 创建时间:   2025-03-05 10:30:00                   │  │
│  │ 调度时间:   2025-03-05 10:30:02                   │  │
│  │ 开始执行:   2025-03-05 10:30:05                   │  │
│  │ 完成时间:   2025-03-05 10:30:50                   │  │
│  │ 总耗时:     45 秒                                 │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 执行信息 ────────────────────────────────────────┐  │
│  │ Worker:    worker-001 (192.168.1.100)             │  │
│  │ 重试次数:   0 / 3                                 │  │
│  │ 超时设置:   3600 秒                               │  │
│  │ 资源需求:   CPU 2核, 内存 4GB, GPU 0              │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 输入参数 (JSON) ─────────────────────────────────┐  │
│  │ {                                                 │  │
│  │   "smiles": ["CCO", "CC(=O)OC1=CC=CC=C1C(=O)O"]  │  │
│  │ }                                                 │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 执行结果 (JSON) ─────────────────────────────────┐  │
│  │ { ... }                                           │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 错误信息 (仅 failed/timeout 显示) ────────────────┐  │
│  │ RuntimeError: QikProp process exited with code 1  │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  [取消任务]                                             │
└─────────────────────────────────────────────────────────┘
```

- 数据来源：`GET /api/v1/tasks/{task_id}`
- Worker ID 可链接跳转到对应 Worker 详情

---

### 4.4 Worker 列表页 (Workers)

**目的**：展示所有计算节点的状态、资源情况。

#### 4.4.1 集群摘要卡片

```
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  总节点数     │ │  在线节点     │ │  总 CPU      │ │  总 GPU      │
│      5       │ │      3       │ │    40 核      │ │    4 卡      │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

- 数据来源：`GET /api/v1/workers/stats`

#### 4.4.2 Worker 表格

| Worker ID (简短) | 主机名       | IP             | 状态       | CPU 使用          | 内存使用          | GPU 使用        | 当前任务 | 支持服务          | 最后心跳            | 操作        |
| ---------------- | ------------ | -------------- | ---------- | ----------------- | ----------------- | --------------- | -------- | ----------------- | ------------------- | ----------- |
| w001             | worker-001   | 192.168.1.100  | 🟢 online  | 8/16 (50%)        | 24/64 GB (37%)    | 1/2 (50%)       | 2        | admet, docking    | 5s ago              | 详情 注销   |
| w002             | worker-002   | 192.168.1.101  | 🟡 busy    | 15/16 (94%)       | 60/64 GB (94%)    | 2/2 (100%)      | 4        | docking           | 3s ago              | 详情 注销   |
| w003             | worker-003   | 192.168.1.102  | 🔴 offline | -                 | -                 | -               | 0        | admet             | 5min ago            | 详情        |

- 状态颜色：online → 绿色，busy → 黄色，offline → 红色，draining → 橙色
- 资源列使用 mini 进度条 + 数值
- 操作列：
  - **详情**：跳转 Worker 详情页
  - **注销**：调用 `DELETE /api/v1/workers/{id}`（需确认弹窗）

---

### 4.5 Worker 详情页 (Worker Detail)

```
┌─────────────────────────────────────────────────────────┐
│  ← 返回 Worker 列表       Worker: worker-001            │
│                                                         │
│  ┌─ 基本信息 ────────────────────────────────────────┐  │
│  │ ID:          w001-xxxx-xxxx                       │  │
│  │ 主机名:      worker-001                           │  │
│  │ IP:          192.168.1.100:8080                   │  │
│  │ 状态:        🟢 online                            │  │
│  │ 注册时间:    2025-03-01 08:00:00                  │  │
│  │ 最后心跳:    2025-03-05 10:30:45 (5s ago)         │  │
│  │ 支持服务:    admet, docking                       │  │
│  │ 最大并发:    8                                    │  │
│  │ 标签:        zone=gpu-cluster                     │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 资源使用 ────────────────────────────────────────┐  │
│  │                                                   │  │
│  │  CPU:  ████████░░░░░░░░  8/16 核 (50%)            │  │
│  │  内存: ██████░░░░░░░░░░  24/64 GB (37%)           │  │
│  │  GPU:  ████████░░░░░░░░  1/2 卡 (50%)             │  │
│  │  GPU显存: ██████████░░░░  12/24 GB (50%)           │  │
│  │                                                   │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─ 当前运行任务 ────────────────────────────────────┐  │
│  │  任务ID           服务      状态     开始时间      │  │
│  │  a1b2c3d4...     admet    running  10:30:05      │  │
│  │  e5f6g7h8...     docking  running  10:35:12      │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  [注销 Worker]                                          │
└─────────────────────────────────────────────────────────┘
```

- 数据来源：`GET /api/v1/workers/{id}`
- 当前任务列表中的任务 ID 可链接跳转到任务详情

---

## 5. 需要新增/补充的后端 API

当前 API 基本可满足管理界面需求，但建议补充以下接口以提升前端体验：

### 5.1 建议新增接口

| 优先级 | 方法  | 路径                          | 说明                                     | 原因                                                        |
| ------ | ----- | ----------------------------- | ---------------------------------------- | ----------------------------------------------------------- |
| P0     | GET   | `/api/v1/tasks` 增加 DB 查询  | 从 PostgreSQL 查询历史任务               | 当前 list_tasks 只查 Redis 运行中任务，无法查看已完成/历史任务 |
| P0     | GET   | `/api/v1/tasks` 增加分页参数   | 支持 `offset`, `limit`, `sort` 参数      | 任务量大时需要分页                                           |
| P1     | GET   | `/api/v1/tasks/stats` 增加时间维度 | 按时间段统计任务完成/失败趋势        | 仪表盘图表需要历史趋势数据                                   |
| P1     | POST  | `/api/v1/tasks/{id}/retry`    | 手动重试失败任务                         | 管理员常见操作，目前无此入口                                 |
| P2     | PATCH | `/api/v1/workers/{id}/drain`  | 标记 Worker 进入 draining 状态           | 优雅下线场景，停止分配新任务但允许当前任务完成               |
| P2     | GET   | `/api/v1/scheduler/status`    | 获取后台任务状态（同步器、结果处理器等） | 管理员需要知道自动同步是否正常运行                           |

### 5.2 现有接口调整建议

1. **`GET /api/v1/tasks`**：增加从 PostgreSQL 查询的 fallback 逻辑，支持查看历史已完成任务
   ```
   GET /api/v1/tasks?status=failed&service=docking&offset=0&limit=20&sort=-created_at
   ```

2. **`GET /api/v1/tasks/stats`**：增加按服务类型分组统计
   ```json
   {
     "total_submitted": 295,
     "pending": 45,
     "running": 12,
     "completed": 230,
     "failed": 8,
     "by_service": {
       "admet": {"pending": 30, "running": 5, "completed": 180, "failed": 3},
       "docking": {"pending": 15, "running": 7, "completed": 50, "failed": 5}
     }
   }
   ```

---

## 6. 数据交互与轮询策略

### 6.1 轮询机制

| 页面/组件         | 接口                            | 轮询间隔  | 说明                         |
| ----------------- | ------------------------------- | --------- | ---------------------------- |
| 仪表盘状态卡片    | `GET /health/detailed`          | 10s       | 平台在线状态感知             |
| 仪表盘资源概览    | `GET /workers/stats`            | 15s       | 集群资源变化不频繁           |
| 仪表盘任务统计    | `GET /tasks/stats`              | 15s       | 队列数据                     |
| 仪表盘 Worker 状态| `GET /workers`                  | 15s       | Worker 概览                  |
| 任务列表          | `GET /tasks`                    | 15s       | 可手动禁用自动刷新           |
| 任务详情          | `GET /tasks/{id}`               | 5s        | 运行中任务需要较快刷新       |
| Worker 列表       | `GET /workers`                  | 15s       |                              |
| Worker 详情       | `GET /workers/{id}`             | 10s       |                              |

### 6.2 错误处理

- API 请求失败时显示 toast 通知，不阻断页面
- 连续 3 次请求失败后暂停轮询，显示 "连接断开" 状态条，提供手动重连按钮
- 网络恢复后自动恢复轮询

---

## 7. 项目结构建议

```
aidd-platform/
├── frontend/                       # 前端项目目录
│   ├── package.json
│   ├── pnpm-lock.yaml
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── tsconfig.app.json
│   ├── env.d.ts
│   ├── index.html
│   ├── public/
│   │   └── favicon.ico
│   └── src/
│       ├── main.ts                 # 入口：创建 app，注册 PrimeVue/Pinia/Router
│       ├── App.vue                 # 根组件（RouterView + Layout）
│       ├── api/                    # API 调用层
│       │   ├── client.ts           # Axios 实例：baseURL、拦截器、错误处理
│       │   ├── tasks.ts            # 任务相关 API 函数
│       │   ├── workers.ts          # Worker 相关 API 函数
│       │   └── health.ts           # 健康检查 API 函数
│       ├── types/                  # TypeScript 类型定义
│       │   ├── task.ts             # TaskStatus, TaskPriority, Task, TaskResponse
│       │   └── worker.ts           # WorkerStatus, Worker, ClusterStats
│       ├── stores/                 # Pinia stores
│       │   ├── tasks.ts            # 任务列表/统计 store
│       │   └── workers.ts          # Worker 列表/集群统计 store
│       ├── views/                  # 页面组件
│       │   ├── Dashboard.vue       # 仪表盘
│       │   ├── TaskList.vue        # 任务列表
│       │   ├── TaskDetail.vue      # 任务详情
│       │   ├── WorkerList.vue      # Worker 列表
│       │   └── WorkerDetail.vue    # Worker 详情
│       ├── components/             # 通用组件
│       │   ├── AppLayout.vue       # 布局框架（Menubar + 主内容区）
│       │   ├── StatusTag.vue       # 状态标签（Tag 组件封装）
│       │   ├── ResourceBar.vue     # 资源进度条（ProgressBar 封装）
│       │   └── StatsCard.vue       # 统计卡片
│       ├── composables/            # 组合式函数
│       │   ├── usePolling.ts       # 基于 VueUse useIntervalFn 的轮询
│       │   └── useTaskStats.ts     # 任务统计数据组合
│       └── router/
│           └── index.ts            # Vue Router 路由配置
├── app/                            # 后端（现有）
├── config/
└── ...
```

---

## 8. 实施计划

### Phase 1：核心功能（MVP）

**目标**：能看、能查、能操作

- [ ] 搭建前端项目脚手架（Vite + Vue 3 + PrimeVue + Pinia + Vue Router）
- [ ] 实现 API 调用层（Axios 封装 + TypeScript 类型）+ Pinia stores
- [ ] 仪表盘页面（状态卡片 + 资源进度条 + 任务统计饼图 + Worker 概览）
- [ ] 任务列表页面（表格 + 筛选 + 取消操作）
- [ ] 任务详情页面（完整信息展示）
- [ ] Worker 列表页面（表格 + 资源展示 + 注销操作）
- [ ] Worker 详情页面
- [ ] 自动轮询机制
- [ ] 后端：补充 PostgreSQL 查询历史任务的 API

### Phase 2：增强功能

- [ ] 创建任务表单
- [ ] 手动重试失败任务
- [ ] Worker drain 操作
- [ ] 按服务类型分组的统计图表
- [ ] 后台调度器/同步器状态展示
- [ ] 任务列表导出 (CSV)

### Phase 3：进阶优化

- [ ] WebSocket 实时推送替代轮询
- [ ] 任务执行日志查看
- [ ] 资源使用历史趋势图（需后端采集时序数据）
- [ ] 用户权限（查看/操作分离）
- [ ] 暗色主题

---

## 9. 开发与部署

### 9.1 本地开发

```bash
cd frontend
pnpm install
pnpm dev             # 启动开发服务器，默认 http://localhost:5173
```

Vite 开发代理配置（`vite.config.ts`）：
```typescript
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8333',  // 后端地址
        changeOrigin: true
      }
    }
  }
})
```

### 9.2 生产部署

**方案 A：FastAPI 直接服务静态文件（最简单）**

```python
# app/main.py 中添加
from fastapi.staticfiles import StaticFiles

app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="frontend")
```

构建后直接由后端进程服务 SPA，无需额外部署 Nginx。

**方案 B：Nginx 独立部署**

```nginx
server {
    listen 80;
    server_name platform.aidd.local;

    # 前端静态文件
    location / {
        root /opt/aidd-platform/frontend/dist;
        try_files $uri $uri/ /index.html;
    }

    # API 代理
    location /api/ {
        proxy_pass http://127.0.0.1:8333;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

---

## 10. 总结

| 维度       | 说明                                                      |
| ---------- | --------------------------------------------------------- |
| 页面数量   | 5 个页面（仪表盘 + 任务列表/详情 + Worker 列表/详情）      |
| 核心操作   | 查看任务/Worker 状态、取消任务、注销 Worker、创建任务       |
| 数据实时性 | 5~15 秒轮询，后续可升级 WebSocket                          |
| 后端改动   | MVP 阶段需补充 PostgreSQL 历史任务查询 API，其余现有 API 足够 |
| 技术栈     | Vue 3 + PrimeVue + Pinia + Vue Router + Vite + Axios + VueUse |
| 开发量估算 | MVP 约 5 个页面 + API 层 + Pinia stores + 通用组件          |
