# Docking 自动调度与结果展示

## TL;DR

在三个服务中实现 **"项目级受体文件上传 → aidd-platform 自动扫描缺失 docking 结果 → 提交 Glide docking 任务 → 平台后处理写入结果 → 前端展示 docking 分数与结果文件下载"** 的完整链路。

核心设计参照已有的 ADMET 自动同步模式（`AdmetSyncChecker`），新增：

- **backend-aichemol**：新建 `project_receptors`（项目受体文件表）和 `docking_result`（对接结果表）两张新表，新增受体文件上传 API 和 docking 结果查询 API
  - `docking_result` 表包含 `pose_sdf_minio_path`（单化合物 SDF 路径）和 `receptor_pdb_minio_path`（受体 PDB 路径）字段，用于前端 3D Pose 查看
  - 新增 `GET /{id}/pose-sdf` 和 `GET /{id}/receptor-pdb` 端点返回 presigned URL
- **aidd-platform**：
  - `DockingSyncChecker` — 自动扫描缺失结果，预插入 pending 记录，提交 Glide 任务
  - `DockingResultProcessor` — 扫描已完成/失败的任务，从 MinIO 下载打分 CSV 和 manifest.json，解析并写入 `docking_result` 表（含 per-compound SDF/PDB 路径）
- **v4Aichemtool**：新增项目受体文件管理 UI 和 docking 结果表格列（含文件下载）

### 职责分离原则

| 角色 | 职责 |
|------|------|
| **Worker**（aidd-toolkit） | 计算 + 生成结果文件 + 上传到 MinIO |
| **Platform**（aidd-platform） | 任务调度 + 结果解析 + 业务表写入 |

---

## 用户操作流程

### ① 上传受体文件

用户在前端（`v4Aichemtool`）选择项目 → 上传受体 Grid 文件（`.zip`）

### ② 文件存储 — `backend-aichemol`

```
POST /receptors/upload
```

- 文件存入 MinIO：`receptors/{project_id}/{filename}`
- 记录存入 `project_receptors` 表

---

## 自动调度（定时）

### ③ DockingSyncChecker 定时扫描 — `aidd-platform`

> 默认每小时执行一次

1. 查询 `project_receptors` 表 → 获取 `is_active=true` 的受体文件
2. 对每个受体：
   - `LEFT JOIN docking_result` 找出缺失 docking 结果的化合物 SMILES
   - 排除已有 `pending` / `computing` / `success` 状态的记录
3. 全部有结果 → **跳过**
4. 有缺失 →
   - **a.** 预插入 `pending` 记录到 `docking_result` 表（`ON CONFLICT DO NOTHING`，防止重复）
   - **b.** 将 SMILES 列表生成 CSV 并上传到 MinIO
   - **c.** 按批次提交 Task：

```json
{
  "service": "docking",
  "task_type": "glide",
  "input_params": {
    "input_file": "minio://tasks/input/docking_sync/{date}/{task_id}_input.csv",
    "grid_file": "minio://receptors/{project_id}/grid.zip",
    "receptor_id": "xxx",
    "project_id": "xxx",
    "source": "docking_sync",
    "batch_index": 0
  },
  "input_files": ["<input_file>", "<grid_file>"]
}
```

   - **d.** 更新 `pending` 记录的 `task_id`

### ④ Redis 队列

任务进入 `aidd:queue:service:docking`，等待 Worker 消费。

### ⑤ Docking Worker — `aidd-toolkit`

Worker 从队列取出任务后执行：

1. 从 MinIO 下载 Grid 文件和输入 CSV
2. CSV → LigPrep 3D 构象生成（`.mae`）
3. 运行 Glide SP/XP docking
4. 后处理：`glide_merge`（Top-1 pose）+ PSE + Excel + Pocket
5. **上传结果文件到 MinIO**：

| 文件 | 说明 |
|------|------|
| `{task_id}_dock.csv` | 打分 CSV，含 `PK_{target}` 列 |
| `{task_id}_pv.maegz` | 全 pose 结构 |
| `{task_id}_pv_1p.maegz` | Top-1 pose |
| `{task_id}/poses/` | **Pose 拆分目录**（见下方说明） |
| `{task_id}.pse` | PyMOL 可视化 |
| `{task_id}_dock.xlsx` | Excel 报告 |
| `{task_id}_pockets.zip` | 口袋 PDB |

**Pose 拆分目录结构**：

```
poses/
├── receptor.pdb          # 受体蛋白（maegz 第 1 个结构）
├── cpd1_pose.sdf         # 化合物 1 的 SDF
├── cpd2_pose.sdf         # 化合物 2 的 SDF
└── manifest.json         # title → 文件名映射
```

`manifest.json` 路径会加入 `output_files` 列表，供 Platform 后处理解析。

6. 将 `output_files` 列表 + `result` 摘要写入 Redis task hash
7. 报告任务完成（`status="success"`）

> **⚠️ Worker 不直接写入 `docking_result` 数据库表**

### ⑥ DockingResultProcessor 后处理 — `aidd-platform`

> 默认每 60 秒扫描一次

- 扫描 Redis `completed` / `failed` 队列中的 docking 任务
- 通过 `aidd:docking:processed` 集合跳过已处理任务

**已完成的任务：**

1. 从 `output_files` 找到 `_dock.csv` 路径
2. 从 MinIO 下载 CSV，解析 SMILES → 打分映射
3. 从 `output_files` 找到 `manifest.json` 路径，下载并解析 title → SDF 文件名映射
4. 通过 CSV 的 `title` 列将 SMILES 关联到对应的 SDF 文件路径
5. `UPDATE docking_result`：`pending` → `success`，写入 `docking_score`、`glide_gscore` 等字段
6. 同时写入 `pose_sdf_minio_path`（单化合物 SDF 路径）和 `receptor_pdb_minio_path`（受体 PDB 路径）
7. 不在 CSV 中的分子标记为 `failed`

**已失败的任务：**

1. `UPDATE docking_result`：`pending` → `failed`，写入 `error_message`

---

## 结果查询

### ⑦ 前端展示 — `v4Aichemtool`

前端加载化合物列表时：

```
GET /docking-results/project/{project_id}
```

→ 批量获取该项目每个化合物的 docking scores

查看单化合物 3D Pose：

```
GET /docking-results/{id}/pose-sdf      → 返回配体 SDF presigned URL
GET /docking-results/{id}/receptor-pdb  → 返回受体 PDB presigned URL
```

前端可用 3D 分子查看器（如 3Dmol.js / Mol*）加载 SDF + PDB 文件渲染单化合物对接构象。

化合物表格中展示：

| 化合物名 | SMILES | Receptor-A (score) | Receptor-B |
|----------|--------|-------------------|------------|
| CPD-001 | `CC(=O)` | -8.32 kcal/mol ⬇ | -7.15 ⬇ |
| CPD-002 | `c1ccc` | ⏳ 计算中... | -6.88 ⬇ |

- ⬇ = 下载 `.maegz` 结果文件
- ⏳ = `pending` / `computing` 状态
- 🔬 = 查看 3D Pose（通过 `pose-sdf` + `receptor-pdb` 端点加载）

---

## 流程总览图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           用户操作流程                                    │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ① 用户在前端选择项目 → 上传受体 Grid 文件(.zip)                          │
│     [v4Aichemtool]                                                       │
│         │                                                                │
│         ▼                                                                │
│  ② POST /receptors/upload                                                │
│     [backend-aichemol]                                                   │
│         │                                                                │
│         ├──► 文件存入 MinIO (路径: receptors/{project_id}/{filename})      │
│         └──► 记录存入 project_receptors 表                                │
│                                                                          │
│ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ 自动调度(定时) ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
│                                                                          │
│  ③ DockingSyncChecker 定时扫描 (默认每小时)                               │
│     [aidd-platform]                                                      │
│         │                                                                │
│         ▼                                                                │
│     查询 project_receptors 表 → 获取 is_active=true 的受体文件            │
│         │                                                                │
│         ▼                                                                │
│     对每个受体:                                                           │
│       LEFT JOIN docking_result 找出缺失 docking 结果的化合物 SMILES       │
│       (排除已有 pending/computing/success 状态的记录)                      │
│         │                                                                │
│         ├── 全部有结果 → 跳过                                             │
│         │                                                                │
│         └── 有缺失 →                                                      │
│              a. 预插入 pending 记录到 docking_result 表                    │
│                 (ON CONFLICT DO NOTHING，防止重复)                         │
│              b. 将 SMILES 列表生成 CSV 并上传到 MinIO                      │
│              c. 按批次提交 Task                                            │
│                 service="docking", task_type="glide"                     │
│                 input_params = {                                         │
│                   "input_file": "minio://tasks/input/...csv",           │
│                   "grid_file": "minio://receptors/.../grid.zip",        │
│                   "receptor_id": "xxx",                                  │
│                   "project_id": "xxx",                                   │
│                   "source": "docking_sync",                              │
│                   "batch_index": 0                                       │
│                 }                                                        │
│              d. 更新 pending 记录的 task_id                               │
│                   │                                                      │
│                   ▼                                                      │
│            ④ Redis 队列: aidd:queue:service:docking                      │
│                   │                                                      │
│                   ▼                                                      │
│            ⑤ Docking Worker (aidd-toolkit)                               │
│               - 从 MinIO 下载 Grid 文件和输入 CSV                         │
│               - CSV → LigPrep 3D 构象生成 (.mae)                         │
│               - 运行 Glide SP/XP docking                                 │
│               - 后处理: glide_merge(Top-1) + PSE + Excel + Pocket        │
│               - 上传结果文件到 MinIO:                                     │
│                   · _dock.csv (打分 CSV，含 PK_{target} 列)              │
│                   · _pv.maegz / _pv_1p.maegz (结构文件)                  │
│                   · poses/ (receptor.pdb + {title}_pose.sdf + manifest)  │
│                   · .pse / _dock.xlsx / _pockets.zip                     │
│               - output_files + result 摘要写入 Redis task hash            │
│               - 报告任务完成 (status="success")                           │
│               ★ Worker 不直接写入 docking_result 数据库表                 │
│                   │                                                      │
│                   ▼                                                      │
│            ⑥ DockingResultProcessor 后处理 (默认每 60 秒扫描)             │
│               [aidd-platform]                                            │
│               - 扫描 Redis completed/failed 队列中的 docking 任务         │
│               - 通过 aidd:docking:processed 集合跳过已处理任务             │
│               - 已完成的任务:                                             │
│                   · 从 output_files 找到 _dock.csv                       │
│                   · MinIO 下载 CSV → 解析 SMILES → 打分映射              │
│                   · 从 output_files 找到 manifest.json                  │
│                   · 解析 manifest → 关联 smiles → SDF 文件路径          │
│                   · UPDATE docking_result: pending → success              │
│                   · 写入 pose_sdf_minio_path + receptor_pdb_minio_path   │
│                   · 不在 CSV 中的分子标记为 failed                        │
│               - 已失败的任务:                                             │
│                   · UPDATE docking_result: pending → failed               │
│                                                                          │
│ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ 结果查询 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│                                                                          │
│  ⑦ 前端加载化合物列表时                                                   │
│     [v4Aichemtool]                                                       │
│         │                                                                │
│         ▼                                                                │
│     GET /docking-results/project/{project_id}                            │
│     → 批量获取该项目每个化合物的 docking scores                            │
│                                                                          │
│     GET /docking-results/{id}/pose-sdf     → 配体 SDF presigned URL      │
│     GET /docking-results/{id}/receptor-pdb → 受体 PDB presigned URL      │
│         │                                                                │
│         ▼                                                                │
│     化合物表格中展示:                                                     │
│     ┌──────────┬─────────┬──────────────────────┬──────────────┐         │
│     │ 化合物名  │ SMILES  │ Receptor-A (score)   │ Receptor-B   │        │
│     ├──────────┼─────────┼──────────────────────┼──────────────┤         │
│     │ CPD-001  │ CC(=O)  │ -8.32 kcal/mol  [⬇]  │ -7.15 [⬇]   │        │
│     │ CPD-002  │ c1ccc   │ ⏳ 计算中...          │ -6.88 [⬇]   │        │
│     └──────────┴─────────┴──────────────────────┴──────────────┘         │
│                              [⬇] = 下载 .maegz 结果文件                   │
│                              ⏳ = pending/computing 状态                   │
│                              [🔬] = 查看 3D Pose (SDF + PDB)                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```