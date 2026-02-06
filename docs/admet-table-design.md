# ADMET 结果表设计方案

> 目标：将 QikProp 计算结果结构化存入 PostgreSQL，供第三方业务系统通过 SQL 直接查询展示。

---

## 背景

### 当前数据流

```
提交任务 → Redis → Worker 执行 QikProp → 结果 CSV 上传 MinIO
                                        → tasks.result 仅存 {'status': 'completed'}
```

**问题**：第三方系统要展示 ADMET 属性，只能从 MinIO 下载 CSV 再解析，无法直接 SQL 查询。

### 目标数据流

```
提交任务 → Redis → Worker 执行 QikProp → 结果 CSV 上传 MinIO（归档保留）
                                        → UPSERT 到 admet_compute_result 表（按 SMILES 去重）
                                        → tasks.result 存摘要信息
```

第三方系统共享同一 PostgreSQL，可直接 `SELECT * FROM admet_compute_result WHERE smiles = ?` 获取数据。

### 设计决策

**同一 SMILES + 相同计算参数 → 结果确定性一致**，因此：

- **不需要 `task_id`**：表定位为「分子属性字典」，与具体任务解耦。任务维度的追溯通过 `tasks` 表的 `output_files` 即可。
- **不需要 `title`**：`title` 是用户为分子起的别名，不同任务中同一个 SMILES 可能有不同 title，不属于计算结果。
- **以 `smiles` 为唯一键**：重复提交同一分子时执行 UPSERT，始终保留最新计算结果，天然去重。

---

## 表结构设计

### 核心表：`admet_compute_result`

每行对应一个唯一分子（SMILES）的 QikProp 计算结果，本质上是**分子 ADMET 属性字典表**。

```sql
-- =========================================================================
-- ADMET QikProp 计算结果表（分子属性字典）
-- 以 SMILES 为唯一键，存储每个分子的 21 项 ADMET 属性
-- =========================================================================
CREATE TABLE IF NOT EXISTS admet_compute_result (
    -- 主键
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- =====================================================================
    -- 分子标识（唯一键）
    -- =====================================================================
    smiles          TEXT NOT NULL UNIQUE,    -- SMILES 结构式，唯一标识一个分子
    
    -- =====================================================================
    -- 吸收性质 (Absorption)
    -- =====================================================================
    percent_human_oral_absorption   FLOAT,  -- 人体口服吸收率 (%)，推荐 > 80%
    human_oral_absorption           INT,    -- 口服吸收等级：1=低, 2=中, 3=高
    qp_pcaco                        FLOAT,  -- Caco-2 细胞渗透性 (nm/sec)，> 500 高渗透
    qp_pmdck                        FLOAT,  -- MDCK 细胞渗透性 (nm/sec)，> 500 高渗透
    
    -- =====================================================================
    -- 分布性质 (Distribution)
    -- =====================================================================
    qp_log_po_w     FLOAT,                  -- 辛醇/水分配系数（亲脂性），推荐 -2.0 ~ 6.5
    qp_log_bb       FLOAT,                  -- 血脑屏障穿透性 (log BB)，推荐 -3.0 ~ 1.2
    qp_log_khsa     FLOAT,                  -- 人血清白蛋白结合，推荐 -1.5 ~ 1.5
    qp_log_kp       FLOAT,                  -- 皮肤渗透性，推荐 -8.0 ~ -1.0
    cns             INT,                    -- CNS 活性预测：-2(不活跃) ~ +2(活跃)
    
    -- =====================================================================
    -- 代谢性质 (Metabolism)
    -- =====================================================================
    metab           INT,                    -- 代谢反应位点数量，推荐 1 ~ 8
    
    -- =====================================================================
    -- 排泄/溶解度 (Excretion / Solubility)
    -- =====================================================================
    qp_log_s        FLOAT,                  -- 水溶解度 (log S)，推荐 -6.5 ~ 0.5
    qp_log_pw       FLOAT,                  -- 水/气分配系数，推荐 4 ~ 45
    
    -- =====================================================================
    -- 毒性 (Toxicity)
    -- =====================================================================
    qp_log_herg     FLOAT,                  -- hERG K+ 通道抑制 (log IC50)，> -5 低风险
    
    -- =====================================================================
    -- 药物相似性 (Drug-likeness)
    -- =====================================================================
    rule_of_five    INT,                    -- 违反 Lipinski 五规则数量，0 为最佳
    mol_mw          FLOAT,                  -- 分子量，推荐 130 ~ 725
    
    -- =====================================================================
    -- 表面积性质
    -- =====================================================================
    psa             FLOAT,                  -- 极性表面积 (Å²)，推荐 7 ~ 200
    sasa            FLOAT,                  -- 溶剂可及表面积 (Å²)，推荐 300 ~ 1000
    fosa            FLOAT,                  -- 疏水性表面积 (Å²)，推荐 0 ~ 750
    fisa            FLOAT,                  -- 亲水性表面积 (Å²)，推荐 7 ~ 330
    pisa            FLOAT,                  -- π 电子表面积 (Å²)，推荐 0 ~ 450
    wpsa            FLOAT,                  -- 弱极性表面积 (Å²)，推荐 0 ~ 175
    
    -- =====================================================================
    -- 时间戳
    -- =====================================================================
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 索引设计

```sql
-- SMILES 唯一约束已自动创建索引（由 UNIQUE 约束保证），无需额外建

-- 按 hERG 毒性风险筛选（毒性预警场景）
CREATE INDEX idx_admet_compute_result_herg ON admet_compute_result(qp_log_herg)
    WHERE qp_log_herg IS NOT NULL;

-- 按口服吸收率筛选（成药性筛选场景）
CREATE INDEX idx_admet_compute_result_oral ON admet_compute_result(percent_human_oral_absorption)
    WHERE percent_human_oral_absorption IS NOT NULL;

-- 按 Lipinski 合规筛选
CREATE INDEX idx_admet_compute_result_ro5 ON admet_compute_result(rule_of_five)
    WHERE rule_of_five IS NOT NULL;
```

### 自动更新触发器（复用已有函数）

```sql
CREATE TRIGGER update_admet_compute_result_updated_at
    BEFORE UPDATE ON admet_compute_result
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
```

---

## 列名映射关系

QikProp 原始输出列名 → 数据库列名的映射：

| QikProp 原始列名 | 数据库列名 | 数据类型 | 说明 |
|---|---|---|---|
| `smiles` | `smiles` | TEXT | SMILES 结构式（唯一键） |
| `PercentHumanOralAbsorption` | `percent_human_oral_absorption` | FLOAT | 口服吸收率 (%) |
| `HumanOralAbsorption` | `human_oral_absorption` | INT | 口服吸收等级 |
| `QPlogPo/w` | `qp_log_po_w` | FLOAT | 辛醇/水分配系数 |
| `QPlogS` | `qp_log_s` | FLOAT | 水溶解度 |
| `QPPCaco` | `qp_pcaco` | FLOAT | Caco-2 渗透性 |
| `QPPMDCK` | `qp_pmdck` | FLOAT | MDCK 渗透性 |
| `QPlogBB` | `qp_log_bb` | FLOAT | 血脑屏障 |
| `QPlogHERG` | `qp_log_herg` | FLOAT | hERG 毒性 |
| `CNS` | `cns` | INT | CNS 活性 |
| `#metab` | `metab` | INT | 代谢位点数 |
| `QPlogKhsa` | `qp_log_khsa` | FLOAT | 白蛋白结合 |
| `QPlogKp` | `qp_log_kp` | FLOAT | 皮肤渗透性 |
| `QPlogPw` | `qp_log_pw` | FLOAT | 水/气分配系数 |
| `RuleOfFive` | `rule_of_five` | INT | Lipinski 违规数 |
| `mol_MW` | `mol_mw` | FLOAT | 分子量 |
| `PSA` | `psa` | FLOAT | 极性表面积 |
| `SASA` | `sasa` | FLOAT | 溶剂可及表面积 |
| `FOSA` | `fosa` | FLOAT | 疏水性表面积 |
| `FISA` | `fisa` | FLOAT | 亲水性表面积 |
| `PISA` | `pisa` | FLOAT | π 电子表面积 |
| `WPSA` | `wpsa` | FLOAT | 弱极性表面积 |

在代码中可以用一个字典来完成映射：

```python
COLUMN_MAPPING = {
    'smiles':                       'smiles',
    'PercentHumanOralAbsorption':   'percent_human_oral_absorption',
    'HumanOralAbsorption':          'human_oral_absorption',
    'QPlogPo/w':                    'qp_log_po_w',
    'QPlogS':                       'qp_log_s',
    'QPPCaco':                      'qp_pcaco',
    'QPPMDCK':                      'qp_pmdck',
    'QPlogBB':                      'qp_log_bb',
    'QPlogHERG':                    'qp_log_herg',
    'CNS':                          'cns',
    '#metab':                       'metab',
    'QPlogKhsa':                    'qp_log_khsa',
    'QPlogKp':                      'qp_log_kp',
    'QPlogPw':                      'qp_log_pw',
    'RuleOfFive':                   'rule_of_five',
    'mol_MW':                       'mol_mw',
    'PSA':                          'psa',
    'SASA':                         'sasa',
    'FOSA':                         'fosa',
    'FISA':                         'fisa',
    'PISA':                         'pisa',
    'WPSA':                         'wpsa',
}
```

---

## 数据写入方案

### 写入时机

在 `wrapper.py` 的 `_handle_qikprop` 方法中，QikProp 计算完成并上传 MinIO **之后**，批量 UPSERT 到 `admet_compute_result` 表。

### 写入流程

```
QikProp 计算完成
    │
    ├── 1. 上传 CSV 到 MinIO（已有，保留作为归档）
    │
    └── 2. 批量 UPSERT 到 admet_compute_result 表（新增）
            ├── 将 DataFrame 列名通过 COLUMN_MAPPING 映射为数据库列名
            ├── 使用 INSERT ... ON CONFLICT (smiles) DO UPDATE 实现去重
            ├── 已存在的 SMILES 更新属性值和 updated_at
            └── 写入失败不影响任务成功状态（降级处理）
```

### 参考代码

```python
def save_admet_compute_result(self, result_df: pd.DataFrame) -> bool:
    """将 ADMET 计算结果写入数据库（按 SMILES 去重）"""
    try:
        # 列名映射
        df_db = result_df.rename(columns=COLUMN_MAPPING)

        # 取数据库中存在的列
        db_columns = [
            'smiles',
            'percent_human_oral_absorption', 'human_oral_absorption',
            'qp_log_po_w', 'qp_log_s', 'qp_pcaco', 'qp_pmdck',
            'qp_log_bb', 'qp_log_herg', 'cns', 'metab',
            'qp_log_khsa', 'qp_log_kp', 'qp_log_pw',
            'rule_of_five', 'mol_mw',
            'psa', 'sasa', 'fosa', 'fisa', 'pisa', 'wpsa'
        ]
        available = [c for c in db_columns if c in df_db.columns]
        df_insert = df_db[available]

        # 构建 UPSERT SQL
        cols = ', '.join(available)
        placeholders = ', '.join(['%s'] * len(available))
        # 冲突时更新所有属性列（排除 smiles 本身）
        update_cols = [c for c in available if c != 'smiles']
        update_clause = ', '.join(
            f"{c} = EXCLUDED.{c}" for c in update_cols
        )
        sql = f"""
            INSERT INTO admet_compute_result ({cols})
            VALUES ({placeholders})
            ON CONFLICT (smiles) DO UPDATE SET
                {update_clause},
                updated_at = CURRENT_TIMESTAMP
        """

        values = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]

        with self.db.get_connection() as conn:
            cur = conn.cursor()
            cur.executemany(sql, values)
            conn.commit()

        logger.info(f"已写入 {len(values)} 条 ADMET 结果到数据库（UPSERT）")
        return True

    except Exception as e:
        logger.warning(f"ADMET 结果写入数据库失败（不影响任务状态）: {e}")
        return False
```

### UPSERT 行为说明

| 场景 | 行为 |
|------|------|
| 新 SMILES | INSERT 新行 |
| 已存在的 SMILES | UPDATE 所有属性列 + `updated_at` |
| 批量提交含重复 SMILES | 最后一条生效 |

**好处**：
- 天然去重，数据量可控，不会无限膨胀
- 第三方系统查 SMILES 永远只有一行结果，无需 DISTINCT
- 同一分子被多个任务计算时，只保留最新值

---

## 第三方系统查询示例

由于共享同一个 PostgreSQL，第三方系统可直接用 SQL 查询。

### 1. 根据 SMILES 获取 ADMET 属性

```sql
SELECT * FROM admet_compute_result WHERE smiles = 'CC(=O)OC1=CC=CC=C1C(=O)O';
```

### 2. 批量查询一组分子的 ADMET 属性

```sql
SELECT * FROM admet_compute_result
WHERE smiles IN ('CCO', 'CC(=O)OC1=CC=CC=C1C(=O)O', 'CN1C=NC2=C1C(=O)N(C(=O)N2C)C');
```

### 3. 筛选高口服吸收 + 低心脏毒性的分子

```sql
SELECT smiles, 
       percent_human_oral_absorption, 
       qp_log_herg, 
       rule_of_five
FROM admet_compute_result
WHERE percent_human_oral_absorption > 80
  AND qp_log_herg > -5
  AND rule_of_five = 0
ORDER BY percent_human_oral_absorption DESC;
```

### 4. 全库成药性统计概览

```sql
SELECT 
    COUNT(*) AS total_molecules,
    AVG(percent_human_oral_absorption) AS avg_oral_absorption,
    COUNT(*) FILTER (WHERE percent_human_oral_absorption > 80) AS high_absorption_count,
    COUNT(*) FILTER (WHERE qp_log_herg > -5) AS low_herg_risk_count,
    COUNT(*) FILTER (WHERE rule_of_five = 0) AS lipinski_compliant_count
FROM admet_compute_result;
```

### 5. 超出推荐范围的属性预警

```sql
SELECT smiles,
    CASE WHEN qp_log_herg <= -5 THEN '⚠ hERG 高风险' END AS herg_warning,
    CASE WHEN percent_human_oral_absorption < 25 THEN '⚠ 口服吸收差' END AS absorption_warning,
    CASE WHEN rule_of_five > 0 THEN '⚠ 违反 Ro5' END AS ro5_warning,
    CASE WHEN mol_mw > 725 THEN '⚠ 分子量过大' END AS mw_warning
FROM admet_compute_result
WHERE qp_log_herg <= -5 
   OR percent_human_oral_absorption < 25 
   OR rule_of_five > 0 
   OR mol_mw > 725;
```

### 6. 第三方系统关联自身业务表查询

```sql
-- 假设第三方系统有 project_molecules 表记录项目中的分子
SELECT pm.project_id, pm.molecule_name, ar.*
FROM project_molecules pm
JOIN admet_compute_result ar ON pm.smiles = ar.smiles
WHERE pm.project_id = 'PRJ-001'
ORDER BY ar.percent_human_oral_absorption DESC;
```

---

## 数据量评估与维护

### 数据量预估

由于以 SMILES 去重，数据量取决于**唯一分子数**而非任务数：

| 场景 | 唯一分子数 | 存储空间 |
|------|-----------|---------|
| 小团队 | 1,000 - 10,000 | < 5 MB |
| 中等规模 | 10,000 - 100,000 | < 50 MB |
| 大规模虚拟筛选 | 100,000 - 1,000,000 | ~300 MB |
| 超大规模 | 1,000,000+ | ~1 GB+ |

21 个 FLOAT/INT 列 + SMILES，**单行约 250-400 字节**。由于去重存储，数据量远小于按任务存储的方案。

### 维护建议

- **不需要分区**：去重后的数据量通常在百万级以内，PostgreSQL 单表即可高效处理
- **不需要归档**：字典表性质，所有数据都是有效数据，没有冷热之分
- **MinIO 中的 CSV 归档任务维度的原始结果**：如需按任务追溯，查 `tasks.output_files` 定位到 MinIO 文件

---

## 与现有架构的关系

```
                     PostgreSQL (共享)
                    ┌──────────────────────────────────────────────────┐
                    │  tasks              ← aidd-platform 管理        │
                    │  workers            ← aidd-platform 管理        │
                    │  admet_compute_result      ← Worker 写入（UPSERT）     │
                    │                       第三方按 SMILES 直接查询  │
                    └──────────────────────────────────────────────────┘
                          ▲          ▲              ▲
                          │          │              │
              ┌───────────┘          │              └───────────┐
              │                      │                          │
     aidd-platform            aidd-toolkit Worker         第三方业务系统
     (任务调度 + API)         (QikProp 计算)            (JOIN 自身分子表查询)
                                     │
                                     │ UPSERT
                                     ▼
                              admet_compute_result
```

### 数据流说明

1. **任务提交**：第三方系统或用户通过 Platform API 提交 ADMET 任务，SMILES 列表进入 Redis 队列
2. **计算执行**：Worker 消费队列，执行 QikProp 计算
3. **结果持久化**：
   - CSV 上传到 MinIO（任务维度的完整归档）
   - UPSERT 到 `admet_compute_result`（分子维度的去重字典）
4. **结果查询**：第三方系统用 SMILES 作为 JOIN 键关联自身业务表，直接 SQL 查询

### 职责划分

| 组件 | 对 `admet_compute_result` 的操作 |
|------|--------------------------|
| aidd-toolkit Worker | **UPSERT** — 计算完成后写入（按 SMILES 去重） |
| 第三方系统 | **SELECT** — 用 SMILES 作为键直接 SQL 查询 |
| aidd-platform | 不直接操作此表 |

---

## 完整建表 SQL

可追加到 [scripts/init-db.sql](../scripts/init-db.sql) 末尾：

```sql
-- =========================================================================
-- ADMET QikProp 计算结果表（分子属性字典，按 SMILES 去重）
-- =========================================================================
CREATE TABLE IF NOT EXISTS admet_compute_result (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- 分子标识（唯一键）
    smiles          TEXT NOT NULL UNIQUE,
    
    -- 吸收 (Absorption)
    percent_human_oral_absorption   FLOAT,
    human_oral_absorption           INT,
    qp_pcaco                        FLOAT,
    qp_pmdck                        FLOAT,
    
    -- 分布 (Distribution)
    qp_log_po_w     FLOAT,
    qp_log_bb       FLOAT,
    qp_log_khsa     FLOAT,
    qp_log_kp       FLOAT,
    cns             INT,
    
    -- 代谢 (Metabolism)
    metab           INT,
    
    -- 溶解度
    qp_log_s        FLOAT,
    qp_log_pw       FLOAT,
    
    -- 毒性 (Toxicity)
    qp_log_herg     FLOAT,
    
    -- 药物相似性
    rule_of_five    INT,
    mol_mw          FLOAT,
    
    -- 表面积
    psa             FLOAT,
    sasa            FLOAT,
    fosa            FLOAT,
    fisa            FLOAT,
    pisa            FLOAT,
    wpsa            FLOAT,
    
    -- 时间戳
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 索引（SMILES 唯一约束已自动创建索引）
CREATE INDEX idx_admet_compute_result_herg ON admet_compute_result(qp_log_herg) WHERE qp_log_herg IS NOT NULL;
CREATE INDEX idx_admet_compute_result_oral ON admet_compute_result(percent_human_oral_absorption) WHERE percent_human_oral_absorption IS NOT NULL;
CREATE INDEX idx_admet_compute_result_ro5 ON admet_compute_result(rule_of_five) WHERE rule_of_five IS NOT NULL;

-- 自动更新 updated_at
CREATE TRIGGER update_admet_compute_result_updated_at
    BEFORE UPDATE ON admet_compute_result
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
```
