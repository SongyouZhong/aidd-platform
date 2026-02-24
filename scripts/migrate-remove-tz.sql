-- =========================================================================
-- 迁移脚本：移除时区信息
-- 将所有 TIMESTAMP WITH TIME ZONE 列改为 TIMESTAMP（不带时区）
-- PostgreSQL 会自动将已有的 tz-aware 值转换为服务器本地时间存储
-- =========================================================================

-- tasks 表
ALTER TABLE tasks ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE 'Asia/Shanghai';
ALTER TABLE tasks ALTER COLUMN started_at TYPE TIMESTAMP USING started_at AT TIME ZONE 'Asia/Shanghai';
ALTER TABLE tasks ALTER COLUMN completed_at TYPE TIMESTAMP USING completed_at AT TIME ZONE 'Asia/Shanghai';
ALTER TABLE tasks ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at AT TIME ZONE 'Asia/Shanghai';

-- workers 表
ALTER TABLE workers ALTER COLUMN registered_at TYPE TIMESTAMP USING registered_at AT TIME ZONE 'Asia/Shanghai';
ALTER TABLE workers ALTER COLUMN last_heartbeat TYPE TIMESTAMP USING last_heartbeat AT TIME ZONE 'Asia/Shanghai';
ALTER TABLE workers ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at AT TIME ZONE 'Asia/Shanghai';

-- 验证：查看修改后的列类型
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name IN ('tasks', 'workers')
  AND column_name IN ('created_at', 'started_at', 'completed_at', 'updated_at', 'registered_at', 'last_heartbeat')
ORDER BY table_name, column_name;
