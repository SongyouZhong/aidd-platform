-- =========================================================================
-- AIDD Platform 数据库初始化脚本
-- =========================================================================

-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =========================================================================
-- 任务表
-- =========================================================================
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service VARCHAR(100) NOT NULL,
    task_type VARCHAR(100) DEFAULT '',
    name VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 2,
    
    -- 输入输出
    input_params JSONB DEFAULT '{}'::jsonb,
    input_files JSONB DEFAULT '[]'::jsonb,
    output_files JSONB DEFAULT '[]'::jsonb,
    result JSONB,
    
    -- 资源需求
    resource_cpu_cores INT DEFAULT 1,
    resource_memory_gb FLOAT DEFAULT 1.0,
    resource_gpu_count INT DEFAULT 0,
    resource_gpu_memory_gb FLOAT DEFAULT 0.0,
    
    -- 执行信息
    worker_id UUID,
    job_id UUID,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    timeout_seconds INT DEFAULT 3600,
    progress FLOAT DEFAULT 0,
    error_message TEXT,
    duration_seconds FLOAT,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 任务索引
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_service ON tasks(service);
CREATE INDEX idx_tasks_priority ON tasks(priority);
CREATE INDEX idx_tasks_job_id ON tasks(job_id);
CREATE INDEX idx_tasks_worker_id ON tasks(worker_id);
CREATE INDEX idx_tasks_created_at ON tasks(created_at);

-- =========================================================================
-- 作业表
-- =========================================================================
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    priority VARCHAR(50) NOT NULL DEFAULT 'normal',
    
    -- 统计信息
    total_tasks INT DEFAULT 0,
    completed_tasks INT DEFAULT 0,
    failed_tasks INT DEFAULT 0,
    
    -- 结果
    results JSONB,
    error_message TEXT,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 作业索引
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_created_at ON jobs(created_at);

-- =========================================================================
-- Worker 表
-- =========================================================================
CREATE TABLE IF NOT EXISTS workers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hostname VARCHAR(255) NOT NULL,
    ip_address VARCHAR(50),
    port INT DEFAULT 8080,
    status VARCHAR(50) NOT NULL DEFAULT 'offline',
    
    -- 总资源
    total_cpu_cores INT NOT NULL,
    total_memory_gb FLOAT NOT NULL,
    total_gpu_count INT DEFAULT 0,
    total_gpu_memory_gb FLOAT DEFAULT 0.0,
    
    -- 已用资源
    used_cpu_cores INT DEFAULT 0,
    used_memory_gb FLOAT DEFAULT 0.0,
    used_gpu_count INT DEFAULT 0,
    used_gpu_memory_gb FLOAT DEFAULT 0.0,
    
    -- 配置
    supported_services TEXT[], -- PostgreSQL 数组类型
    max_concurrent_tasks INT DEFAULT 4,
    labels JSONB,
    
    -- 时间戳
    registered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Worker 索引
CREATE INDEX idx_workers_status ON workers(status);
CREATE INDEX idx_workers_hostname ON workers(hostname);

-- =========================================================================
-- 触发器：自动更新 updated_at
-- =========================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_tasks_updated_at
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_workers_updated_at
    BEFORE UPDATE ON workers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =========================================================================
-- 完成
-- =========================================================================
SELECT 'AIDD Platform database initialized successfully' AS status;
