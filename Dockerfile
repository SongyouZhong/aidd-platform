# =========================================================================
# AIDD Platform Docker 镜像
# =========================================================================
FROM mambaorg/micromamba:1.5-jammy

LABEL maintainer="AIDD Team"
LABEL description="AIDD Platform - Task Scheduling System"

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MAMBA_DOCKERFILE_ACTIVATE=1

# 设置工作目录
WORKDIR /app

# 切换到 root 用户安装系统依赖
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 切换回 micromamba 用户
USER $MAMBA_USER

# 复制环境配置文件
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml .

# 使用 mamba 创建环境
RUN micromamba install -y -n base -f environment.yml && \
    micromamba clean --all --yes

# 复制应用代码
COPY --chown=$MAMBA_USER:$MAMBA_USER app/ ./app/
COPY --chown=$MAMBA_USER:$MAMBA_USER config/ ./config/

# 暴露端口
EXPOSE 8000

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# 启动命令
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
