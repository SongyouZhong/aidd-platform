"""
AIDD Platform - 任务调度平台

FastAPI 应用入口
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.api.v1 import api_router
from app.api.deps import get_dispatcher, get_redis_client, close_connections

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    logger.info("Starting AIDD Platform...")
    
    # 初始化 Redis 连接
    try:
        redis_client = await get_redis_client()
        logger.info("Redis connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
    
    # 启动调度器（本地模式）
    dispatcher = get_dispatcher()
    # 注意：在生产环境中应该在后台任务中运行
    # asyncio.create_task(dispatcher.start())
    
    logger.info("AIDD Platform started successfully")
    
    yield
    
    # 关闭时
    logger.info("Shutting down AIDD Platform...")
    await dispatcher.stop()
    await close_connections()
    logger.info("AIDD Platform shut down complete")


def create_app() -> FastAPI:
    """创建 FastAPI 应用"""
    settings = get_settings()
    
    app = FastAPI(
        title="AIDD Platform",
        description="AI Drug Discovery 任务调度平台",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )
    
    # CORS 中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # 生产环境应限制
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # 注册路由
    app.include_router(api_router, prefix="/api/v1")
    
    return app


# 创建应用实例
app = create_app()


@app.get("/")
async def root():
    """根路径"""
    return {
        "name": "AIDD Platform",
        "version": "0.1.0",
        "docs": "/docs"
    }
