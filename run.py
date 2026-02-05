#!/usr/bin/env python3
"""
开发模式启动脚本
使用方法: python run.py
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8333,
        reload=True
    )
