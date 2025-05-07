import os, sys
import asyncio
import threading
from fastapi import FastAPI

current_dir = os.path.dirname(os.path.abspath(__file__))
# 将父目录添加到 sys.path
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from app.services.faird_service import get_flight_server
from app.api import endpoints

app = FastAPI()

# 注册 API 路由
app.include_router(endpoints.router)

@app.on_event("startup")
async def startup_event():
    # 在后台线程中启动 Flight 服务
    def run_flight_server():
        flight_server = get_flight_server()
        flight_server.serve()

    thread = threading.Thread(target=run_flight_server, daemon=True)
    thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)