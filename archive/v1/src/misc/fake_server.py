from typing import Deque, AsyncGenerator
from collections import deque
import asyncio
from asyncio import Task
from contextlib import asynccontextmanager

from fastapi import FastAPI
from classy_fastapi import Routable, get
import uvicorn

BUFFER: Deque[int] = deque(maxlen=1)
START: int = 50
TIMEOUT: float = 5.0

async def produce_items(buffer: Deque[int], *, start: int = 0, timeout: float) -> None:
    cur_num: int = start
    while True:
        buffer.append(cur_num)
        await asyncio.sleep(timeout)
        cur_num += 1

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    task: Task = asyncio.create_task(produce_items(BUFFER, start=START, timeout=TIMEOUT))
    print("Started producer")
    yield
    task.cancel()
    print("Stopped producer")

class AppRoutes(Routable):

    @get("/current")
    async def get_current_item(self) -> int | None:
        return BUFFER[-1] if len(BUFFER) > 0 else None

def _get_app() -> FastAPI:
    app: FastAPI = FastAPI(lifespan=lifespan)
    routes: AppRoutes = AppRoutes()
    app.include_router(routes.router)
    return app

def run():
    app: FastAPI = _get_app()
    uvicorn.run(app=app,
                host="localhost",
                port=8081)

if __name__ == "__main__":
    run()