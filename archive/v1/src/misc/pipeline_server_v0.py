from typing import List, Deque, AsyncGenerator
from collections import deque
from timeit import default_timer
from datetime import datetime, timezone
import asyncio
from asyncio import Task, Lock, sleep
from contextlib import asynccontextmanager

import numpy as np
from numpy.typing import NDArray
import cv2
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_file_as
from fastapi import FastAPI
from classy_fastapi import Routable, get
import uvicorn

from archive.v1.src.v_0_1_0.pipeline.pipeline import ImageStreamingPipelineConfig, ImageStreamingPipeline

OUT_IMG_FORMAT: str = ".jpg"
CV2_FLAGS: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 90]

class FramePacket(BaseModel):
    img: bytes
    ts_unix: float

def _numpy_to_frame_packet(img_numpy: NDArray) -> FramePacket:
    contiguous: NDArray = np.ascontiguousarray(img_numpy)
    success, encoded = cv2.imencode(
        ext=OUT_IMG_FORMAT, img=contiguous, params=CV2_FLAGS
    ) # type: bool, NDArray
    encoded_bytes: bytes = encoded.tobytes()
    ts_unix: float = datetime.now(tz=timezone.utc).timestamp()
    return FramePacket(img=encoded_bytes, ts_unix=ts_unix)

class PipelineWrapper:

    def __init__(self, *,
                 buffer: Deque[FramePacket],
                 min_timeout: float = 0.0,
                 config_path: str):
        pipeline_config: ImageStreamingPipelineConfig = parse_yaml_file_as(
            ImageStreamingPipelineConfig, config_path
        )
        self._pipeline: ImageStreamingPipeline = ImageStreamingPipeline(pipeline_config)
        self._buffer: Deque[FramePacket] = buffer
        self._min_timeout: float = min_timeout
        self._lock: Lock = Lock()
        self._is_producing: bool = False

    async def produce_to_deque(self) -> None:
        try:
            self._is_producing = True

            last_append_ts: float | None = None

            for idx, img in enumerate(self._pipeline): # type: int, NDArray
                packet: FramePacket = _numpy_to_frame_packet(img)
                cur_ts: float = default_timer()
                to_sleep: float = (
                    last_append_ts + self._min_timeout - cur_ts
                ) if idx > 0 else 0.0
                if to_sleep > 0.0:
                    await sleep(to_sleep)
                else:
                    # yield control anyway
                    await sleep(0)
                async with self._lock:
                    self._buffer.append(packet)
                # wait if needed
                last_append_ts = default_timer()
                print(f"Appended to buffer: {packet.ts_unix}")
        finally:
            self._is_producing = False

    @asynccontextmanager
    async def fastapi_lifespan(self, app: FastAPI) -> AsyncGenerator[None]:
        task: Task = asyncio.create_task(self.produce_to_deque())
        print("Started pipeline")
        yield
        task.cancel()
        print("Stopped pipeline")


class AppRoutes(Routable):

    def __init__(self, buffer: Deque[FramePacket]):
        super().__init__()
        self._buffer: Deque[FramePacket] = buffer

    @get("/frame")
    async def get_latest_frame(self) -> float | None:
        return self._buffer[-1].ts_unix if len(self._buffer) > 0 else None

def _get_app(pipeline: PipelineWrapper, *, buffer: Deque[FramePacket]) -> FastAPI:
    app: FastAPI = FastAPI(lifespan=pipeline.fastapi_lifespan)
    routes: AppRoutes = AppRoutes(buffer)
    app.include_router(routes.router)
    return app

def _build_pipeline_wrapper(buffer: Deque[FramePacket],
                            min_timeout: float):
    config_path: str = "/src/v1/pipeline/image_streaming.yaml"
    pipeline_wrapper: PipelineWrapper = PipelineWrapper(
        buffer=buffer, min_timeout=min_timeout, config_path=config_path)
    return pipeline_wrapper

def run():
    buffer_maxlen: int = 5
    min_timeout: float = 0.5
    buffer: Deque[FramePacket] = deque(maxlen=buffer_maxlen)
    pipeline: PipelineWrapper = _build_pipeline_wrapper(buffer, min_timeout)
    app: FastAPI = _get_app(pipeline, buffer=buffer)
    uvicorn.run(app=app,
                host="localhost", port=8081)

if __name__ == "__main__":
    run()