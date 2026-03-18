__version__ = "0.1.0"

from typing import List, Iterator, Self, TypeAlias, Literal
from dataclasses import dataclass
from timeit import default_timer
import time
from datetime import datetime, timezone
import multiprocessing
from multiprocessing import Queue
from multiprocessing.queues import Queue as QueueType
from queue import Empty as QueueEmptyException
import asyncio

import numpy as np
from numpy.typing import NDArray
import cv2
from pydantic import BaseModel
from pydantic_yaml import parse_yaml_file_as
import uvicorn
from fastapi import FastAPI, Response
from classy_fastapi import Routable, get

from archive.v1.src.v_0_1_0.pipeline.pipeline import ImageStreamingPipelineConfig, ImageStreamingPipeline
from common.utils.concurrency.mp_utils import update_single_item_queue, ShutdownableProcess, stop_shutdownable

# Set to `forkserver` to prevent signal handler conflicts inside the pipeline worker process
# (which did happen at times with `fork` due to FastAPI's own handlers being injected).
# It is possible for `forkserver` to still cause conflicts -- set to `spawn` in that case
# (potentially slower startup, but inherits less of the parent process's state).
PROCESS_START_METHOD: Literal["fork", "forkserver", "spawn"] = "forkserver"

OUT_IMG_FORMAT: str = ".jpg"
HTTP_RESPONSE_MEDIA_TYPE: str = "image/jpeg"
CV2_FLAGS: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 90]

GET_LATEST_FRAME_MAX_TIMEOUT: float = 5.0
PIPELINE_WORKER_ALIVE_STATUS_POLLING_INTERVAL: float = 1.0
PIPELINE_WORKER_TIMEOUT_PER_JOIN: float = 10.0

class FramePacket(BaseModel):
    img: bytes
    ts_unix: float

FramePacketQueue: TypeAlias = QueueType[FramePacket | None]

def _encode_numpy_image(img_numpy: NDArray) -> bytes:
    contiguous: NDArray = np.ascontiguousarray(img_numpy)
    success, encoded = cv2.imencode(
        ext=OUT_IMG_FORMAT, img=contiguous, params=CV2_FLAGS
    )  # type: bool, NDArray
    encoded_bytes: bytes = encoded.tobytes()
    return encoded_bytes

def _numpy_to_frame_packet(img_numpy: NDArray) -> FramePacket:
    encoded_bytes: bytes = _encode_numpy_image(img_numpy)
    ts_unix: float = datetime.now(tz=timezone.utc).timestamp()
    return FramePacket(img=encoded_bytes, ts_unix=ts_unix)

@dataclass
class PipelineWorkerTimes:
    in_pipeline: float
    packet_creation: float
    buffer_update: float

def _get_pipeline_worker_timing_log_line(frame_ts: float, times: PipelineWorkerTimes) -> str:
    total: float = times.in_pipeline + times.packet_creation + times.buffer_update
    out: str = f"Produced frame {frame_ts} in {total:.3f} s\n"
    out += " | ".join(
        f"{name}: {value:.3f}" for name, value in zip(
            ["in pipeline", "packet creation", "buffer update"],
            [times.in_pipeline, times.packet_creation, times.buffer_update]
        )
    )
    return out

class PipelineWorker(ShutdownableProcess):

    """
    A wrapper around `ImageStreamingPipeline`, meant to be run as a separate process.
    Continuously gets new items from the pipeline and pushes them into the provided
    `multiprocessing.Queue` (currently used as a makeshift ring buffer with a max length of one
    in which the item is continually being replaced by this producer, and occasionally consumed
    by a single consumer).
    On receiving the shutdown signal (the instance's `.shutdown()` method),
    finishes the processing of the current item and joins.
    """

    def __init__(self,
                 *, buffer: FramePacketQueue,
                 config_path: str,
                 min_timeout: float,
                 with_timing: bool = True,
                 **kwargs
                 ):
        if not min_timeout >= 0.0:
            raise ValueError(f"Invalid min_timeout (must be non-negative), received: {min_timeout}")

        super().__init__(**kwargs)

        self._buffer: FramePacketQueue = buffer
        self._config_path: str = config_path
        self._min_timeout: float = min_timeout
        self._with_timing: bool = with_timing

    def _run_worker(self):
        """
        Initialise a pipeline from the config, start it, and push the produced items to the queue.
        The queue is meant to contain one item only and have one producer (this worker) and one consumer.
        For every new item, if the queue is not empty, empty it and then put a new item in it.
        This ensures that the consumer will always get the newest item available
        by the time it calls `buffer.get()` to retrieve one.

        TODO: For multiple consumers (multiple requests), implement a pub-sub (or perhaps something else).
        TODO: Implement a ring buffer in shared memory rather than limiting the number of buffered items to one.
        """

        pipeline_config: ImageStreamingPipelineConfig = parse_yaml_file_as(
            ImageStreamingPipelineConfig, self._config_path
        )
        pipeline: ImageStreamingPipeline = ImageStreamingPipeline(pipeline_config)

        last_append_ts: float | None = None
        try:
            pipeline_iter: Iterator[NDArray] = iter(pipeline)
            idx: int = 0
            while not self.is_exit_signal():
                try:

                    start_ts: float = default_timer()

                    # get the next item from the pipeline
                    img: NDArray = next(pipeline_iter)
                    img_produced_ts: float = default_timer()
                    # convert to a packet
                    packet: FramePacket = _numpy_to_frame_packet(img)
                    packet_created_ts: float = default_timer()

                    cur_ts: float = default_timer()
                    to_sleep: float = (last_append_ts + self._min_timeout - cur_ts) if idx > 0 else 0.0
                    if to_sleep > 0.0:
                        time.sleep(to_sleep)

                    buffer_update_start_ts: float = default_timer()
                    # empty the buffer if not empty, then put the packet in it
                    update_single_item_queue(packet, self._buffer)
                    buffer_update_end_ts: float = default_timer()

                    last_append_ts = default_timer()
                    if self._with_timing:
                        log_line: str = _get_pipeline_worker_timing_log_line(
                            packet.ts_unix,
                            PipelineWorkerTimes(in_pipeline=img_produced_ts - start_ts,
                                                packet_creation=packet_created_ts - img_produced_ts,
                                                buffer_update=buffer_update_end_ts - buffer_update_start_ts)
                        )
                        print(log_line)
                    else:
                        print(f"Appended to buffer: {packet.ts_unix}")
                    idx += 1
                except StopIteration as e:
                    print(f"Hit exception: {e}")
                    raise RuntimeError("Stream ended") from e
                except Exception as e:
                    print(f"Hit exception: {e}")
                    raise RuntimeError("Caught exception in stream") from e
        finally:
            # put a sentinel to signal that the processing has ended
            update_single_item_queue(None, self._buffer)
            print("Put sentinel")

    def run(self):
        self._run_worker()


class PipelineWrapper:

    """
    An asynchronous context manager wrapper around the pipeline worker
    that launches it in a separate process.
    """

    def __init__(self, *,
                 buffer: FramePacketQueue,
                 min_timeout: float = 0.0,
                 config_path: str):
        self.buffer: FramePacketQueue = buffer
        self._min_timeout: float = min_timeout
        self._config_path: str = config_path

        self._worker: PipelineWorker = self._create_worker()

    def __enter__(self) -> Self:
        self.start_worker()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop_worker()

    def _create_worker(self) -> PipelineWorker:
        worker: PipelineWorker = PipelineWorker(name="pipeline_worker",
                                                buffer=self.buffer,
                                                config_path=self._config_path,
                                                min_timeout=self._min_timeout)
        return worker

    def start_worker(self):
        if self._worker.is_alive():
            raise RuntimeError("Pipeline worker already started")
        print("Starting pipeline worker...")
        self._worker.start()
        print("Pipeline worker started")

    def stop_worker(self):
        stop_shutdownable(self._worker,
                          timeout_per_join=PIPELINE_WORKER_TIMEOUT_PER_JOIN)


class AppRoutes(Routable):

    def __init__(self, buffer: FramePacketQueue):
        super().__init__()
        self._buffer: FramePacketQueue = buffer

    @get("/frame")
    async def get_latest_frame(self) -> Response | None:
        try:
            packet: FramePacket | None = await asyncio.to_thread(
                self._buffer.get,
                timeout=GET_LATEST_FRAME_MAX_TIMEOUT
            )
            if packet is None:
                raise RuntimeError("No frame packets available, pipeline processing has ended")
            return Response(content=packet.img,
                            media_type="image/jpeg")
        except ValueError as e:
            # the buffer is closed
            raise RuntimeError("Called get_latest_frame_ts on a closed buffer") from e
        except QueueEmptyException as e:
            # the buffer is empty: the assumed reason is that no items
            # have been put into it since it was empty and the timeout event was triggered
            raise RuntimeError("Buffer empty, timed out waiting") from e

def _get_app(buffer: FramePacketQueue) -> FastAPI:

    """
    A factory for the FastAPI app.
    """

    # @asynccontextmanager
    # async def lifespan(fastapi_app: FastAPI) -> AsyncGenerator[None]:
    #     yield

    app: FastAPI = FastAPI(
        # lifespan=lifespan
    )
    routes: AppRoutes = AppRoutes(buffer)
    app.include_router(routes.router)
    return app

def _build_pipeline_wrapper(buffer: FramePacketQueue,
                            min_timeout: float):
    config_path: str = "/src/v1/pipeline/image_streaming.yaml"
    pipeline_wrapper: PipelineWrapper = PipelineWrapper(
        buffer=buffer, min_timeout=min_timeout, config_path=config_path
    )
    return pipeline_wrapper


def run():
    min_timeout: float = 0.0
    buffer: FramePacketQueue = Queue()
    try:
        pipeline: PipelineWrapper = _build_pipeline_wrapper(buffer, min_timeout)
        app: FastAPI = _get_app(buffer)
        with pipeline:
            print("Started pipeline")
            uvicorn.run(app=app,
                        host="localhost", port=8081)
    finally:
        buffer.close()


if __name__ == "__main__":
    multiprocessing.set_start_method("forkserver")
    run()