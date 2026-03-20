__version__ = "0.2.0"

from typing import Iterator, AsyncGenerator, Self, TypeAlias, Tuple, OrderedDict as OrderedDictType
from dataclasses import dataclass
from collections import OrderedDict
from timeit import default_timer
import time
from threading import Lock, Thread
import multiprocessing
from multiprocessing import Queue
from multiprocessing.queues import Queue as QueueType
from queue import Full as QueueFullException
from contextlib import asynccontextmanager
import logging
from logging import Logger

from numpy.typing import NDArray
from pydantic_yaml import parse_yaml_file_as
import uvicorn
from fastapi import FastAPI, Response
from classy_fastapi import Routable, get

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from archive.v1.src.v_0_1_0.pipeline.pipeline import PipelineArtefacts_Old
from archive.v1.src.v_0_2_0.pipeline.pipeline import ArtefactsStreamingPipeline, ArtefactsStreamingPipelineConfig
from tram_analytics.v1.pipeline.server.helpers.packet import _encode_numpy_image
from tram_analytics.v1.pipeline.server.worker.worker import PIPELINE_WORKER_TIMEOUT_PER_JOIN, PipelineWorkerTimes
from common.utils.concurrency.mp_utils import ShutdownableProcess, stop_shutdownable

PIPELINE_CACHE_MAX_LEN: int = 50

@dataclass
class PipelinePacket:
    annotated_image: bytes
    artefacts: PipelineArtefacts_Old

def _build_pipeline_packet(annot_img_numpy: NDArray, artefacts: PipelineArtefacts_Old) -> PipelinePacket:
    encoded_img: bytes = _encode_numpy_image(annot_img_numpy)
    return PipelinePacket(annotated_image=encoded_img,
                          artefacts=artefacts)

PipelineQueue: TypeAlias = QueueType[PipelinePacket | None]

def _get_pipeline_worker_timing_log_line(frame_id: str, times: PipelineWorkerTimes) -> str:
    total_without_sleeping: float = times.in_pipeline + times.packet_creation + times.buffer_update
    total: float = total_without_sleeping + times.sleeping
    out: str = f"Produced frame {frame_id} in {total:.3f} s ({total_without_sleeping:.3f} s without sleeping)\n"
    out += " | ".join(
        f"{name}: {value:.3f}" for name, value in zip(
            ["in pipeline", "packet creation", "sleeping", "buffer update"],
            [times.in_pipeline, times.packet_creation, times.sleeping, times.buffer_update]
        )
    )
    return out

class PipelineWorker(ShutdownableProcess):

    """
    A wrapper around `ArtefactsStreamingPipeline`, meant to be run as a separate process.
    On start, initialises a new pipeline, then continuously gets new items from it
    and pushes them into the provided `multiprocessing.Queue`.
    IMPORTANT: If the queue is full, DISCARDS new items.
    On receiving the shutdown signal (the instance's `.shutdown()` method),
    finishes the processing of the current item and joins.
    """

    def __init__(self,
                 *, buffer: PipelineQueue,
                 config_path: str,
                 min_timeout: float,
                 with_timing: bool = True,
                 **kwargs
                 ):
        if not min_timeout >= 0.0:
            raise ValueError(f"Invalid min_timeout (must be non-negative), received: {min_timeout}")

        super().__init__(**kwargs)

        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._buffer: PipelineQueue = buffer
        self._config_path: str = config_path
        self._min_timeout: float = min_timeout
        self._with_timing: bool = with_timing

    def _run_worker(self):
        """
        Initialise a pipeline from the config, start it, and push the produced items to the queue.

        TODO: For multiple consumers (multiple requests), implement a pub-sub (or perhaps something else).
        TODO: Implement a ring buffer in shared memory rather than limiting the number of buffered items to one.
        TODO: change to drop-head behaviour when full (prioritise recent items)
        """

        pipeline_config: ArtefactsStreamingPipelineConfig = parse_yaml_file_as(
            ArtefactsStreamingPipelineConfig, self._config_path
        )
        pipeline: ArtefactsStreamingPipeline = ArtefactsStreamingPipeline(pipeline_config)

        last_append_ts: float | None = None
        try:
            pipeline_iter: Iterator[Tuple[NDArray, PipelineArtefacts_Old]] = iter(pipeline)
            idx: int = 0
            while not self.is_exit_signal():
                try:

                    start_ts: float = default_timer()

                    # get the next item from the pipeline
                    annotated_img, pipeline_artefacts = next(pipeline_iter) # type: NDArray, PipelineArtefacts_Old
                    img_produced_ts: float = default_timer()

                    frame_id: str = pipeline_artefacts.frame_metadata.frame_id

                    # convert to a packet
                    packet: PipelinePacket = _build_pipeline_packet(annotated_img, pipeline_artefacts)
                    packet_created_ts: float = default_timer()

                    cur_ts: float = default_timer()
                    to_sleep: float = (last_append_ts + self._min_timeout - cur_ts) if idx > 0 else 0.0
                    if to_sleep > 0.0:
                        time.sleep(to_sleep)

                    buffer_update_start_ts: float = default_timer()

                    # empty the buffer if not empty, then put the packet in it
                    # update_single_item_queue(packet, self._buffer)

                    # put the packet into the queue if not full
                    try:
                        self._buffer.put_nowait(packet)
                    except QueueFullException:
                        # drop the packet
                        # IMPORTANT: If the queue is full, drop the packet to prioritise throughput.
                        msg: str = f"Dropped the pipeline output packet for frame {frame_id}: the buffer is full"
                        self._logger.warning(msg)
                    buffer_update_end_ts: float = default_timer()

                    last_append_ts = default_timer()
                    if self._with_timing:
                        log_line: str = _get_pipeline_worker_timing_log_line(
                            frame_id,
                            PipelineWorkerTimes(in_pipeline=img_produced_ts - start_ts,
                                                packet_creation=packet_created_ts - img_produced_ts,
                                                sleeping=buffer_update_start_ts - packet_created_ts,
                                                buffer_update=buffer_update_end_ts - buffer_update_start_ts)
                        )
                        self._logger.debug(log_line)
                    else:
                        self._logger.debug(f"Appended to buffer: {frame_id}")
                    idx += 1
                except StopIteration as e:
                    self._logger.critical(f"Hit exception: {e}")
                    raise RuntimeError("Stream ended") from e
                except Exception as e:
                    self._logger.critical(f"Hit exception: {e}")
                    raise RuntimeError("Caught exception in stream") from e
        finally:
            # put a sentinel to signal that the processing has ended
            self._buffer.put(None)
            self._logger.debug("Put sentinel")

    def run(self):
        self._run_worker()


class PipelineWrapper:

    """
    An asynchronous context manager wrapper around the pipeline worker
    that launches it in a separate process.
    """

    def __init__(self, *,
                 buffer: PipelineQueue,
                 min_timeout: float = 0.0,
                 config_path: str):
        self.buffer: PipelineQueue = buffer
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

class StaleReferenceException(Exception):
    pass

class PipelineCache:

    """
    An implementation for a cache for instances of `PipelinePacket`
    that stores at most `max_len` items, keyed by frame ID.
    Exposes `push()`, which removes the key-value pair for the oldest added item
    if the cache is full, then adds a key-value pair for the new item.
    """

    def __init__(self, max_len: int):
        if not (isinstance(max_len, int) and max_len > 0):
            raise ValueError(f"max_len must be a positive integer, got: {max_len}")

        # frame_id -> PipelinePacket
        self._cache: OrderedDictType[str, PipelinePacket] = OrderedDict()
        self._latest_key: str | None = None
        self._max_len: int = max_len
        # a threading lock to use whenever updating the cache
        self._cache_lock: Lock = Lock()

    def push(self, item: PipelinePacket) -> None:
        """
        If the cache has reached the max length, discard the oldest key-value pair.
        Put the item into the cache, with `item.artefacts.frame_metadata.frame_id` used as the key.
        """
        frame_id: str = item.artefacts.frame_metadata.frame_id
        with self._cache_lock:
            is_full: bool = len(self._cache) >= self._max_len
            if is_full:
                # discard the oldest key-value pair
                self._cache.popitem(last=False)
            # put the new item
            self._cache[frame_id] = item
            # update the latest key
            self._latest_key = frame_id

    def _get_latest(self) -> PipelinePacket:
        if self._latest_key is None:
            raise RuntimeError("Tried to access the latest item in the cache "
                               "whose _latest_key is set to None (no data yet?)")
        packet: PipelinePacket | None = self._cache.get(self._latest_key)
        if packet is None:
            raise RuntimeError(f"The value stored as the cache's latest key ({self._latest_key}) "
                               f"was not found in the cache")
        return packet

    def get_latest_artefacts(self) -> PipelineArtefacts_Old:
        latest: PipelinePacket = self._get_latest()
        return latest.artefacts

    def get_image_by_id(self, frame_id: str) -> bytes:
        packet: PipelinePacket | None = self._cache.get(frame_id)
        if packet is None:
            raise StaleReferenceException(f"Frame packet {frame_id} not found in the cache (already discarded?)")
        return packet.annotated_image


def _buffer_to_cache_worker(buffer: PipelineQueue, cache: PipelineCache) -> None:
    while True:
        # TODO: perhaps add a max timeout
        item: PipelinePacket | None = buffer.get()
        if item is None:
            print("Got a sentinel from the pipeline: buffer-to-cache worker stopped")
            return
        cache.push(item)

class AppRoutes(Routable):

    def __init__(self, cache: PipelineCache):
        super().__init__()
        self._cache: PipelineCache = cache

    @get("/latest")
    async def get_latest_state(self) -> PipelineArtefacts_Old:
        state: PipelineArtefacts_Old = self._cache.get_latest_artefacts()
        return state

    @get("/image/{frame_id}")
    async def get_image(self, frame_id: str) -> Response | None:
        try:
            image: bytes = self._cache.get_image_by_id(frame_id)
            return Response(content=image,
                            media_type="image/jpeg")
        except StaleReferenceException as e:
            raise RuntimeError(f"Could not retrieve frame {frame_id}") from e

def _get_app(buffer: PipelineQueue) -> FastAPI:

    """
    A factory for the FastAPI app.
    """

    cache: PipelineCache = PipelineCache(max_len=PIPELINE_CACHE_MAX_LEN)

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI) -> AsyncGenerator[None]:
        # TODO: change from a daemon thread to a shutdownable one
        buffer_to_cache_worker: Thread = Thread(
            target=_buffer_to_cache_worker,
            args=(buffer, cache),
            daemon=True
        )
        buffer_to_cache_worker.start()
        yield

    app: FastAPI = FastAPI(
        lifespan=lifespan
    )
    routes: AppRoutes = AppRoutes(cache)
    app.include_router(routes.router)
    return app

def _build_pipeline_wrapper(*, config_path: str,
                            buffer: PipelineQueue,
                            min_timeout: float):
    pipeline_wrapper: PipelineWrapper = PipelineWrapper(
        buffer=buffer, min_timeout=min_timeout, config_path=config_path
    )
    return pipeline_wrapper

@dataclass
class PipelineServerRunArgs:
    # TODO: incorporate min_timeout in config, remove completely later
    min_timeout: float
    config_path: str

def _get_args() -> PipelineServerRunArgs:
    return PipelineServerRunArgs(
        min_timeout=0.0,
        config_path="/src/v1/pipeline/artefacts_and_images.yaml"
    )

def run():
    run_args: PipelineServerRunArgs = _get_args()

    buffer: PipelineQueue = Queue()
    try:
        pipeline: PipelineWrapper = _build_pipeline_wrapper(
            config_path=run_args.config_path,
            buffer=buffer,
            min_timeout=run_args.min_timeout
        )
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