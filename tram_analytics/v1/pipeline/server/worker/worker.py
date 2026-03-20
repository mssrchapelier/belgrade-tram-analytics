import logging
from logging import Logger
from multiprocessing.queues import Queue as QueueType
from queue import Full as QueueFullException
from timeit import default_timer
from typing import Iterator, Tuple, override, TypeAlias, Self, NamedTuple

from numpy import uint8
from numpy.typing import NDArray
from pydantic_yaml import parse_yaml_file_as

from common.settings.constants import LOGGING_SERVER_HOST, LOGGING_SERVER_PORT
from common.utils.concurrency.mp_utils import ShutdownableProcess, stop_shutdownable
from common.utils.logging_utils.logging_utils import get_logger_name_for_object, configure_global_logging_to_tcp_socket
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.pipeline.pipeline.artefacts_streaming.config import ArtefactsStreamingPipelineConfig
from tram_analytics.v1.pipeline.server.helpers.packet import PipelinePacket, _build_pipeline_packet
from tram_analytics.v1.pipeline.server.helpers.pipeline_cache import PipelineCache

PIPELINE_WORKER_TIMEOUT_PER_JOIN: float = 10.0

PipelineQueue: TypeAlias = QueueType[PipelinePacket | None]

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
                 with_timing: bool = True,
                 logging_level: int,
                 **kwargs
                 ):

        super().__init__(**kwargs)

        self._logging_level: int = logging_level

        self._buffer: PipelineQueue = buffer
        self._config_path: str = config_path
        self._with_timing: bool = with_timing

    def _run_worker(self):
        """
        Initialise a pipeline from the config, start it, and push the produced items to the queue.

        TODO: For multiple consumers (multiple requests), implement a pub-sub (or perhaps something else).
        TODO: Implement a ring buffer in shared memory rather than limiting the number of buffered items to one.
        TODO: change to drop-head behaviour when full (prioritise recent items)
        """

        from tram_analytics.v1.pipeline.pipeline.artefacts_streaming.pipeline import ArtefactsStreamingPipeline

        logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        pipeline_config: ArtefactsStreamingPipelineConfig = parse_yaml_file_as(
            ArtefactsStreamingPipelineConfig, self._config_path
        )
        pipeline: ArtefactsStreamingPipeline = ArtefactsStreamingPipeline(pipeline_config)

        last_append_ts: float | None = None
        try:
            pipeline_iter: Iterator[Tuple[NDArray[uint8], PipelineArtefacts]] = iter(pipeline)
            idx: int = 0
            while not self.is_exit_signal():
                try:

                    start_ts: float = default_timer()

                    # get the next item from the pipeline
                    annotated_img, pipeline_artefacts = next(pipeline_iter)  # type: NDArray[uint8], PipelineArtefacts
                    img_produced_ts: float = default_timer()

                    frame_id: str = pipeline_artefacts.frame_metadata.frame_id

                    # convert to a packet
                    packet: PipelinePacket = _build_pipeline_packet(annotated_img, pipeline_artefacts)
                    packet_created_ts: float = default_timer()

                    # empty the buffer if not empty, then put the packet in it
                    # update_single_item_queue(packet, self._buffer)

                    # put the packet into the queue if not full
                    try:
                        self._buffer.put_nowait(packet)
                    except QueueFullException:
                        # drop the packet
                        # IMPORTANT: If the queue is full, drop the packet to prioritise throughput.
                        msg: str = f"Dropped the pipeline output packet for frame {frame_id}: the buffer is full"
                        logger.warning(msg)
                    buffer_update_end_ts: float = default_timer()

                    last_append_ts = default_timer()
                    if self._with_timing:
                        log_line: str = _get_pipeline_worker_timing_log_line(
                            frame_id,
                            PipelineWorkerTimes(in_pipeline=img_produced_ts - start_ts,
                                                packet_creation=packet_created_ts - img_produced_ts,
                                                buffer_update=buffer_update_end_ts - packet_created_ts)
                        )
                        logger.debug(log_line)
                    else:
                        logger.debug(f"Appended to buffer: {frame_id}")
                    idx += 1
                except StopIteration as e:
                    logger.critical(f"Hit exception: {e}")
                    raise RuntimeError("Stream ended") from e
                except Exception as e:
                    logger.critical(f"Hit exception: {e}")
                    raise RuntimeError("Caught exception in stream") from e
        finally:
            # put a sentinel to signal that the processing has ended
            self._buffer.put(None)
            logger.debug("Put sentinel")

    @override
    def run(self):
        configure_global_logging_to_tcp_socket(
            host=LOGGING_SERVER_HOST, port=LOGGING_SERVER_PORT,
            level=self._logging_level
        )
        self._run_worker()


class PipelineWorkerTimes(NamedTuple):
    in_pipeline: float
    packet_creation: float
    buffer_update: float


def _get_pipeline_worker_timing_log_line(frame_id: str, times: PipelineWorkerTimes) -> str:
    total: float = times.in_pipeline + times.packet_creation + times.buffer_update
    out: str = f"Produced frame {frame_id} in {total:.3f} s\n"
    out += " | ".join(
        f"{name}: {value:.3f}" for name, value in zip(
            ["in pipeline", "packet creation", "buffer update"],
            [times.in_pipeline, times.packet_creation, times.buffer_update]
        )
    )
    return out


class PipelineWrapper:
    """
    An asynchronous context manager wrapper around the pipeline worker
    that launches it in a separate process.
    """

    def __init__(self, *,
                 buffer: PipelineQueue,
                 config_path: str):
        # TODO: remove min_timeout

        self.buffer: PipelineQueue = buffer
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
                                                logging_level=logging.getLogger().level)
        return worker

    def start_worker(self):
        # TODO: change print calls to logging calls
        if self._worker.is_alive():
            # TODO: change to a warning (make idempotent)
            raise RuntimeError("Pipeline worker already started")
        print("Starting pipeline worker...")
        self._worker.start()
        print("Pipeline worker started")

    def stop_worker(self):
        stop_shutdownable(self._worker,
                          timeout_per_join=PIPELINE_WORKER_TIMEOUT_PER_JOIN)


def _buffer_to_cache_worker(buffer: PipelineQueue, cache: PipelineCache) -> None:
    while True:
        # TODO: perhaps add a max timeout
        item: PipelinePacket | None = buffer.get()
        if item is None:
            print("Got a sentinel from the pipeline: buffer-to-cache worker stopped")
            return
        cache.push(item)
