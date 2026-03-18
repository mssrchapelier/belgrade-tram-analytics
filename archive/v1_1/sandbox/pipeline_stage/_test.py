from typing import NamedTuple, override, List
import logging
from logging import Logger, StreamHandler, FileHandler, Formatter
import asyncio
from asyncio import Queue, Task
import sys
from _io import TextIOWrapper
import time

import numpy as np
from numpy import int64
from numpy.typing import NDArray
from numpy.random import Generator, default_rng

from archive.v1_1.pipeline.stages.base.pipeline_stage import PipelineStageConfig, BasePipelineStage

def _processing_func(x: int, y: int) -> int:
    return abs(x - y)

def _processing_func_numpy(x: NDArray[int64], y: NDArray[int64]) -> NDArray[int64]:
    return np.absolute(x - y)

class NumberPair(NamedTuple):
    x: int
    y: int

class MockAsyncServer:

    """
    Simulates a server with an async endpoint performing "work"
    (waiting a random amount of time before emitting a response)
    """

    def __init__(self, *, min_wait: float, max_wait: float,
                 seed: int) -> None:
        # simulate random processing times
        self._min_wait: float = min_wait
        self._max_wait: float = max_wait

        self._rng: Generator = default_rng(seed)

    def _get_random_processing_time(self) -> float:
        return self._rng.uniform(low=self._min_wait, high=self._max_wait)

    async def predict(self, inputs: NumberPair) -> int:
        processing_time: float = self._get_random_processing_time()
        await asyncio.sleep(processing_time)
        result: int = _processing_func(inputs.x, inputs.y)
        return result

class MockPipelineStage(
    BasePipelineStage[NumberPair, int, PipelineStageConfig]
):

    def __init__(self, *, in_queue: Queue[NumberPair],
                 out_queue: Queue[int],
                 config: PipelineStageConfig,
                 logger: Logger,
                 server: MockAsyncServer):
        super().__init__(in_queue=in_queue, out_queue=out_queue, config=config, logger=logger)
        self._server: MockAsyncServer = server

    @override
    async def _call_server_endpoint(self, input_item: NumberPair) -> int:
        return await self._server.predict(input_item)

    @override
    async def _handle_successful_output(self, *, input_item: NumberPair, output_item: int) -> None:
        pass

    @override
    async def _handle_output_exception(self, *, input_item: NumberPair, exc: BaseException) -> None:
        pass

def _print_expected_inputs_outputs(inputs_numpy: NDArray[int64], outputs_numpy: NDArray[int64]) -> None:
    inputs: List[List[int]] = inputs_numpy.tolist()
    outputs: List[int] = outputs_numpy.tolist()
    print("--- EXPECTED INPUTS -> OUTPUTS ---")
    for inputs_row, output in zip(inputs, outputs): # type: List[int], int
        input_x: int = inputs_row[0]
        input_y: int = inputs_row[1]
        print(f"({input_x}, {input_y}) -> {output}")

def _populate_input_queue(inputs_numpy: NDArray[int64], queue: Queue[NumberPair]) -> None:
    inputs: List[List[int]] = inputs_numpy.tolist()
    for input_row in inputs: # type: List[int]
        item: NumberPair = NumberPair(x=input_row[0], y=input_row[1])
        queue.put_nowait(item)

async def _consume_out_queue[T](queue: Queue[T], logger: Logger) -> None:
    while True:
        item: T = await queue.get()
        msg: str = f"Output queue consumer | Got result: {item}"
        logger.info(msg)
        queue.task_done()

def _get_logger(log_out_path: str | None = None) -> Logger:
    logger: Logger = logging.getLogger(__name__)

    # (set level to DEBUG to display more)
    logger.setLevel(logging.DEBUG)

    formatter: Formatter = Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S"
    )
    # set time to UTC
    formatter.converter = time.gmtime

    stderr_stream_handler: StreamHandler[TextIOWrapper] = StreamHandler(sys.stderr)
    stderr_stream_handler.setLevel(logging.DEBUG)
    stderr_stream_handler.setFormatter(formatter)
    logger.addHandler(stderr_stream_handler)

    if log_out_path is not None:
        file_handler: FileHandler = FileHandler(log_out_path, mode="w", encoding="utf8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.info("NOTE: All timestamps are in UTC")

    return logger

async def _test_pipeline_stage():
    seed: int = 187392
    config: PipelineStageConfig = PipelineStageConfig(timeout=2.0,
                                                      max_concurrent_requests=5)
    num_samples: int = 50
    log_out_path: str = "REDACTED/2026_02_16_mock_pipeline_log.txt"

    logger: Logger = _get_logger(log_out_path)

    gen: Generator = default_rng(seed)
    inputs_numpy: NDArray[int64] = gen.integers(low=1, high=1000, size=(num_samples, 2), dtype=int64)
    # func: the absolute value of the difference between inputs[:, 0] and inputs[:, 1]
    outputs_numpy: NDArray[int64] = _processing_func_numpy(inputs_numpy[:, 0], inputs_numpy[:, 1])

    # _print_expected_inputs_outputs(inputs_numpy, outputs_numpy)

    in_queue: Queue[NumberPair] = Queue()
    out_queue: Queue[int] = Queue()

    _populate_input_queue(inputs_numpy, in_queue)

    server: MockAsyncServer = MockAsyncServer(min_wait=0.5, max_wait=2.5, seed=seed)
    pipeline_stage: MockPipelineStage = MockPipelineStage(
        in_queue=in_queue, out_queue=out_queue, config=config, logger=logger, server=server
    )

    out_queue_consumer_task: Task[None] = asyncio.create_task(
        _consume_out_queue(out_queue, logger)
    )

    # start processing
    pipeline_stage.start()
    # wait a bit
    await asyncio.sleep(6.0)
    # stop
    await pipeline_stage.stop()
    # wait a bit while stopped
    logger.info("Waiting for 3 seconds to resume ....")
    await asyncio.sleep(3.0)
    # start again
    logger.info("Signalling to resume")
    pipeline_stage.start()
    # wait until all inputs have been consumed
    await in_queue.join()
    # shut processing down
    logger.info("Signalling to shut down")
    await pipeline_stage.shutdown()
    # wait until all outputs have been consumed
    await out_queue.join()
    logger.info("All results consumed\n\nDone")
    # cancel the consumer task
    out_queue_consumer_task.cancel()


if __name__ == "__main__":
    asyncio.run(_test_pipeline_stage())