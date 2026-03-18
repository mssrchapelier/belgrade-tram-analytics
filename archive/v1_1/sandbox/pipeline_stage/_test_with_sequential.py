from typing import override, List
from logging import Logger
import asyncio
from asyncio import Queue, Task, Lock

from numpy import int64
from numpy.typing import NDArray
from numpy.random import Generator, default_rng
import zmq
from zmq.asyncio import Context as AsyncContext, Socket as AsyncSocket

from archive.v1_1.pipeline.stages.base.pipeline_stage import PipelineStageConfig, BasePipelineStage
from archive.v1_1.sandbox.pipeline_stage._test import _get_logger, _consume_out_queue
from archive.v1_1.sandbox.pipeline_stage.mock_sequential_server import serialise_int, deserialise_int

class MockAsyncSequentialClient:

    def __init__(self, *, server_address: str, logger: Logger) -> None:
        self._zmq_context: AsyncContext = AsyncContext()
        self._socket: AsyncSocket = self._zmq_context.socket(socket_type=zmq.REQ)
        self._socket.connect(server_address)
        self._lock: Lock = Lock()
        self._logger: Logger = logger

    def cleanup(self) -> None:
        self._socket.close()
        self._zmq_context.destroy()

    async def update(self, inputs: int) -> int:
        inputs_bytes: bytes = serialise_int(inputs)
        async with self._lock:
            self._logger.debug(f"Client sending: {inputs}")
            await self._socket.send(inputs_bytes)
            result_bytes: bytes = await self._socket.recv()
        result: int = deserialise_int(result_bytes)
        self._logger.debug(f"Client received: {result}")
        return result

class MockPipelineStage(
    BasePipelineStage[int, int, PipelineStageConfig]
):

    def __init__(self, *, in_queue: Queue[int],
                 out_queue: Queue[int],
                 config: PipelineStageConfig,
                 logger: Logger,
                 client: MockAsyncSequentialClient):
        super().__init__(in_queue=in_queue, out_queue=out_queue, config=config, logger=logger)
        self._client: MockAsyncSequentialClient = client

    @override
    async def _call_server_endpoint(self, input_item: int) -> int:
        return await self._client.update(input_item)

    @override
    async def _handle_successful_output(self, *, input_item: int, output_item: int) -> None:
        pass

    @override
    async def _handle_output_exception(self, *, input_item: int, exc: BaseException) -> None:
        raise exc

def _populate_input_queue(inputs_numpy: NDArray[int64], queue: Queue[int]) -> None:
    inputs: List[int] = inputs_numpy.tolist()
    for item in inputs: # type: int
        queue.put_nowait(item)

async def _test_pipeline_stage():
    seed: int = 187392
    config: PipelineStageConfig = PipelineStageConfig(timeout=None,
                                                      max_concurrent_requests=1)
    num_samples: int = 50
    log_out_path: str = "REDACTED/2026_02_16_mock_sequential_pipeline_log.txt"
    server_address: str = "ipc:///tmp/test_pipeline_stage"
    # server_address: str = "tcp://localhost:8090"

    logger: Logger = _get_logger(log_out_path)
    gen: Generator = default_rng(seed)
    inputs_numpy: NDArray[int64] = gen.integers(low=1, high=100, size=num_samples, dtype=int64)

    print("Inputs: {}".format(", ".join(str(num) for num in inputs_numpy.tolist())))

    in_queue: Queue[int] = Queue()
    out_queue: Queue[int] = Queue()

    _populate_input_queue(inputs_numpy, in_queue)

    async_client: MockAsyncSequentialClient = MockAsyncSequentialClient(server_address=server_address,
                                                                        logger=logger)
    pipeline_stage: MockPipelineStage = MockPipelineStage(
        in_queue=in_queue, out_queue=out_queue, config=config, logger=logger, client=async_client
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
    # logger.info("Signalling to resume")
    # pipeline_stage.start()
    # wait until all inputs have been consumed
    # await in_queue.join()
    # shut processing down
    logger.info("Signalling to shut down")
    await pipeline_stage.shutdown()
    # wait until all outputs have been consumed
    # await out_queue.join()
    # logger.info("All results consumed\n\nDone")
    # cancel the consumer task
    out_queue_consumer_task.cancel()

if __name__ == "__main__":
    asyncio.run(_test_pipeline_stage())