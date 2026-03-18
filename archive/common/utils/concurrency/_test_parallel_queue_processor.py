from typing import override, List, TypeAlias
import random
from math import ceil
import asyncio
from asyncio import sleep, Queue, Task
from uuid import uuid4

from archive.common.utils.concurrency.parallel_queue_processor import (
    BasePipelineExecutor, RequestContainer, ResponseContainer, PipelineExecutorException
)

def _mock_predict_sync(n: float) -> int:
    # mock function for predict (just rounds up the number)
    return ceil(n)

class MockInferenceServer:

    def __init__(self, *, min_wait: float, max_wait: float):
        self._min_wait: float = min_wait
        self._max_wait: float = max_wait

    def _get_random_time(self) -> float:
        return self._min_wait + random.random() * (self._max_wait - self._min_wait)

    async def predict(self, n: float) -> int:
        # simulate work by sleeping
        to_sleep: float = self._get_random_time()
        await sleep(to_sleep)
        # call the mock predict function
        return _mock_predict_sync(n)

MockRequestContainer: TypeAlias = RequestContainer[float]
MockResponseContainer: TypeAlias = ResponseContainer[int]

# a mock concrete instantiation of `BasePipelineExecutor`
# (wraps requests to the mock async endpoint `MockInferenceServer.predict(...)`)

class MockPipelineExecutor(
    BasePipelineExecutor[float, int]
):

    def __init__(self, *, in_queue: Queue[MockRequestContainer | None],
                 out_queue: Queue[MockResponseContainer],
                 max_concurrent_requests: int,
                 request_timeout: float | None = None,
                 server: MockInferenceServer) -> None:
        super().__init__(in_queue=in_queue,
                         out_queue=out_queue,
                         max_concurrent_requests=max_concurrent_requests,
                         request_timeout=request_timeout)
        self._server: MockInferenceServer = server

    @override
    async def _send_request(self, request: float) -> int:
        result: int = await self._server.predict(request)
        return result

# --- helpers ---

def _create_request_container(payload: float) -> MockRequestContainer:
    request_id: str = uuid4().hex
    return RequestContainer(request_id=request_id, payload=payload)

async def _retrieve_and_print_results(q: Queue[MockResponseContainer]) -> None:
    # will not exit on its own -- cancel the task from the calling code
    while True:
        container: MockResponseContainer = await q.get()
        request_id: str = container.request_id
        request_id_section: str = f"Request ID: {request_id[:6]}.."
        try:
            value: int = container.unwrap()
            print(f"{request_id_section} | Success, value: {value}")
        except PipelineExecutorException as exc:
            # deal with the exception in whatever way deemed appropriate (traceback inspection, etc.);
            # here, just stating the fact of having received an exception
            print(f"{request_id_section} | Fail (error when processing)")
        finally:
            q.task_done()

# --- test ---

async def _run_mock_executor():
    # --- "server" ---
    # a mock server with randomised response times

    # limits on random duration to process a request
    min_processing_duration: float = 0.5
    max_processing_duration: float = 2.5
    server: MockInferenceServer = MockInferenceServer(min_wait=min_processing_duration,
                                                      max_wait=max_processing_duration)

    # --- inputs ---

    # create request payloads
    num_requests: int = 400
    # ... floats in [0.0, 100.0)
    request_payloads: List[float] = list(map(lambda idx: random.random() * 100.0,
                                             range(num_requests)))
    # compute the expected outputs (using the same sync function that the test "server" uses)
    expected_outputs: List[int] = list(map(lambda item: _mock_predict_sync(item),
                                           request_payloads))

    print("Input items:\n{}".format(", ".join(f"{item:.1f}.." for item in request_payloads)))
    print("Expected outputs:\n{}".format(", ".join(f"{item}" for item in expected_outputs)))

    # --- queues ---
    # for communication with the parallel processor

    # create queues
    src_queue: Queue[MockRequestContainer | None] = Queue()
    dest_queue: Queue[MockResponseContainer] = Queue()
    # enqueue to the input queue
    for req_payload in request_payloads: # type: float
        request_container: MockRequestContainer = _create_request_container(req_payload)
        src_queue.put_nowait(request_container)
    # put a sentinel at the end
    src_queue.put_nowait(None)

    # --- executor ---

    # set the max number of concurrent requests maintained by the parallel processor
    max_concurrent_requests: int = 30
    # set a request timeout limit
    # (here, low enough to simulate some timeouts happening,
    # mean timeout rate 25%)
    request_timeout: float = 2.0
    executor: MockPipelineExecutor = MockPipelineExecutor(
        in_queue=src_queue, out_queue=dest_queue,
        server=server,
        max_concurrent_requests=max_concurrent_requests,
        request_timeout=request_timeout
    )

    # --- output printer ---

    # start the output queue consumer
    out_queue_consumer_task: Task[None] = asyncio.create_task(
        _retrieve_and_print_results(dest_queue)
    )

    # --- simulation ---

    # simulate short bursts of work, then stopping and restarting
    num_timed_runs: int = 3
    timed_run_duration: float = 5.0
    for run_idx in range(num_timed_runs):
        # start -> run for a few seconds -> stop
        executor.start()
        await asyncio.sleep(timed_run_duration)
        await executor.stop()

    # start the executor again
    executor.start()
    # run until completion (exhaust the source queue first, then the output queue)
    await src_queue.join()
    await dest_queue.join()
    # cancel the output queue consumer task
    out_queue_consumer_task.cancel()

    print("Done")

    # executor.start()
    # await executor.stop()

if __name__ == "__main__":
    asyncio.run(_run_mock_executor())