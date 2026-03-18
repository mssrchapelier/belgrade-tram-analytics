import asyncio
import logging
from asyncio import Future, Queue, Task, QueueShutDown
from dataclasses import dataclass, field
from logging import Logger
from typing import Self, Dict, AsyncIterator

import aiostream
from aiostream import Stream
from aiostream.core import Streamer
from pydantic import BaseModel, PositiveFloat, PositiveInt

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.models.message import WorkerJobID, WorkerOutputMessageWrapper, \
    BaseFrameJobInProgressMessage, WorkerInputMessageWrapper
from tram_analytics.v2.pipeline._base.pipeline_stage.adapters.output_adapters.mq.to_workers import \
    StageMQToWorkersOutputPort
from tram_analytics.v2.pipeline._base.pipeline_stage.processor import Processor


class ProcessorShutdownException(Exception):
    """
    An Exception to raise when `.put()` is called on a processor that is shutting down.
    """
    pass

class ProcessorTimeoutException(Exception):
    """
    To be raised when the timeout for waiting on any single result has been reached.
    Set in the future returned by `ProcessorOutputItemWithTimeout`.
    """
    pass

# TODO: refactor, remove, no need over `WorkerOutputMessageWrapper`
@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessorOutputItemWithTimeout[OutputMsgT]:
    """
    Extends `ProcessorOutputItem` to include `ProcessorTimeoutException` as a possible type of exception
    (created by `ProcessorWithTimeout` when the timeout on response from the wrapped processor has been reached).
    I. e., the possible types of exceptions that may be set to the contained future
    are `ProcessorException` (for exceptions during the processing itself)
    and `ProcessorTimeoutException` (for timeouts on processing).
    """

    job_id: WorkerJobID
    _outcome: Future[OutputMsgT] = field(default_factory=Future)

    @classmethod
    async def from_processor_result(cls, result: WorkerOutputMessageWrapper[OutputMsgT]) -> Self:
        new_item: Self = cls(job_id=result.job_id)
        try:
            output: OutputMsgT = await result.get_output()
            new_item._outcome.set_result(output)
        except Exception as exc:
            new_item._outcome.set_exception(exc)
        return new_item

    @classmethod
    def with_exception(cls, job_id: WorkerJobID, exc: Exception) -> Self:
        new_item: Self = cls(job_id=job_id)
        new_item._outcome.set_exception(exc)
        return new_item

    async def get_output(self) -> OutputMsgT:
        # will either return `OutputT`, or raise a `ProcessorTimeoutException` or any different type of exception
        return await self._outcome


class ProcessorWithTimeoutConfig(BaseModel):
    """
    Settings for `ProcessorWithTimeout`.
    """
    # How much to wait for the wrapped processor to return an output for any given input item
    # (otherwise, set the returned future's exception to a timeout exception).
    timeout: PositiveFloat | None
    # Whether to keep the outputs being emitted in the same order as the corresponding inputs arrived.
    preserve_order: bool = True
    # The maximum number of tasks (waiting for an output or a timeout for any given input item)
    # that can be run concurrently.
    max_concurrent_tasks: PositiveInt


class ProcessorWithTimeoutAndReordering[InputMsgT: BaseFrameJobInProgressMessage, OutputMsgT: BaseFrameJobInProgressMessage]:

    """
    Wraps a `Processor` to achieve the following:

    1. For every item put into the input queue, the result is awaited from the processor not forever, but with a timeout.
    If it expires, a `ProcessorOutputItemWithTimeout` is created with this frame ID,
    whose `.get_output()` will raise a `ProcessorTimeoutException`; this item will be pushed to the output queue.
    This ensures that a `Processor` that has slowed down so much that its responses exceed the timeout
    will not block the `.get()` call on the output queue.

    2. Integrates `aiostream.stream.map` to:
    - preserve the order (so that the outputs come out in the same order as inputs), if specified;
    - limit the number of concurrent requests to the wrapped processor.
    If a result is received before that, that result will be put into the output queue.

    The inputs are sent to the wrapped processor in the same order they arrive;
    whether the outputs come in the same order depends on the value of `preserve_order`
    and on the implementation of the processor.

    """

    # UPDATE: with optional reordering

    def __init__(self,
                 *,
                 mq_to_worker_adapter: StageMQToWorkersOutputPort[InputMsgT, OutputMsgT],
                 config: ProcessorWithTimeoutConfig) -> None:
        self._config: ProcessorWithTimeoutConfig = config
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._processor: Processor[InputMsgT, OutputMsgT] = Processor(
            mq_to_worker_adapter=mq_to_worker_adapter
        )
        self._in_queue: Queue[WorkerInputMessageWrapper[InputMsgT]] = Queue()
        self.out_queue: Queue[ProcessorOutputItemWithTimeout[OutputMsgT]] = Queue()

        self._job_id_to_future: Dict[WorkerJobID, Future[WorkerOutputMessageWrapper[OutputMsgT]]] = dict()

        self._is_shutting_down: bool = False

        # Tasks are spawned for each incoming item which produce into the output queue:
        # they live until either an output was received from the wrapped processor for the frame id,
        # or the timeout on getting the output for it has exceeded.

        # executor
        self._executor: AsyncIterator[ProcessorOutputItemWithTimeout[OutputMsgT]] = self._get_executor(
            ordered=self._config.preserve_order,
            task_limit=self._config.max_concurrent_tasks
        )

        # start consuming from the wrapped processor's output queue
        self._consume_outputs_and_fulfil_futures_task: Task[None] = asyncio.create_task(
            self._consume_outputs_and_fulfil_futures()
        )
        # start the output feeder (will engage the executor -> the input feeder iterators)
        self._output_feeder_task: Task[None] = asyncio.create_task(
            self._output_producer()
        )

    async def _get_executor(
            self, *, ordered: bool, task_limit: int
    ) -> AsyncIterator[ProcessorOutputItemWithTimeout[OutputMsgT]]:
        """
        - For any item that it can consume, creates a request immediately.
        - Maintains at most `task_limit` alive tasks.
        - Crucially, if `ordered` is `True`, maintains the order of the outputs
        so that it corresponds to that of the inputs;
        if any responses come out of order, buffers them until it can yield them.
        - Yields those results that are ready (i. e. whose turn has come, if `ordered`)
        to the output feeder.
        """
        stream: Stream[ProcessorOutputItemWithTimeout[OutputMsgT]] = aiostream.stream.map(
            # consume from the input queue
            self._input_consumer(),
            # the coroutine to call for each item
            func=self._process_item_for_map,
            # whether to preserve order
            ordered=ordered,
            # the maximum number of concurrent tasks
            task_limit=task_limit
        )
        async with stream.stream() as streamer:  # type: Streamer[ProcessorOutputItemWithTimeout[OutputMsgT]]
            async for output in streamer: # type: ProcessorOutputItemWithTimeout[OutputMsgT]
                yield output

    async def shutdown(self) -> None:
        """
        Initiate the shutdown of this processor. Will wait for all inputs already consumed to be processed
        (including their resulting in a timeout exception),
        but not for any results from the underlying processor that arrive after their timeout.
        """
        if self._is_shutting_down:
            self._logger.warning("ProcessorWithTimeoutAndReordering already shutting down")
            return
        self._is_shutting_down = True
        self._logger.info("ProcessorWithTimeoutAndReordering shutdown started")
        # shut down the input queue
        self._in_queue.shutdown()
        # wait until all input tasks have completed (either in a successful result or in a timeout)
        self._logger.debug("ProcessorWithTimeoutAndReordering waiting for all input tasks to complete ...")
        await self._in_queue.join()
        # wait until all items have been consumed from the output queue
        self._logger.debug("ProcessorWithTimeoutAndReordering: input tasks completed, waiting for all outputs to be consumed ...")
        await self.out_queue.join()
        self._logger.info("ProcessorWithTimeoutAndReordering: shutdown completed")

    def put(self, item: WorkerInputMessageWrapper[InputMsgT]) -> None:
        """
        Put an item into the input queue.
        """
        if self._is_shutting_down:
            raise ProcessorShutdownException()
        self._in_queue.put_nowait(item)

    async def _input_consumer(self) -> AsyncIterator[WorkerInputMessageWrapper[InputMsgT]]:
        # while not (self._is_shutting_down and self._in_queue.empty()):
        while True:
            # get a new item
            try:
                item: WorkerInputMessageWrapper[InputMsgT] = await self._in_queue.get()
                self._logger.debug(f"ProcessorWithTimeoutAndReordering: input consumer pulled job {item.job_id}, "
                                   f"(frame: {item.inputs_msg.frame_id}, session end: {item.inputs_msg.is_session_end})")
                yield item
            except QueueShutDown:
                self._logger.debug("ProcessorWithTimeoutAndReordering: input consumer exiting")
                break

    async def _output_producer(self) -> None:
        """
        Get outputs from executor (until it is exhausted on shutdown after processing all items)
        and put them into the output queue.
        The executor returns already reordered results as soon as possible if reordering;
        otherwise, it just returns results one by one as soon as they become available.
        """
        async for output_item in self._executor: # type: ProcessorOutputItemWithTimeout[OutputMsgT]
            # put the output item into the output queue
            await self.out_queue.put(output_item)
            self._logger.debug(f"ProcessorWithTimeoutAndReordering: output producer put item for frame: "
                               f"{output_item.job_id}")
            # acknowledge the completion of one item
            self._in_queue.task_done()
        self.out_queue.shutdown()
        self._logger.debug("ProcessorWithTimeoutAndReordering: output producer exiting")

    async def _process_item(
            self, input_item: WorkerInputMessageWrapper[InputMsgT]
    ) -> ProcessorOutputItemWithTimeout[OutputMsgT]:
        """
        Process one item.

        Multiple simultaneous calls to this coroutine (up to the limit specified) are made by the executor as tasks.
        The input item is sent to the wrapped processor for processing,
        but a timeout on getting the outputs for the same frame id is maintained;
        if it is exceeded, the output item with a timeout exception is created and passed instead.

        The executor performs reordering (if specified) of results returned by these calls,
        to maintain the order in which the inputs arrived.
        """
        job_id: WorkerJobID = input_item.job_id
        self._register_timeout_future(job_id)
        # forward the item to the wrapped processor
        await self._processor.put_input_msg(input_item)
        # wait until the wrapped processor either returns the output or times out on this item
        output_item: ProcessorOutputItemWithTimeout[OutputMsgT] = await self._wait_for_output_with_timeout(job_id)
        # discard the future from the buffer
        self._job_id_to_future.pop(job_id)
        return output_item

    async def _process_item_for_map(
            self, input_item: WorkerInputMessageWrapper[InputMsgT],
            *_: WorkerInputMessageWrapper[InputMsgT]
    ) -> ProcessorOutputItemWithTimeout[OutputMsgT]:
        # Wrapper `_process_item` to match the expected signature of func in `aiostream.stream.map`
        # (otherwise the type checker keeps issuing a warning despite correct types)
        return await self._process_item(input_item)

    def _register_timeout_future(self, job_id: WorkerJobID) -> None:
        # Create a future to be fulfilled:
        # - either when an output item with the same frame id is received -- with the result set;
        # - or when the timeout has exceeded -- with a `ProcessorTimeoutException` set.
        future: Future[WorkerOutputMessageWrapper[OutputMsgT]] = Future()
        # store the future in the dict so that the underlying processor's output queue consumer
        # can find and resolve it (if before timeout)
        self._job_id_to_future[job_id] = future

    async def _wait_for_output_with_timeout(self, job_id: WorkerJobID) -> ProcessorOutputItemWithTimeout[OutputMsgT]:
        # retrieve the future for this frame id
        future: Future[WorkerOutputMessageWrapper[OutputMsgT]] = self._job_id_to_future[job_id]
        async with asyncio.timeout(self._config.timeout):
            try:
                self._logger.debug(f"-- Waiting for output for job: {job_id} (timeout: {self._config.timeout:.2f} s)")
                # wait for an output for this frame id from the underlying processor
                result: WorkerOutputMessageWrapper[OutputMsgT] = await future
                # success (success or an exception from inside the underlying processor, but not a timeout):
                # wrap into `ProcessorOutputItemWithTimeout`
                output_item: ProcessorOutputItemWithTimeout[OutputMsgT] = await (
                    ProcessorOutputItemWithTimeout.from_processor_result(result)
                )
            except TimeoutError:
                self._logger.debug(f"-- Timed out waiting for job from worker server: {job_id}")
                # create a custom timeout exception (for easier differentiation in the calling code
                # inside a try-except block)
                processor_timeout_exc: ProcessorTimeoutException = ProcessorTimeoutException()
                # create an output item and set its exception
                output_item = (
                    ProcessorOutputItemWithTimeout.with_exception(job_id=job_id,
                                                                  exc=processor_timeout_exc)
                )
            except Exception as exc:
                self._logger.debug(f"-- Got an exception for job from worker server: {job_id}")
                # rewrap and pass
                output_item = (
                    ProcessorOutputItemWithTimeout.with_exception(job_id=job_id,
                                                                  exc=exc)
                )
            finally:
                return output_item

    async def _consume_outputs_and_fulfil_futures(self) -> None:
        # while not (self._is_shutting_down and self._processor.out_queue.empty()):
        while True:
            try:
                # consume an output from the underlying processor's output queue
                output_item: WorkerOutputMessageWrapper[OutputMsgT] = await self._processor.out_queue.get()
                job_id: WorkerJobID = output_item.job_id
                if job_id in self._job_id_to_future:
                    # means it has not timed out yet:
                    # get the future for this frame id and set its result to the retrieved output item
                    future: Future[WorkerOutputMessageWrapper[OutputMsgT]] = self._job_id_to_future[job_id]
                    future.set_result(output_item)
                # (otherwise, it has timed out -- simply discard it)
            except QueueShutDown:
                self._logger.debug("ProcessorWithTimeoutAndReordering: output queue shut down, "
                                   "_consume_outputs_and_fulfil_futures exiting")
                break

