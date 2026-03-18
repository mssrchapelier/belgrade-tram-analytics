from typing import Dict, NamedTuple, Self, AsyncIterator
from abc import ABC, abstractmethod
import asyncio
from asyncio import Queue, Future, Task
import logging
from logging import Logger
from dataclasses import dataclass, field

from pydantic import BaseModel, PositiveFloat, PositiveInt
import aiostream
from aiostream.core import Stream, Streamer

from tram_analytics.v2.pipeline._base.models.message import BaseFrameJobInProgressMessage, MessageWithAckFuture
from src.v1_2.pipeline._base.workers.adapters.data_adapters import BaseDataRetrievalAdapter, BaseDataPersistenceAdapter


class ProcessorException(Exception):
    """
    An Exception to be set in the future returned by `BaseProcessor`
    in the case of any error raised whilst processing the respective input item.
    """

    # NOTE: Raise from the original exception to chain it,
    # for the exception handler to get information about the original exception
    # whilst catching based on this class
    pass

class ProcessorShutdownException(Exception):
    """
    An Exception to raise when `.put()` is called on a processor that is shutting down.
    """
    pass

class ProcessorInputItem[InputT](NamedTuple):
    frame_id: str
    inputs: InputT

@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessorOutputItem[OutputT]:
    """
    A wrapper for processor outputs that holds:
    - the frame ID (for cross-reference);
    - a future containing either the output or a `ProcessorException`
    raised during the processing of this item.
    Moivation:
    (1) To allow for exception handling on a per-item basis.
    (2) To enable cross-referencing the frame IDs of the outputs,
    especially because whether the outputs preserve the order of the inputs
    depends on the processor's implementation.
    """

    frame_id: str
    _outcome: Future[OutputT] = field(default_factory=Future)

    def set_result(self, result: OutputT) -> None:
        self._outcome.set_result(result)

    def set_exception(self, exc: ProcessorException) -> None:
        # type safety guard: allow setting the outcome's exception
        # only to an instance of `ProcessorException`
        self._outcome.set_exception(exc)

    async def get_output(self) -> OutputT:
        # will either return `OutputT`, or raise a `ProcessorException`
        return await self._outcome

class ProcessorTimeoutException(Exception):
    """
    To be raised when the timeout for waiting on any single result has been reached.
    Set in the future returned by `ProcessorOutputItemWithTimeout`.
    """
    pass

@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessorOutputItemWithTimeout[OutputT]:
    """
    Extends `ProcessorOutputItem` to include `ProcessorTimeoutException` as a possible type of exception
    (created by `ProcessorWithTimeout` when the timeout on response from the wrapped processor has been reached).
    I. e., the possible types of exceptions that may be set to the contained future
    are `ProcessorException` (for exceptions during the processing itself)
    and `ProcessorTimeoutException` (for timeouts on processing).
    """

    frame_id: str
    _outcome: Future[OutputT] = field(default_factory=Future)

    @classmethod
    async def from_processor_result(cls, result: ProcessorOutputItem[OutputT]) -> Self:
        new_item: Self = cls(frame_id=result.frame_id)
        try:
            output: OutputT = await result.get_output()
            new_item._outcome.set_result(output)
        except ProcessorException as exc:
            new_item._outcome.set_exception(exc)
        return new_item

    @classmethod
    def with_exception(cls, frame_id: str, exc: ProcessorTimeoutException) -> Self:
        new_item: Self = cls(frame_id=frame_id)
        new_item._outcome.set_exception(exc)
        return new_item

    async def get_output(self) -> OutputT:
        # will either return `OutputT`, or raise a `ProcessorException` or a `ProcessorTimeoutException`
        return await self._outcome

class BaseProcessor[InputT, OutputT](ABC):

    def __init__(self) -> None:
        # (frame id -> input) to be processed
        self.in_queue: Queue[ProcessorInputItem[InputT]] = Queue()
        # (frame id -> future with output/exception) that have been processed
        self.out_queue: Queue[ProcessorOutputItem[OutputT]] = Queue()

    def put(self, item: ProcessorInputItem[InputT]) -> None:
        # Enqueue the item for processing immediately.
        self.in_queue.put_nowait(item)

class ProcessorWithTimeoutConfig(BaseModel):
    """
    Settings for `ProcessorWithTimeout`.
    """
    logger_name: str
    # How much to wait for the wrapped processor to return an output for any given input item
    # (otherwise, set the returned future's exception to a timeout exception).
    timeout: PositiveFloat | None
    # Whether to keep the outputs being emitted in the same order as the corresponding inputs arrived.
    preserve_order: bool
    # The maximum number of tasks (waiting for an output or a timeout for any given input item)
    # that can be run concurrently.
    max_concurrent_tasks: PositiveInt


class ProcessorWithTimeout[InputT, OutputT]:

    """
    Wraps a `BaseProcessor` to achieve the following:

    1. For every item put into the input queue, the result is awaited from the processor not forever, but with a timeout.
    If it expires, a `ProcessorOutputItemWithTimeout` is created with this frame ID,
    whose `.get_output()` will raise a `ProcessorTimeoutException`; this item will be pushed to the output queue.
    This ensures that a `BaseProcessor` that has slowed down so much that its responses exceed the timeout
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

    def __init__(self, processor: BaseProcessor[InputT, OutputT],
                 *, config: ProcessorWithTimeoutConfig) -> None:
        self._config: ProcessorWithTimeoutConfig = config
        self._logger: Logger = logging.getLogger(self._config.logger_name)

        self._processor: BaseProcessor[InputT, OutputT] = processor
        self._in_queue: Queue[ProcessorInputItem[InputT]] = Queue()
        self.out_queue: Queue[ProcessorOutputItemWithTimeout[OutputT]] = Queue()

        self._frame_id_to_future: Dict[str, Future[ProcessorOutputItem[OutputT]]] = dict()

        self._is_shutting_down: bool = False

        # Tasks are spawned for each incoming item which produce into the output queue:
        # they live until either an output was received from the wrapped processor for the frame id,
        # or the timeout on getting the output for it has exceeded.

        # executor
        self._executor: AsyncIterator[ProcessorOutputItemWithTimeout[OutputT]] = self._get_executor(
            ordered=self._config.preserve_order,
            task_limit=self._config.max_concurrent_tasks
        )

        # start consuming from the wrapped processor's output queue
        self._consume_outputs_and_fulfil_futures_task: Task[None] = asyncio.create_task(
            self._consume_outputs_and_fulfil_futures()
        )
        # start the output feeder (will engage the executor -> the input feeder iterators)
        self._output_feeder_task: Task[None] = asyncio.create_task(
            self._output_feeder()
        )

    async def _get_executor(
            self, *, ordered: bool, task_limit: int
    ) -> AsyncIterator[ProcessorOutputItemWithTimeout[OutputT]]:
        """
        - For any item that it can consume, creates a request immediately.
        - Maintains at most `task_limit` alive tasks.
        - Crucially, if `ordered` is `True`, maintains the order of the outputs
        so that it corresponds to that of the inputs;
        if any responses come out of order, buffers them until it can yield them.
        - Yields those results that are ready (i. e. whose turn has come, if `ordered`)
        to the output feeder.
        """
        stream: Stream[ProcessorOutputItemWithTimeout[OutputT]] = aiostream.stream.map(
            # consume from the input queue
            self._input_feeder(),
            # the coroutine to call for each item
            func=self._process_item,
            # whether to preserve order
            ordered=ordered,
            # the maximum number of concurrent tasks
            task_limit=task_limit
        )
        async with stream.stream() as streamer:  # type: Streamer[ProcessorOutputItemWithTimeout[OutputT]]
            async for output in streamer: # type: ProcessorOutputItemWithTimeout[OutputT]
                yield output

    async def shutdown(self) -> None:
        """
        Initiate the shutdown of this processor. Will wait for all inputs already consumed to be processed
        (including their resulting in a timeout exception),
        but not for any results from the underlying processor that arrive after their timeout.
        """
        if self._is_shutting_down:
            self._logger.warning("ProcessorWithTimeout already shutting down")
            return
        self._is_shutting_down = True
        self._logger.info("ProcessorWithTimeout shutdown started")
        # shut down the input queue
        self._in_queue.shutdown()
        # wait until all input tasks have completed (either in a successful result or in a timeout)
        self._logger.debug("ProcessorWithTimeout waiting for all input tasks to complete ...")
        await self._in_queue.join()
        # wait until all items have been consumed from the output queue
        self._logger.debug("ProcessorWithTimeout: input tasks completed, waiting for all outputs to be consumed ...")
        await self.out_queue.join()
        self._logger.info("ProcessorWithTimeout shutdown completed")

    def put(self, item: ProcessorInputItem[InputT]) -> None:
        """
        Put an item into the input queue.
        """
        if self._is_shutting_down:
            raise ProcessorShutdownException()
        self._in_queue.put_nowait(item)

    async def _input_feeder(self) -> AsyncIterator[ProcessorInputItem[InputT]]:
        while not (self._is_shutting_down and self._in_queue.empty()):
            # get a new item
            item: ProcessorInputItem[InputT] = await self._in_queue.get()
            yield item

    async def _output_feeder(self) -> None:
        """
        Get outputs from executor (until it is exhausted on shutdown after processing all items)
        and put them into the output queue.
        The executor returns already reordered results as soon as possible if reordering;
        otherwise, it just returns results one by one as soon as they become available.
        """
        async for output_item in self._executor: # type: ProcessorOutputItemWithTimeout[OutputT]
            # put the output item into the output queue
            await self.out_queue.put(output_item)
            # acknowledge the completion of one item
            self._in_queue.task_done()

    async def _process_item(self, input_item: ProcessorInputItem[InputT]) -> ProcessorOutputItemWithTimeout[OutputT]:
        """
        Process one item.

        Multiple simultaneous calls to this coroutine (up to the limit specified) are made by the executor as tasks.
        The input item is sent to the wrapped processor for processing,
        but a timeout on getting the outputs for the same frame id is maintained;
        if it is exceeded, the output item with a timeout exception is created and passed instead.

        The executor performs reordering (if specified) of results returned by these calls,
        to maintain the order in which the inputs arrived.
        """
        frame_id: str = input_item.frame_id
        self._register_timeout_future(frame_id)
        # forward the item to the wrapped processor
        self._processor.put(input_item)
        # wait until the wrapped processor either returns the output or times out on this item
        output_item: ProcessorOutputItemWithTimeout[OutputT] = await self._wait_for_output_with_timeout(frame_id)
        # discard the future from the buffer
        self._frame_id_to_future.pop(frame_id)
        return output_item

    def _register_timeout_future(self, frame_id: str) -> None:
        # Create a future to be fulfilled:
        # - either when an output item with the same frame id is received -- with the result set;
        # - or when the timeout has exceeded -- with a `ProcessorTimeoutException` set.
        future: Future[ProcessorOutputItem[OutputT]] = Future()
        # store the future in the dict so that the underlying processor's output queue consumer
        # can find and resolve it (if before timeout)
        self._frame_id_to_future[frame_id] = future

    async def _wait_for_output_with_timeout(self, frame_id: str) -> ProcessorOutputItemWithTimeout[OutputT]:
        # retrieve the future for this frame id
        future: Future[ProcessorOutputItem[OutputT]] = self._frame_id_to_future[frame_id]
        async with asyncio.timeout(self._config.timeout):
            try:
                # wait for an output for this frame id from the underlying processor
                result: ProcessorOutputItem[OutputT] = await future
                # success (success or an exception from inside the underlying processor, but not a timeout):
                # wrap into `ProcessorOutputItemWithTimeout`
                output_item: ProcessorOutputItemWithTimeout[OutputT] = await (
                    ProcessorOutputItemWithTimeout.from_processor_result(result)
                )
            except TimeoutError:
                # create a custom timeout exception (for easier differentiation in the calling code
                # inside a try-except block)
                processor_timeout_exc: ProcessorTimeoutException = ProcessorTimeoutException()
                # create an output item and set its exception
                output_item = (
                    ProcessorOutputItemWithTimeout.with_exception(frame_id=frame_id,
                                                                  exc=processor_timeout_exc)
                )
            finally:
                return output_item

    async def _consume_outputs_and_fulfil_futures(self) -> None:
        while not (self._is_shutting_down and self._processor.out_queue.empty()):
            # consume an output from the underlying processor's output queue
            output_item: ProcessorOutputItem[OutputT] = await self._processor.out_queue.get()
            frame_id: str = output_item.frame_id
            if frame_id in self._frame_id_to_future:
                # means it has not timed out yet:
                # get the future for this frame id and set its result to the retrieved output item
                future: Future[ProcessorOutputItem[OutputT]] = self._frame_id_to_future[frame_id]
                future.set_result(output_item)
            # (otherwise, it has timed out -- simply discard it)


class BaseOutputMessageQueue[OutputMsgT](ABC):

    def __init__(self) -> None:
        # messages ready to be published to the message queue
        self._in_queue: Queue[OutputMsgT] = Queue()

    def enqueue_for_publishing(self, message: OutputMsgT) -> None:
        """
        Enqueue a message to be published to the message queue broker.
        """
        # No max size, no locks around the queue, so put_nowait should not cause issues.
        self._in_queue.put_nowait(message)

class PipelineStageConfig(BaseModel):
    # The logger name for this stage.
    logger_name: str
    # How long to wait for inputs.
    # With a set timeout, a timeout will result in the frame being skipped by this stage.
    input_prefetch_timeout: PositiveFloat | None
    # How long to wait for the processor to process each individual input (once forwarded to the processor).
    # processing_timeout: PositiveFloat | None
    # How long to wait for the outputs to have been stored successfully.
    # Likewise, a timeout will result in the frame being skipped.
    output_persist_timeout: PositiveFloat | None
    processing: ProcessorWithTimeoutConfig

class PipelineStageShutdownException(Exception):
    # To be raised when attempting to call `on_receive` on a pipeline stage instance
    # that is shutting down or has been shut down.
    pass

class BasePipelineStage[
    InputMsgT: BaseFrameJobInProgressMessage, OutputMsgT: BaseFrameJobInProgressMessage,
    InputT, OutputT,
    ConfigT: PipelineStageConfig
](ABC):

    """
    A pipeline stage that is triggered by messages from one message queue broker adapter
    (the one into which it is injected as a dependency)
    and pushes messages to be published to a different message queue broker adapter.
    - Maintains the sequential order of inputs and drops out-of-order input items
    (the provided idempotency handles the broker's restarts and consequently any re-deliveries).
    - Is injected with the processor containing the actual processing logic;
    this may be parallelisable (detection, feature extraction) or strictly sequential
    (tracking, vehicle info, scene state).
    """

    class OutputPersistenceTimeout(Exception):
        # a separate class to differentiate the cases conveniently in `.retrieve_from_processor()`
        pass

    def __init__(self,
                 *, config: ConfigT,
                 input_file_storage: BaseDataRetrievalAdapter[InputT],
                 output_file_storage: BaseDataPersistenceAdapter[OutputT],
                 processor: BaseProcessor[InputT, OutputT],
                 output_msq_queue_producer: BaseOutputMessageQueue[OutputMsgT]) -> None:
        self._config: ConfigT = config
        self._logger: Logger = logging.getLogger(self._config.logger_name)
        # The sequence number for the last frame seen.
        # Used for idempotency purposes; namely to prevent the processing of frames already processed earlier
        # if input messages with their sequence number arrives again,
        # which may happen if the broker goes down and then re-delivers messages that had been accepted
        # but not acknowledged before it went down.
        #
        # NOTE: It is for the input message queue broker adapter
        # to ensure that messages are not attempted to be acknowledged twice (!!!) --
        # this is the implementation detail of the adapter, not of this use case.
        self._last_accepted_seq_num: int | None = None

        # A buffer to hold the items currently in progress.
        # Keyed by frame ID as the unique identifier;
        # mapped is the input message (in order to construct the output message from it)
        # and the futures to fulfil when the input message is ready to be acknowledged
        # by the calling message queue broker adapter.
        self._frames_in_progress: Dict[str, MessageWithAckFuture[InputMsgT]] = dict()
        # Tasks to fetch inputs, keyed by frame ID.
        self._prefetch_inputs_tasks: Dict[str, Task[InputT]] = dict()

        # The frame IDs accepted for processing, ready to be sent to the processor as soon as the inputs have been fetched.
        # Motivation: to ensure: (1) maintaining the order of arrival when sending inputs to the processor;
        # (2) the asynchronous fetching of the inputs at the same time.
        self._accepted_and_waiting_for_inputs: Queue[str] = Queue()

        self._input_file_storage: BaseDataRetrievalAdapter[InputT] = input_file_storage
        # self._processor: BaseProcessor[InputT, OutputT] = processor
        self._processor_with_timeout: ProcessorWithTimeout[InputT, OutputT] = (
            ProcessorWithTimeout(processor=processor,
                                 config=self._config.processing)
        )
        self._output_file_storage: BaseDataPersistenceAdapter[OutputT] = output_file_storage
        self._output_msg_queue_producer: BaseOutputMessageQueue[OutputMsgT] = output_msq_queue_producer

        self._wait_for_inputs_and_send_to_processor_task: Task[None] = asyncio.create_task(
            self._wait_for_inputs_and_send_to_processor()
        )
        self._retrieve_from_processor_task: Task[None] = asyncio.create_task(
            self._retrieve_from_processor()
        )

        # Whether this instance has been signalled to shut down.
        self._is_shutting_down: bool = False


    async def shutdown(self) -> None:
        # set the "shutting down" flag to true
        # - new calls to `on_receive` will raise a `PipelineStageShutdownException`
        # - the first part of the while condition in the loops
        #   in `._wait_for_inputs_and_send_to_processor()` and `._retrieve_from_processor()`
        #   is set to `True`, causing them to wait until `_accepted_and_waiting_for_inputs`
        #   and `_frames_in_progress` (resp.) empty out
        self._is_shutting_down = True
        # put the "accepted and waiting for inputs" queue in shutdown mode (can no longer accept inputs)
        # NOTE: not currently obligatory because nothing is awaiting a `.join()` on that queue.
        # However, if that is ever added, NOT putting the queue into shutdown mode will cause that join to block.
        # Best to leave it here to prevent a subtle bug in that case (currently this has no effect on the algorithm).
        self._accepted_and_waiting_for_inputs.shutdown()
        # wait for the feeders to and from the processor to finish (with `_is_shutting_down` set to `True`,
        # will now stop iteration once all pending items have been processed).
        await self._wait_for_inputs_and_send_to_processor_task
        await self._retrieve_from_processor_task

    def on_receive(self, input_msg_with_ack_future: MessageWithAckFuture[InputMsgT]) -> None:
        """
        The method to be called in the input message queue broker adapter to handle one incoming message.
        """
        if self._is_shutting_down:
            raise PipelineStageShutdownException()

        input_msg: InputMsgT = input_msg_with_ack_future.message
        frame_id: str = input_msg.frame_id
        session_id: int = input_msg.session_id
        seq_num: int = input_msg.seq_num

        if self._last_accepted_seq_num is not None and seq_num <= self._last_accepted_seq_num:
            # Idempotency guard:
            # This sequence number has already been accepted for processing (or skipped).
            # Signal to acknowledge the message immediately and do not process it.
            input_msg_with_ack_future.ack_future.set_result(None)
            self._logger.warning(f"Dropping frame {frame_id}: seq_num {seq_num} has already been processed or skipped "
                                 f"(last accepted seq_num: {self._last_accepted_seq_num})")
            return
        # NOTE: Sequence numbers *can* be skipped.
        # Whether they are skipped or not is the concern of any upstream modules and the message queue broker.
        # In any module, only the sequential order of the frames accepted for processing is ensured.
        # If a skipped frame arrives out of order later, it will be discarded (see immediately above).

        self._logger.debug(f"Frame accepted for processing: {frame_id}")
        # increment the last accepted sequence number
        self._last_accepted_seq_num = seq_num
        # add the input message and the acknowledgement future to the buffer
        self._frames_in_progress[frame_id] = input_msg_with_ack_future

        # create a prefetch input task, store in the buffer
        prefetch_input_task: Task[InputT] = asyncio.create_task(self._prefetch_input(frame_id))
        self._prefetch_inputs_tasks[frame_id] = prefetch_input_task

        # put the frame ID into the "accepted and ready for inputs" queue.
        # As soon as its turn comes and the inputs for it are ready, it will be sent to the processor.
        self._accepted_and_waiting_for_inputs.put_nowait(frame_id)

    async def _prefetch_input(self, frame_id: str) -> InputT:
        """
        Given the frame ID, prefetch the inputs from the input file storage (with a timeout if configured).
        """
        try:
            async with asyncio.timeout(self._config.input_prefetch_timeout):
                # May raise a TimeoutError; is to be handled during the resolution of the task
                # in which this coroutine is running.
                self._logger.debug(f"Frame waiting for inputs to be fetched: {frame_id}")
                inputs: InputT = await self._input_file_storage.retrieve(frame_id)
                self._logger.debug(f"Frame inputs fetched successfully: {frame_id}")
                return inputs
        except TimeoutError as exc:
            self._logger.debug(f"Timed out waiting for inputs for frame: {frame_id}")
            # re-raise
            raise exc

    async def _persist_output(self, *, frame_id: str, output: OutputT) -> None:
        """
        Attempt to persist the output (using the frame_id as the unique key) in the output storage;
        with a timeout if configured.
        """
        try:
            async with asyncio.timeout(self._config.output_persist_timeout):
                # May raise a TimeoutError.
                self._logger.debug(f"Attempting to persist outputs for frame: {frame_id}")
                await self._output_file_storage.store(frame_id=frame_id, output=output)
                self._logger.debug(f"Successfully persisted outputs for frame: {frame_id}")
        except TimeoutError as exc:
            self._logger.debug(f"Timed out waiting to persist outputs for frame: {frame_id}")
            # raising a custom exception (with the original one chained) to better differentiate
            # inside the try-except block from which this coroutine is called
            raise self.OutputPersistenceTimeout from exc

    async def _wait_for_inputs_and_send_to_processor(self) -> None:
        """
        A worker continually reading the next frame ID to be processed, awaiting inputs for it to be fetched,
        and sending them to the processor sequentially (if the inputs have been fetched successfully).

        Meant to be run in a separate task.

        If a timeout is set on input prefetch in the config and it expires,
        mark the input message for acknowledgement and skip the processing of this item.
        """

        # await inputs
        # send to processor
        while not (self._is_shutting_down and self._accepted_and_waiting_for_inputs.empty()):
            # get the next frame id
            frame_id: str = await self._accepted_and_waiting_for_inputs.get()
            self._logger.debug(f"Frame reached turn to be sent to processor: {frame_id}")
            try:
                # attempt to fetch inputs
                inputs: InputT = await self._input_file_storage.retrieve(frame_id)
                # pass to the processor
                self._logger.debug(f"Successfully got inputs for frame, enqueueing to processor: {frame_id}")
                self._processor_with_timeout.put(
                    ProcessorInputItem(frame_id=frame_id, inputs=inputs)
                )
            except TimeoutError:
                # timed out
                self._logger.warning(f"Frame skipped, timed out waiting for inputs: {frame_id}")
                self._mark_for_acknowledgement_and_discard_input_msg(frame_id)
            finally:
                self._accepted_and_waiting_for_inputs.task_done()

    def _mark_for_acknowledgement_and_discard_input_msg(self, frame_id: str) -> None:
        # - get the input message and the acknowledgement future (discarding them from the buffer)
        input_msg_with_ack_future: MessageWithAckFuture[InputMsgT] = self._frames_in_progress.pop(frame_id)
        # - set the result of the ack future (i. e. signal to acknowledge the input message)
        input_msg_with_ack_future.ack_future.set_result(None)
        self._logger.debug(f"Marked input message for frame: {frame_id} as ready for acknowledgement")

    async def _retrieve_from_processor(self) -> None:
        """
        A worker for retrieving the processed items from the processor,
        sending results for persistence or logging exceptions,
        and sending output messages to be published.

        Meant to be run as a separate task.
        """
        while not (self._is_shutting_down and len(self._frames_in_progress) == 0):
            # NOTE: In shutdown, will block on get unless a timeout is set on receiving the next item from the processor.
            # NOTE: Re shutdown: depending on the processor to set a timeout on any individual item.

            # wait for the processor to produce the next output
            processing_result: ProcessorOutputItemWithTimeout[OutputT] = (
                await self._processor_with_timeout.out_queue.get()
            )
            frame_id: str = processing_result.frame_id
            self._logger.debug(f"Got an outcome from processor for frame {frame_id}, unwrapping")

            try:
                output: OutputT = await processing_result.get_output()
                # attempt to store the output
                # NOTE: Waiting for the output to be persisted before sending the output message
                # because otherwise the downstream stages may attempt to fetch it before it has been persisted.
                await self._persist_output(frame_id=frame_id, output=output)
                # build the output message (using data from the input message)
                input_msg: InputMsgT = self._frames_in_progress[frame_id].message
                output_msg: OutputMsgT = self._build_output_message(input_msg)
                # push the output message to the output message queue broker
                self._output_msg_queue_producer.enqueue_for_publishing(output_msg)
                self._logger.debug(f"Created and enqueued for publishing output message for frame: {frame_id}")
            except ProcessorException:
                # log the fact that there was an exception; the details should be logged by the processor itself
                self._logger.warning(f"Processor returned an exception for frame {frame_id}; skipping frame")
            except ProcessorTimeoutException:
                self._logger.warning(f"Processor timed out processing inputs for frame {frame_id}; skipping frame")
            except self.OutputPersistenceTimeout:
                self._logger.warning(f"Timed out waiting to persist outputs for frame {frame_id}; skipping frame")
            finally:
                # in any outcome, acknowledge the input message and discard it (and the ack future) from the buffer
                self._mark_for_acknowledgement_and_discard_input_msg(frame_id)
                self._processor_with_timeout.out_queue.task_done()

    @abstractmethod
    def _build_output_message(self, input_msg: InputMsgT) -> OutputMsgT:
        pass