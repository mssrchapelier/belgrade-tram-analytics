from typing import AsyncIterator, Set, Dict, NamedTuple, Literal, Awaitable, Any, Tuple
from abc import ABC, abstractmethod
import asyncio
from asyncio import Queue, Future, Event, Task, TaskGroup
from logging import Logger

from pydantic import BaseModel, PositiveInt, PositiveFloat
import aiostream
from aiostream.core import Stream, Streamer

class ReadyResultInfo[InputT, OutputT](NamedTuple):
    """
    A container for data related to a processing outcome ready to be handled:
    the inputs and the received future.
    """
    # the original inputs
    inputs: InputT
    # the future with either the response or the exception caught set
    result_future: Future[OutputT]

class StageState[InputT, OutputT]:

    """
    Stores the mutable state for the pipeline stage
    (async events, data for tracking requests in progress and results ready,
    access methods and guardrails).
    """

    def __init__(self, logger: Logger) -> None:
        self._logger: Logger = logger

        # Whether the input feeder iterator is allowed to consume new items from the input queue.
        self._is_consuming_inputs: Event = Event()
        # (this one is also needed to wait upon in the input feeder)
        self._is_not_consuming_inputs: Event = Event()
        # unset at init time (set when `start` is called)
        self._is_consuming_inputs.clear()
        # Whether the pipeline is in the process of stopping.
        # Used to prevent starting it again until it has stopped.
        self._is_stopping: Event = Event()
        # Whether the pipeline stage has received the signal to permanently shut down.
        # Signals the input and output feeder iterators to permanently stop.
        # Once set, intended to not be unset (thus the getter and setter).
        self.is_shut_down: Event = Event()

        # The following are managed by `_get_response_and_mark_as_ready()`
        # in the pipeline stage:

        # the sequence number to be assigned to the next consumed item
        self.next_seq_num: int = 0
        # request tasks currently processed: seq_num -> input
        self.requests_in_progress: Dict[int, InputT] = dict()
        # request tasks completed, responses waiting to be enqueued
        # seq_num -> (input, future containing output or exception)
        self.results_ready: Dict[
            int, ReadyResultInfo[InputT, OutputT]
        ] = dict()
        # Whether there are no requests currently in progress.
        self.no_requests_in_progress: Event = Event()

    @property
    def num_requests_in_progress(self) -> int:
        return len(self.requests_in_progress)

    @property
    def num_results_ready(self) -> int:
        return len(self.results_ready)

    # --- guardrails around consuming flags ---
    # because both flags are present and needed,
    # guard the setting logic here (so that one of the events
    # is not accidentally toggled without toggling the other one)

    # (1) allow reading state as a boolean

    @property
    def is_consuming_inputs(self) -> bool:
        return self._is_consuming_inputs.is_set()

    # (2) correct toggling of the state: toggle both events

    def turn_on_consuming_inputs(self) -> None:
        # idempotent
        self._is_consuming_inputs.set()
        self._is_not_consuming_inputs.clear()

    def _turn_off_consuming_inputs(self) -> None:
        # idempotent
        self._is_consuming_inputs.clear()
        self._is_not_consuming_inputs.set()

    # (3) return awaitables from both events without making the individual events accessible
    # do not convert these into subroutines --
    # return the awaitables so that they can be wrapped into tasks
    async def wait_until_turned_on_consuming_inputs(self) -> Literal[True]:
        return await self._is_consuming_inputs.wait()

    async def wait_until_turned_off_consuming_inputs(self) -> Literal[True]:
        return await self._is_not_consuming_inputs.wait()

    def initiate_stopping(self) -> None:
        if not self.is_consuming_inputs:
            # cannot initiate if not consuming
            self._logger.warning("Tried to initiate stopping, but is currently not consuming inputs")
            return
        if self._is_stopping.is_set():
            # already stopping; idempotent
            self._logger.warning("Tried to initiate stopping, but is already in the process of stopping")
            return
        self._turn_off_consuming_inputs()
        self._is_stopping.set()

    @property
    def is_stopping(self) -> bool:
        return self._is_stopping.is_set()

    def mark_stopping_as_completed(self) -> None:
        if not self._is_stopping.is_set():
            raise RuntimeError("Cannot mark stopping as completed: is not currently stopping")
        if self.is_consuming_inputs:
            raise RuntimeError("Trying to mark stopping as completed but is currently set to be consuming inputs")
        self._is_stopping.clear()

    def get_msg_for_requests_in_progress(self) -> str:
        # list requests in progress (sequence numbers, inputs)
        # output as a string for the logger
        msg: str = " ; ".join(
            f"{seq_num}: {input_item}"
            for seq_num, input_item in self.requests_in_progress.items()
        )
        return msg

    def get_msg_for_ready_results(self) -> str:
        # list results that are ready (seq nums, inputs)
        msg: str = " ; ".join(
            f"{seq_num}: {result_info.inputs}"
            for seq_num, result_info in self.results_ready.items()
        )
        return msg

class PipelineStageConfig(BaseModel):
    """
    Config for `BasePipelineStage`.
    """

    # How much to wait for the completion of any individual request.
    # On timeout, an `asyncio.Future` with exception set to `TimeoutError`
    # is emitted to be handled in the way defined in the pipeline stage subclass.
    #
    # It is important to set this to a reasonable value for real-time systems to limit latency,
    # and for any situations where any item may be processed for so long
    # that the buffer holding already completed requests that were issued later can overflow.
    #
    # If getting results for *every* item is more important instead, set this to `None`.

    timeout: PositiveFloat | None

    # The maximum number of requests that can be in progress in parallel.
    # This also limits the speed of reading from the input queue,
    # because items are only consumed if more requests can immediately be made for them.
    max_concurrent_requests: PositiveInt

class BasePipelineStage[
    InputT, OutputT, ConfigT: PipelineStageConfig
](ABC):

    """
    A pipeline stage that continuously consumes items
    from the input queue and pushes results to the output queue,
    parallelising processing but preserving the order of outputs.

    Any exceptions raised during the processing of any individual item
    are dealt with inside the stage (logging etc.), with the item being dropped.

    This implementation is essentially a wrapper around `aiostream.stream.map`
    that adds startable/stoppable reading from/writing to `asyncio.Queue` instances,
    a timeout on the execution of the coroutine per each item,
    a mechanism to catch and handle exceptions on a per-item basis without crashing the pipeline stage,
    and some wrapping around the coroutine called for each item to enable exception handling
    and request/result tracking.

    **Use cases**

    Meant to be used with requests that can be processed in parallel by processors,
    where it is not critical that every request will be processed successfully
    (this implementation handles exceptions caught during the processing
    of any request on an individual basis, then simply drops the item),
    but where it IS critical **that responses do not come out of order**
    (perhaps because downstream stages depend on the order being sequential).

    For example, in video stream processing: with object detection/segmentation
    or at any stage of processing where the video frames are treated *independently*,
    this implementation can be used to ensure the parallel processing of frames by such a processor,
    allowing for dropping of some of the frames due to exceptions (timeouts or otherwise),
    whilst ensuring the chronological ordering of the outputs (detections, etc.)

    Additionally, the timeout serves as both an indirect means to handle backpressure (see below)
    and, for real-time systems, a (more direct) means to ensure that stale requests are discarded promptly.

    **Usage**

        - `.start()`: start consuming from the input queue (or consume, if previously stopped);
        - `.stop()`: graceful pause: stop consuming from the input queue, finish processing requests
          already in progress normally, and push results to the output queue;
        - `.shutdown()`: same as `.stop()`, but causes all tasks to complete and return,
          ensuring a graceful exit.

    **Notes**

    1. While the timeout may be set to `None`, in many cases where any individual input
    may trigger abnormally long processing, it is important to set a reasonable value
    for the timeout in the configuration provided; otherwise, the buffer used to hold outputs
    that have been received early but are waiting for their turn to be sent may overflow.

    2. `.task_done()` is called on the input queue after the processing of every item
    (whether it resulted in a response or an exception), so `.join()` on the input queue
    can be awaited by the calling code.

    **Implementation**

    Subclasses must implement the following methods defined here as abstract:
        - `._call_server_endpoint(...)`: the coroutine to be called for each item
          (e. g. a server request endpoint);
        - `._handle_successful_output(...)`: the coroutine to be called for every successful response
          immediately before pushing it to the output queue;
        - `._handle_output_exception(...)`: the coroutine to be called for the exception caught
          when processing any individual item.
    """

    def __init__(self,
                 *, in_queue: Queue[InputT],
                 out_queue: Queue[OutputT],
                 config: ConfigT,
                 logger: Logger) -> None:
        # the internal state of this object
        # (event flags with their access methods, request and result storage)
        self._state: StageState[InputT, OutputT] = StageState(logger)

        # the queue from which to consume
        self._in_queue: Queue[InputT] = in_queue
        # the queue to which to produce
        self._out_queue: Queue[OutputT] = out_queue
        self._config: ConfigT = config
        self._logger: Logger = logger

        # THE MAIN COMPONENT: The executor stream
        # An async iterator consuming from the input feeder
        # (itself an async iterator consuming from the input queue; see details in the method).
        self._executor: AsyncIterator[int] = self._get_request_executor()

        # Start consuming results from the executor stream and processing them
        # (handling outputs and per-item exceptions, pushing the outputs to the output queue).
        # Exits on its own once the executor stream iterator reaches its end
        # (on shutdown, after emitting all remaining results).
        self._output_feeder_task: Task[None] = asyncio.create_task(
            self._output_feeder()
        )

    async def _get_request_executor(self) -> AsyncIterator[int]:
        # - For any item that it can consume, creates a request immediately
        # (nested wrappers underneath which this is a call to `_call_server_endpoint`).
        # - Maintains at most `max_concurrent_requests` alive tasks.
        # - Crucially, maintains the order of the outputs so that it corresponds to that of the inputs;
        # if any responses come out of order, buffers them until it can yield them.
        # - Yields the sequence numbers of the results that are ready to be output to the output feeder.
        #
        # IMPORTANT: In the config, `timeout` must be set to a reasonable value.
        # Requests are issued inside `asyncio.Timeout` contexts.
        # TIMEOUTS ARE IMPORTANT in this setting because otherwise a request
        # for which neither a successful response is received nor an exception is caught
        # for a sufficiently long time, will cause the internal buffer of the stream to OVERFLOW.
        #
        # The internal state of this executor is not directly accessed in this application.
        # Instead, request call wrappers enable tracking
        # which requests are in progress and which have been completed,
        # in this instance's `_state`. See `StageState` for details.
        #
        # The request call wrappers likewise cause either the successful response
        # or any exception caught during the processing of any individual item
        # to be set to an `asyncio.Future`, and it is the future that is returned
        # and later handled from inside the output feeder.
        # This enables straightforward iteration on the executor stream,
        # as well as proper handling of exceptions for every item separately.
        stream: Stream[int] = aiostream.stream.map(
            # consume from async iterator set to consume from the input queue
            self._input_feeder(),
            # configure to call the outer endpoint wrapper for each item
            func=self._process_request_and_add_to_ready,
            # ensure that results come out in the same order as that of the inputs
            ordered=True,
            # limit concurrent requests
            task_limit=self._config.max_concurrent_requests
        )
        async with stream.stream() as streamer:  # type: Streamer[int]
            async for seq_num in streamer: # type: int
                yield seq_num

    @abstractmethod
    async def _call_server_endpoint(self, input_item: InputT) -> OutputT:
        """
        Wrapper around the call to the server's endpoint
        to get results for an individual item consumed.
        """
        pass

    async def _get_server_response_with_timeout(self, input_item: InputT) -> OutputT:
        """
        Wrapper around `_call_server_endpoint` to throw a `TimeoutError`
        if the specified timeout was exceeded.
        """
        async with asyncio.timeout(self._config.timeout):
            return await self._call_server_endpoint(input_item)

    async def _get_server_response_as_future(self, input_item: InputT) -> Future[OutputT]:
        """
        Wrapper around `_get_server_response_with_timeout`:
        get the response returned or the exception caught,
        attach to an `asyncio.Future` and return.

        Rationale: Any exceptions caught when processing a single item should not stop
        the iteration of the executor stream, and should be dealt with
        on an individual, per-item, basis.
        """
        msg_for_input: str = f"Input: {input_item}"

        future: Future[OutputT] = asyncio.get_running_loop().create_future()
        try:
            self._logger.debug(f"Input: {input_item} | Sending request ...")
            response: OutputT = await self._get_server_response_with_timeout(input_item)
            future.set_result(response)
            self._logger.debug(f"{msg_for_input} | Result ready: Success: {response}")
        except BaseException as exc:
            future.set_exception(exc)
            exc_msg: str = f"Result ready: Exception caught - {type(exc).__name__} (message: {exc})"
            self._logger.debug(f"{msg_for_input} | {exc_msg}",
                               # exc_info=True
                               )
        finally:
            return future

    def _register_new_request(self, input_item: InputT) -> int:
        # store as request task in progress
        seq_num: int = self._state.next_seq_num
        # increment the sequence number for next caller
        self._state.next_seq_num += 1

        self._state.requests_in_progress[seq_num] = input_item
        self._state.no_requests_in_progress.clear()

        return seq_num

    def _move_request_from_progress_to_ready(self, seq_num: int, result_future: Future[OutputT]) -> None:
        # deregister as task in progress
        input_item: InputT = self._state.requests_in_progress.pop(seq_num)
        # register as a ready result
        result_info: ReadyResultInfo[InputT, OutputT] = ReadyResultInfo(inputs=input_item,
                                                                        result_future=result_future)
        self._state.results_ready[seq_num] = result_info

        if self._state.num_requests_in_progress == 0:
            # set the flag, in case the stopping coroutine is waiting for it to be set
            self._logger.debug("No requests are in progress; flag set")
            self._state.no_requests_in_progress.set()

    async def _process_request_and_add_to_ready(self, input_item: InputT) -> int:
        """
        Wrapper around `_get_server_response_as_future`:
        - register as request in progress;
        - send the request;
        - get a future with the response/exception set
          (AFTER the response has been received or exception raised);
        - deregister as in progress, register as a result ready;
        - return the sequence number of the result (by which to retrieve inputs/outputs later).
        """
        seq_num: int = self._register_new_request(input_item)
        # wait for the request to complete
        result: Future[OutputT] = await self._get_server_response_as_future(input_item)
        self._move_request_from_progress_to_ready(seq_num, result)
        # return the result received
        return seq_num

    async def _listen_on_control_flags_before_attempting_consuming(self) -> Tuple[bool, bool]:
        # wait until either:
        # (1) this feeder is allowed to consume inputs
        turned_on_consuming_inputs_task: Task[Literal[True]] = asyncio.create_task(
            self._state.wait_until_turned_on_consuming_inputs()
        )
        # (2) the feeders have been signalled to shut down
        is_shut_down_task: Task[Literal[True]] = asyncio.create_task(
            self._state.is_shut_down.wait()
        )
        tasks: Set[Task[Literal[True]]] = {turned_on_consuming_inputs_task, is_shut_down_task}
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]

        is_consuming: bool = turned_on_consuming_inputs_task in done
        is_shut_down: bool = is_shut_down_task in done

        return is_consuming, is_shut_down

    async def _listen_on_control_flags_while_consuming(self) -> Tuple[bool, bool]:
        # ... listen on stop-consuming/shutdown signals in the case they come first
        turned_off_consuming_inputs_task: Task[Literal[True]] = asyncio.create_task(
            self._state.wait_until_turned_off_consuming_inputs()
        )
        is_shut_down_task: Task[Literal[True]] = asyncio.create_task(
            self._state.is_shut_down.wait()
        )
        tasks: Set[Task[Any]] = {turned_off_consuming_inputs_task,
                                 is_shut_down_task}
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]

        is_not_consuming: bool = turned_off_consuming_inputs_task in done
        is_shut_down: bool = is_shut_down_task in done

        return is_not_consuming, is_shut_down

    async def _input_feeder(self) -> AsyncIterator[InputT]:
        """
        Consumes and yields items from the input queue.
        """

        # The executor stream is an async iterator awaiting the next item from inside this while loop.
        # What this loop listens for:
        # (1) As long the consuming flags are set so that consuming is turned off,
        # it will be stopped if was not stopped, and only be resumed until it is turned on.
        # (2) The shutdown event. Will cause breaking in this loop.
        # WORKFLOW on shutdown:
        # Breaking causes `StopAsyncIteration` to be raised,
        # which in turn stops consumption in the executor stream.
        # As soon as the executor stream has produced all output items,
        # it, in turn, raises `StopAsyncIteration`,
        # which causes the output feeder to break its while loop and return.

        while True:

            # Somewhat unwieldy, possibly refactor.
            # However, robust event monitoring here is essential;
            # otherwise, if a stop-consuming/shutdown event is set whilst this coroutine
            # is awaiting input from the queue, it will consume an EXTRA ITEM,
            # and if no more input is expected to be coming, it will BLOCK.

            # wait until either:
            # (1) this feeder is allowed to consume inputs (if currently allowed, the check will resolve immediately)
            # (2) the feeders have been signalled to shut down
            self._logger.debug("Input feeder waiting for control events before attempting consuming ...")
            is_consuming, is_shut_down = await self._listen_on_control_flags_before_attempting_consuming()
            if is_shut_down:
                # is shut down; return immediately
                self._logger.debug("Input feeder got a shutdown signal and is exiting")
                break

            assert is_consuming
            assert self._state.is_consuming_inputs

            # try consuming the next item: listen on the queue...
            got_next_item_task: Task[InputT] = asyncio.create_task(self._in_queue.get())
            # ... BUT also on stop-consuming/shutdown signals in the case they come first
            control_events_task: Task[Tuple[bool, bool]] = asyncio.create_task(
                self._listen_on_control_flags_while_consuming()
            )
            tasks: Set[Task[Any]] = {control_events_task, got_next_item_task}
            self._logger.debug("Input feeder waiting for control events while attempting consuming ...")
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )  # type: Set[Task[Any]], Set[Task[Any]]

            if control_events_task in done:
                is_not_consuming, is_shut_down = await control_events_task
                if is_shut_down:
                    # shut down -> complete
                    self._logger.debug("Input feeder got a shutdown signal and is exiting")
                    break
                assert is_not_consuming
                # jump to next cycle;
                # it will then be listening to start-consuming / shutdown
                self._logger.debug("Input feeder was signalled to not consume")
                continue

            assert got_next_item_task in done
            # consume the next item -- should return immediately
            item: InputT = await got_next_item_task
            # pass to the executor
            self._logger.debug(f"Input feeder got item {item}, passing to executor")
            yield item

    async def _output_feeder(self) -> None:
        """
        A background task that consumes the sequence numbers for ready results
        from the executor stream, retrieves the inputs and the futures for outputs,
        unpacks the futures, handles results/exceptions (in the manner defined by the subclass),
        and pushes successfully retrieved results to the output queue.

        Launched at initialisation; returns on shutdown.
        """

        while True:
            try:
                # wait for the next sequence number from the executor stream
                seq_num: int = await anext(self._executor)
                # get inputs, outputs stored under that sequence number
                result_info: ReadyResultInfo[InputT, OutputT] = self._state.results_ready[seq_num]
                input_item: InputT = result_info.inputs
                future: Future[OutputT] = result_info.result_future
                try:
                    # unwrap the future
                    output: OutputT = await future
                    # (a) output (without any exception):
                    # - perform actions on the output
                    await self._handle_successful_output(input_item=input_item,
                                                         output_item=output)
                    # - push to the output queue
                    await self._out_queue.put(output)
                except BaseException as exc:
                    # (b) exception for this item: handle it
                    await self._handle_output_exception(input_item=input_item,
                                                        exc=exc)
                finally:
                    # for any future received, signal that one item has been processed
                    self._in_queue.task_done()
                    # remove from buffer
                    self._state.results_ready.pop(seq_num)
            except StopAsyncIteration:
                # No more items in the executor stream.
                # This is intended to happen only in the case of shutdown
                assert self._state.is_shut_down.is_set()
                # complete
                self._logger.info("Pipeline stage shut down")
                break

    @abstractmethod
    async def _handle_successful_output(self, *, input_item: InputT, output_item: OutputT) -> None:
        """
        Perform any actions regarding the result before putting it into the output queue
        and moving on to the next available one.
        """
        pass

    @abstractmethod
    async def _handle_output_exception(self, *, input_item: InputT, exc: BaseException) -> None:
        """
        A method that handles the exception raised when processing an item.
        Note that until this method returns, further output items that are ready will not be processed;
        possibly create and manage background tasks for logging, writing to the database etc. here.
        """
        pass

    def start(self) -> None:
        """
        Start consuming items from the input queue.
        """
        if self._state.is_shut_down.is_set():
            raise RuntimeError("Cannot start the pipeline stage: has been permanently shut down")
        if self._state.is_consuming_inputs:
            self._logger.warning("Called `.start()` but this pipeline stage is already started")
            return
        # signal the executor feeder to start consuming items
        self._state.turn_on_consuming_inputs()
        self._logger.info("Started pipeline stage")

    async def stop(self) -> None:
        """
        Stop consuming items from the input queue.
        Any items already consumed will continue to be processed and pushed to the output queue.
        Returns when all items already consumed have been processed
        and results for them put into the output queue
        (implemented through awaiting `.join()` on the input queue).
        """
        if not self._state.is_consuming_inputs:
            self._logger.warning("Called `.stop()` but this pipeline stage is already stopped")
            return
        self._logger.info("Stopping pipeline stage ...")
        # signal the executor feeder to stop consuming items
        self._state.initiate_stopping()
        # wait until there are no more pending requests
        # NOTE: there may be results waiting in buffer; the algorithm will be continued on next start
        await self._state.no_requests_in_progress.wait()
        self._logger.info("Stopped pipeline stage")
        self._state.mark_stopping_as_completed()

    async def shutdown(self) -> None:
        """
        Signal shutdown. The input feeder will stop iteration, then the executor stream
        will complete and also stop iteration, upon which the output feeder task will complete.
        """
        if self._state.is_shut_down.is_set():
            self._logger.warning("Called `.shutdown()` but this pipeline stage has already been shut down")
            return
        # set the shutdown flag (will cause the input feeder to stop iteration and trigger shutdown thereby)
        self._logger.info("Initiating shutdown for the pipeline stage ...")
        self._state.is_shut_down.set()
        await self._state.no_requests_in_progress.wait()
        self._logger.info("Pipeline stage shut down")