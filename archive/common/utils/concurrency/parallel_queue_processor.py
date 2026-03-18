from typing import Dict, Set, Tuple, Self, Final
import asyncio
from asyncio import Task, Queue, Future, Event
from abc import ABC, abstractmethod
from dataclasses import dataclass

class PipelineExecutorException(Exception):
    """
    The exception instantiated in `BasePipelineExecutor`
    when the request task resolves in an exception.
    Instantiated from the original exception caught.
    Placed in the created `ResponseContainer` as the exception.
    """
    pass

# --- request and response containers ---

@dataclass(frozen=True, slots=True, kw_only=True)
class RequestContainer[RequestPayloadT]:
    """
    A wrapper for request payloads, containing also the request ID.
    Used by `BasePipelineExecutor` as the type ob objects consumed from the input queue.

    Rationale: to allow the upstream code to easier correlate the responses
    coming through the output queue without tracking the next awaited request ID explicitly
    (even though `BasePipelineExecutor` is designed
    to return results for all requests in the same order they came).
    """
    request_id: str
    payload: RequestPayloadT

@dataclass(frozen=True, slots=True, kw_only=True)
class ResponseContainer[ResponsePayloadT]:
    """
    Used by `BasePipelineExecutor` to return responses
    whilst **re-raising any exceptions** raised during the processing of the corresponding request.

    For every request consumed, an instance of this class will be put into the output queue:
    - If the request task raised an exception: with `error` set to that exception
      and `value` set to `None`.
    - Otherwise: with `value` set to the response payload
      and `error` set to `None`.

    In the upstream code, this instance should be unpacked using `unwrap()`.
    - If the request task raised an exception, this call will raise it.
    - Otherwise, it will return the response payload.
    """

    # The ID of the request corresponding to this response.
    request_id: str

    # If the request task did not raise an exception: the returned value.
    #
    # NOTE: Use `unwrap()` to access this value rather than accessing this field directly.
    # `unwrap()` will raise any exception possibly contained in `error`,
    # which is indeed the purpose of this container.
    value: ResponsePayloadT | None

    # If the request task raised an exception: the exception.
    error: PipelineExecutorException | None

    @classmethod
    def with_value(cls, *, request_id: str, value: ResponsePayloadT) -> Self:
        """
        Factory for tasks that did not raise an exception,
        to initialise a wrapper with the **return value** set.
        """
        return cls(request_id=request_id, value=value, error=None)

    @classmethod
    def with_error(cls, *, request_id: str, err: PipelineExecutorException) -> Self:
        """
        Factory for tasks that raised an exception,
        to initialise a wrapper with the **exception** set.
        """
        return cls(request_id=request_id, value=None, error=err)

    @property
    def is_ok(self) -> bool:
        """
        Whether this container contains a returned value, as opposed to an exception.
        """
        return self.value is not None

    def unwrap(self) -> ResponsePayloadT:
        """
        Return the response contained in this container.
        Re-raises any exception that was raised by the corresponding request task.
        """
        if self.error is not None:
            # re-raise the exception raised by the task
            raise self.error
        assert self.value is not None
        # return the value returned by the task
        return self.value

# --- executor ---

@dataclass(slots=True, kw_only=True)
class BasePipelineExecutorFlags:
    """
    Holds events for `BasePipelineExecutor`.
    Direct instantiation is discouraged;
    call `get_initial_flags()` on the class to get an instance.
    """

    # whether the executor is performing work
    #
    # Note: need both events; specifically need to be able
    # to wait until `_is_not_running` is set.
    # Because of this, implementing the guardrails below.
    _is_running: Event = Event()
    _is_not_running: Event = Event()

    # whether a stop sequence has initiated
    # (this flag is handled by the executor's `stop()` method)
    initiated_stop: Event = Event()

    # Whether a sentinel has been retrieved from the input queue
    # (signalling that no more requests will be coming).
    # Set to `True` once the `None` sentinel is consumed.
    received_sentinel: Event = Event()

    # --- factory ---

    @classmethod
    def get_initial_flags(cls) -> Self:
        flags: Self = cls()
        flags._is_not_running.set()
        return flags

    # --- proper access to running state ---

    # boolean with public access to represent the running state

    # - getter
    @property
    def is_running(self) -> bool:
        return self._is_running.is_set()

    # - setter (toggles both events)
    @is_running.setter
    def is_running(self, value: bool) -> None:
        if value:
            self._is_running.set()
            self._is_not_running.clear()
        else:
            self._is_running.clear()
            self._is_not_running.set()

    # provides a way to wait for run completion
    async def wait_until_not_running(self) -> None:
        await self._is_not_running.wait()


class BasePipelineExecutor[
    RequestPayloadT, ResponsePayloadT
](ABC):

    """
    A request executor that:
        - continuously consumes items from an `asyncio.Queue()`;
        - makes concurrent calls (up to the specified limit on the number of such calls)
          to a coroutine defined in `_send_request()`, passing the consumed items as arguments;
        - and pushes the responses into the output queue whilst preserving the original order.

    **Details**

    A generic wrapper around a (potentially expensive) coroutine call
    (such as an asynchronous request sent to a server) that:

        (1) consumes requests (`RequestContainer`) from an `asyncio.Queue`
        and pushes the responses produced (or any exceptions raised),
        in containers (`ResponseContainer`), to a different `asyncio.Queue`;

        (2) maximises the number of concurrent request tasks being in progress
        at any given moment but limits them to the maximum number specified;

        (3) consumes requests from the input queue only at the rate
        needed to maximise the number of currently active request tasks
        (does not consume pre-emptively; as soon as an item is consumed, the request is sent);

        (4) for the output queue, preserves the original order;
        i. e. for any requests `request_N` and `request_M`
        and their corresponding responses `response_N` and `response_M`,
        if `request_N` preceded `request_M` in the input queue,
        then `response_N` will precede `response_M` in the output queue.
        A buffer of outputs that are ready to be enqueued,
        and the next expected output index, are maintained for that purpose.

    A request timeout can optionally be supplied, to be applied to every request.

    **Usage**

    Two primary methods are provided to start/stop (gracefully) the workflow:

        - `start()` causes the executor to start consuming from the input queue
          and performing the workflow.
        - `stop()` causes the executor to stop consuming from the input queue,
          wait for all already running request tasks to complete,
          and push the response containers into the output queue.
          `stop()` is a coroutine that returns only when the work described above
          has been completed.

    **Type parameters** (items consumed/produced)

    The type of items **consumed** from the input queue is:

    (a) `RequestContainer`, a wrapper containing the request payload itself along with a request ID; or

    (b) `None`, used as a ***sentinel*** to signal that no more items will be coming through the input queue
    (`run()` will return after consuming `None`, completing all pending tasks,
    and pushing all results to the output queue).

    The type of items **pushed** to the output queue is `ResponseContainer`,
    which wraps either the response value (if the request task has completed successfully)
    or the exception (if one was raised by the task), along with the request ID.

    **Any exception whatsoever raised during the processing of a request task
    is chained with a** `PipelineExecutorException` and the latter **is placed in the output container**
    to be re-raised when its `unwrap()` method is called.
    The rationale for this is that it is more proper for the calling code
    to deal with any exceptions on a per-request basis, not for this executor.

    This executor, `RequestContainer`, and `ResponseContainer` are parametrised
    over the type of expected request payloads (`RequestPayloadT`) and response payloads (`ResponsePayloadT`).

    **Subclassing**

    Any concrete subclasses must implement `_send_request()` (defined as an abstract method here)
    to wrap any call to an asynchronous coroutine that actually performs the work
    (for example, an asynchronous endpoint of a server).
    The wrapper must accept `RequestPayloadT` as the sole argument, and return `ResponsePayloadT`.
    Any exceptions raised will be placed in the response container to be re-raised in the calling code
    (see `ResponseContainer` for details).
    """

    def __init__(self, *, in_queue: Queue[RequestContainer[RequestPayloadT] | None],
                 out_queue: Queue[ResponseContainer[ResponsePayloadT]],
                 max_concurrent_requests: int | None,
                 request_timeout: float | None):
        """
        Initialises this executor (but does not start it;
        call `start()` to start consuming from the input queue).

        :param in_queue:
          The queue (`asyncio.Queue`) from which to consume request containers.
          For any item consumed, **a request task is created and started immediately**.
        :param out_queue:
          The queue (`asyncio.Queue`) into which to put response containers.
        :param max_concurrent_requests:
          The maximum number of request tasks that can be running simultaneously
          (must be a positive number).
          **No items are being consumed from the input queue**
          as long as this number of request tasks are in progress.
          `None` to allow creation of an unlimited number of tasks (not recommended).
        :param request_timeout:
          A timeout to set for every request task. Must be `None`
          (for no timeout) or a positive float.
        """

        # Note: NOT providing a default null for `request_timeout`,
        # for subclasses to be required to provide this parameter explicitly
        # in the superclass constructor call

        self._validate_max_requests(max_concurrent_requests)
        self._validate_request_timeout(request_timeout)

        self._in_queue: Queue[RequestContainer[RequestPayloadT] | None] = in_queue
        self._out_queue: Queue[ResponseContainer[ResponsePayloadT]] = out_queue
        self._max_concurrent_requests: Final[int | None] = max_concurrent_requests
        self._request_timeout: Final[float | None] = request_timeout

        # the index to assign to the next request
        self._next_input_idx: int = 0
        # tasks return: (request_idx, result)
        self._pending_tasks: Set[
            Task[Tuple[int, ResponseContainer[ResponsePayloadT]]]
        ] = set()
        # request_idx -> result to be pushed to the output queue
        self._buffered_results: Dict[
            int, ResponseContainer[ResponsePayloadT]
        ] = dict()
        # the next index to enqueue from the buffer to the output queue
        self._awaited_output_idx: int = 0

        # (internal, numerical) request index -> request ID
        self._request_idx_to_id: Dict[int, str] = dict()

        # event flags
        self._event_flags: BasePipelineExecutorFlags = (
            BasePipelineExecutorFlags.get_initial_flags()
        )

    # --- request handler ---

    # subclasses must implement THIS method
    # this is the coroutine around which, in essence, this object wraps
    @abstractmethod
    async def _send_request(self, request: RequestPayloadT) -> ResponsePayloadT:
        """
        A wrapper around the endpoint call to be made for the payload in every consumed item,
        to retrieve the response payload.

        This is the primary method around calls to which this class is a wrapper
        and which the subclasses must define.
        """
        pass

    # --- start/stop methods ---

    # start: function, synchronous (simply launch start and return immediately)
    def start(self) -> None:
        """
        Start consuming from the input queue and performing the primary workflow.
        """
        if self._event_flags.initiated_stop.is_set():
            # no starts until stopping is completed
            print("Already initiated stop, cannot start again before completing the stop")
            return
        if self._event_flags.is_running:
            # ensure idempotency
            print("Already running")
            return

        asyncio.create_task(self._run())
        print("Start on command activated. The executor is running...")

    # stop: coroutine
    #       (initiate stopping, then yield control to the event loop,
    #       but do not return until the instance IS indeed stopped)
    async def stop(self) -> None:
        """
        Stop consuming from the input queue, finish all pending request tasks,
        and push response containers into the output queue
        (returns after this has been completed).
        """
        print("Stop command received")
        if self._event_flags.initiated_stop.is_set():
            # ensure idempotency
            print("Already initiated stop")
            return

        # set the flag
        self._event_flags.initiated_stop.set()
        print("Stop on command initiated...")
        # wait for the workflow to complete on already consumed items
        await self._event_flags.wait_until_not_running()

        # unset the flag
        self._event_flags.initiated_stop.clear()
        print("Stop on command completed")

    # --- validation helpers ---

    @staticmethod
    def _validate_max_requests(max_concurrent_requests: int | None) -> None:
        if not (
                max_concurrent_requests is None
                or (isinstance(max_concurrent_requests, int) and max_concurrent_requests > 0)
        ):
            raise ValueError("max_concurrent_requests must be a positive integer or None")

    @staticmethod
    def _validate_request_timeout(request_timeout: float | None) -> None:
        if request_timeout is not None and request_timeout <= 0.0:
            raise ValueError("request_timeout must be positive or None")

    # --- state access helpers ---

    def _register_request_id(self, *, idx: int, request_id: str) -> None:
        if idx in self._request_idx_to_id:
            raise ValueError(
                f"Can't register request ID {request_id} under index {idx}: the index is already registered"
            )
        self._request_idx_to_id[idx] = request_id

    def _get_request_id(self, request_idx: int) -> str:
        if request_idx not in self._request_idx_to_id:
            raise ValueError(
                f"Can't get request ID for index {request_idx}: the index is not registered"
            )
        return self._request_idx_to_id[request_idx]

    def _deregister_request_id_for_idx(self, request_idx: int) -> None:
        if request_idx not in self._request_idx_to_id:
            raise ValueError(
                f"Can't get request ID for index {request_idx}: the index is not registered"
            )
        self._request_idx_to_id.pop(request_idx)

    def _add_result_to_buffer(
            self, *, request_idx: int, result: ResponseContainer[ResponsePayloadT]
    ) -> None:
        # Dict[int, ResponseContainer[ResponsePayloadT]]
        if request_idx in self._buffered_results:
            raise ValueError(f"Can't add result for request index {request_idx} to buffer: "
                             f"the index is already registered")
        self._buffered_results[request_idx] = result

    def _pop_result_from_buffer(self, request_idx: int) -> ResponseContainer[ResponsePayloadT]:
        if request_idx not in self._buffered_results:
            raise ValueError(f"Cannot remove and return result for request index {request_idx} from buffer: "
                             f"the index is not registered")
        # pop and return
        result: ResponseContainer[ResponsePayloadT] = self._buffered_results.pop(request_idx)
        return result

    # --- request call wrappers ---

    async def _send_request_with_timeout(
            self, request_payload: RequestPayloadT
    ) -> ResponsePayloadT:
        # timeout wrapper around the primary coroutine
        async with asyncio.timeout(self._request_timeout):
            # wait for the response with the set timeout
            response_payload: ResponsePayloadT = await self._send_request(request_payload)
        return response_payload

    async def _get_response_or_executor_exception(
            self, request_payload: RequestPayloadT
    ) -> ResponsePayloadT:
        """
        A wrapper around `_send_request_with_timeout` that re-raises any exceptions caught
        as instances of `PipelineExecutorException`.
        """
        try:
            response_payload: ResponsePayloadT = await self._send_request_with_timeout(request_payload)
            return response_payload
        except BaseException as e:
            # re-raise, chaining with the exception caught
            # TODO: possibly add a more informative exception message
            # (although the chained exception does preserve the traceback)
            raise PipelineExecutorException("Error when processing request") from e

    async def _get_response_container_with_index(
            self, *, request_idx: int, request_payload: RequestPayloadT
    ) -> Tuple[int, ResponseContainer[ResponsePayloadT]]:
        """
        A wrapper around server call preserving the request index
        (simply accepts one with the request payload, and returns it with the response payload).
        Catches a successful response as well as any exception,
        and wraps either in a `ResponseContainer` to be returned along with the request index.
        """
        request_id: str = self._get_request_id(request_idx)
        try:
            # wait for the response with the set timeout
            response_payload: ResponsePayloadT = await self._get_response_or_executor_exception(request_payload)
            # success: return a container with the response set as value
            value_container: ResponseContainer[ResponsePayloadT] = ResponseContainer.with_value(
                request_id=request_id, value=response_payload
            )
            return request_idx, value_container
        except PipelineExecutorException as exc:
            # encountered an exception: return a container with the exception set
            # (to be re-raised by the upstream code on unwrapping)
            err_container: ResponseContainer[ResponsePayloadT] = ResponseContainer.with_error(
                request_id=request_id, err=exc
            )
            return request_idx, err_container

    # --- MAIN WORKFLOW COMPONENTS ---

    async def _consume_and_create_request_tasks(self) -> None:
        """
        Consume items from the input queue and create request tasks until:
        (1) either there are no more items in the input queue,
        (2) or the maximum number of concurrent tasks has been reached,
        (3) or a stop request has been received.
        """

        while (
                # a stop has not been requested
                not self._event_flags.initiated_stop.is_set()
                # a sentinel has not been received
                and not self._event_flags.received_sentinel.is_set()
                # max number of running request tasks has not been reached
                and (self._max_concurrent_requests is not None
                     and len(self._pending_tasks) < self._max_concurrent_requests)
        ):
            # consume the next request
            request: RequestContainer[RequestPayloadT] | None = await self._in_queue.get()
            if request is None:
                # encountered sentinel --> ingestion completed
                self._event_flags.received_sentinel.set()
                # call task_done for the sentinel as well
                # (for non-sentinel items, called only after the response has been put into the output queue)
                self._in_queue.task_done()
                break
            # update the next request index
            request_idx: int = self._next_input_idx
            self._next_input_idx += 1
            # map index to ID
            self._register_request_id(idx=request_idx,
                                      request_id=request.request_id)
            # create a request task, add to pending
            task: Task[Tuple[int, ResponseContainer[ResponsePayloadT]]] = asyncio.create_task(
                self._get_response_container_with_index(request_idx=request_idx,
                                                        request_payload=request.payload)
            )
            self._pending_tasks.add(task)

    async def _wait_for_next_results_and_buffer(self) -> None:
        """
        Wait for the next batch of results from the currently pending requests,
        remove these requests from pending, and buffer their results
        under the corresponding request indices.
        """
        # wait for the next pending task(s) to complete

        done_tasks: Set[Task[Tuple[int, ResponseContainer[ResponsePayloadT]]]]
        pending_tasks: Set[Task[Tuple[int, ResponseContainer[ResponsePayloadT]]]]

        done_tasks, pending_tasks = await asyncio.wait(
            self._pending_tasks,
            return_when=asyncio.FIRST_COMPLETED
        )

        # update the set of pending tasks (assign the new set)
        self._pending_tasks = pending_tasks

        # get request indices and results from the tasks done,
        # and store them in the buffer to enqueue
        for task in done_tasks: # type: Task[Tuple[int, ResponseContainer[ResponsePayloadT]]]
            request_idx, response_container = await task # type: int, ResponseContainer[ResponsePayloadT]
            self._add_result_to_buffer(request_idx=request_idx,
                                       result=response_container)

    async def _enqueue_results_if_indices_reached_turn(self) -> None:
        """
        For the results currently buffered, if the awaited output index has been reached,
        send to the output queue the next contiguous sequence of results
        whilst updating the next awaited output index accordingly.
        Examples:
            (1) Example 1
                * The current awaited output index is `235`.
                * The buffered results are for request indices: `237, 238, 240, 241`.
                * --> Nothing is enqueued.
                * --> The awaited index remains the same.
            (2) Example 2
                * The current awaited output index is likewise `235`.
                * The buffered results are for request indices: `236, 237, 238, 240 and 242`.
                * --> `[236, 237, 238]` are enqueued.
                * --> The new awaited index becomes `239`.
                * --> `[240, 242]` remain in the buffer.
        """
        while self._awaited_output_idx in self._buffered_results:
            # awaited output index encountered in buffered results
            # - get and remove from the buffer
            result: ResponseContainer[ResponsePayloadT] = self._pop_result_from_buffer(self._awaited_output_idx)
            # - push to the output queue
            await self._out_queue.put(result)
            # - signal the completion of work on one item from the input queue
            #   (in the case a `join()` on that queue is being awaited)
            self._in_queue.task_done()
            # - discard the request ID from the index -> ID map
            self._deregister_request_id_for_idx(self._awaited_output_idx)
            # - increment the awaited output index
            self._awaited_output_idx += 1

    # --- workflow runner ---
    # (do not call directly from outside this class;
    # call `start()` and `stop()` instead)

    async def _run(self) -> None:
        """
        Start consuming items from the input queue and pushing results into the output queue.

        Stops when a `None` sentinel is consumed from the input queue
        and as soon as all outstanding request tasks have completed
        and the results pushed into the output queue.

        Note:
            The calling code should call `start()` and `stop()` on this instance
            rather than `_run()`.
        """
        if self._event_flags.is_running:
            print("Already running")
            return

        try:
            self._event_flags._is_not_running.clear()
            self._event_flags._is_running.set()
            # while:
            # (a) a sentinel has not been received from the input queue, OR
            # (b) there still are pending request tasks
            while not self._event_flags.received_sentinel.is_set() or len(self._pending_tasks) > 0:
                # consume from the input queue and create tasks
                # for as many items as possible (until either a sentinel has been consumed,
                # or the max number of concurrent requests has been reached)
                await self._consume_and_create_request_tasks()
                # no more pending tasks --> algorithm completed; exit
                if len(self._pending_tasks) == 0:
                    break
                # wait for the next response(s) to come from the server and buffer it (them)
                await self._wait_for_next_results_and_buffer()
                # send as many results to the output queue as is possible at this moment,
                # preserving the original order
                await self._enqueue_results_if_indices_reached_turn()

            # on completion, put a sentinel into the output queue
            # (BEHAVIOUR DISABLED)
            # await self._out_queue.put(None)
        finally:
            print("Processor stopped")
            self._event_flags.is_running = False
