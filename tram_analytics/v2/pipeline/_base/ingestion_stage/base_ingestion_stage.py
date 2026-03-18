import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Task, Event, Queue, QueueShutDown
from logging import Logger
from typing import AsyncIterator, NamedTuple, Literal, Tuple, Set, Any

from pydantic import BaseModel, PositiveFloat

from tram_analytics.v2.pipeline._base.common.exceptions import OutputPersistenceTimeout, \
    IngestionDroppedItemException
from tram_analytics.v2.pipeline._base.ingestion_stage.adapters.output_adapters.mq.to_pipeline import \
    BaseIngestionMQToPipeline
from tram_analytics.v2.pipeline._base.ingestion_stage.adapters.output_adapters.repository.write_repo import (
    BaseIngestionStageWriteRepo
)
from tram_analytics.v2.pipeline._base.models.message import (
    BaseIngestionDroppedItemMessage, BaseFrameJobInProgressMessage, BaseCriticalErrorMessage
)


class BaseIngestionStageConfig(BaseModel, ABC):
    # The camera (stream) ID for the pipeline to which this stage belongs.
    camera_id: str

    # The maximum duration between the current and the previous seen frame timestamps
    # above which to reset the session ID (and start sequence IDs from 1).
    # reset_session_id_after_stream_down_for: PositiveFloat | None

    # How long to wait for the outputs to have been stored successfully.
    # Likewise, a timeout will result in the frame being skipped.
    output_persist_timeout: PositiveFloat | None

    # TTL (time to live) for messages, in seconds (None for no expiration). Used in message brokers.
    message_ttl: PositiveFloat | None

class OutputPersistTaskItem[OutputMsgT](NamedTuple):
    frame_id: str
    # await this task
    # - a successful result means the item has been persisted and the message can be passed safely
    # (the downstream modules will be able to access that data)
    # - an exception means it has not and the message must be discarded
    # (a health monitoring message indicating the error possibly sent as well)
    persist_task: Task[None]
    # the message to be published in the case of successful persistence
    msg: OutputMsgT

class SessionIDSeqNum(NamedTuple):
    session_id: int
    seq_num: int

class SessionSeqNumUpdater[OutputT]:

    def __init__(self,
                 data_persistence_adapter: BaseIngestionStageWriteRepo[OutputT]):
        self._logger: Logger = logging.getLogger(self.__class__.__module__)

        self._persistence: BaseIngestionStageWriteRepo[OutputT] = data_persistence_adapter

        # None when not running; session ID requested from persistence at each start/continue
        self._next_item_identifiers: SessionIDSeqNum | None = None

    def get_current(self) -> SessionIDSeqNum | None:
        return self._next_item_identifiers

    # --- NOTE ---
    # Implementations of `BaseIngestionStage` should NOT have to call the functions below
    # (all actual resetting is managed in the base class)

    async def start_new_session(self) -> None:
        """
        Request a new session ID from persistence, reset the sequence number to 1,
        and update the values accordingly.

        Should be called when a discontinuity is determined
        that merits signalling that a new session has started.
        """
        if self._next_item_identifiers is not None:
            raise RuntimeError("Requested a session start but the old one must be reset first")
        self._logger.debug("Requesting a new session ID from persistence")
        new_session_id: int = await self._persistence.get_new_session_id()
        new_seq_num: int = 1
        self._next_item_identifiers = SessionIDSeqNum(new_session_id, new_seq_num)
        self._logger.debug(f"New session ID: {new_session_id}, new sequence number: {new_seq_num}")

    def increment_seq_num(self) -> None:
        """
        Increment the sequence number for the next frame.
        The decision to do this is made once it has been determined that the item will not be dropped,
        NOT in the item processing (evaluating) method.
        """
        # increment seq num by 1 for the next frame; called only after processing
        if self._next_item_identifiers is None:
            raise RuntimeError("Called increment_seq_num but the session ID and seq num are not defined "
                               "(no successful items produced yet?)")
        self._next_item_identifiers = SessionIDSeqNum(
            session_id=self._next_item_identifiers.session_id,
            seq_num=self._next_item_identifiers.seq_num + 1
        )

    def clear_session(self) -> None:
        """
        Reset the session ID and seq num to null. Done at stopping
        (so that resumption will trigger the request for a new session ID).
        """
        self._logger.debug("Session ID, sequence number unset (set to null)")
        self._next_item_identifiers = None


class BaseIngestionStage[
    InputT, OutputT, ConfigT: BaseIngestionStageConfig,
    JobInProgressMsgT: BaseFrameJobInProgressMessage,
    DroppedItemMsgT: BaseIngestionDroppedItemMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage
](ABC):

    def __init__(self, *, config: ConfigT,
                 read_repo: BaseIngestionStageWriteRepo[OutputT],
                 write_repo: BaseIngestionMQToPipeline[
                     JobInProgressMsgT, DroppedItemMsgT, CriticalErrMsgT
                 ]) -> None:
        self._config: ConfigT = config
        self._logger: Logger = logging.getLogger(self.__class__.__module__)

        self._data_persistence: BaseIngestionStageWriteRepo[OutputT] = read_repo
        self._mq_to_pipeline: BaseIngestionMQToPipeline[
            JobInProgressMsgT, DroppedItemMsgT, CriticalErrMsgT
        ] = write_repo
        self._src_stream: AsyncIterator[InputT] | None = None

        self._session_id_seq_num_updater: SessionSeqNumUpdater[OutputT] = SessionSeqNumUpdater(
            data_persistence_adapter=read_repo
        )

        # NOTE: started to suffix events with "event"
        # because otherwise it is easy to accidentally treat it as a boolean,
        # which will evaluate an unset event to true
        # - started, not is stopping, not stopped, not shut down
        self._is_running_event: Event = Event()
        # - started or is stopping
        # AND there are no items currently being processed or persistence tasks being sent to queue
        # (i. e. the ingestion loop is either waiting for start, or for a new item or a stop)
        # This flag is managed in the ingestion loop.
        # self._is_ingestion_and_processing_idle_event: Event = Event()
        # self._is_ingestion_and_processing_idle_event.set()
        self._is_ingestion_loop_stopped_event: Event = Event()
        self._is_ingestion_loop_stopped_event.clear()
        self._is_persistence_and_publishing_idle_event: Event = Event()
        self._is_persistence_and_publishing_idle_event.set()
        self._is_stopping_event: Event = Event()
        self._is_shutting_down_event: Event = Event()
        self._has_shut_down_event: Event = Event()

        self._output_persist_tasks: Queue[OutputPersistTaskItem[JobInProgressMsgT]] = Queue()
        self._wait_for_persisted_and_output_messages_task: Task[None] = asyncio.create_task(
            self._wait_for_persisted_and_output_messages()
        )
        self._wait_for_input_process_and_persist_task: Task[None] = asyncio.create_task(
            self._wait_for_input_process_and_persist()
        )

        # the message for the last emitted item (used to track session id, seq num, timestamp, etc.)
        self._prev_emitted_item_msg: JobInProgressMsgT | None = None

    async def start(self) -> None:
        if self._is_stopping_event.is_set():
            self._logger.warning("Requested start but is stopping; ignoring")
            return
        if self._is_shutting_down_event.is_set():
            self._logger.warning("Requested start but is already shutting down; ignoring")
            return
        if self._has_shut_down_event.is_set():
            raise RuntimeError("Can't start ingestion stage: has been shut down")
        if self._is_running_event.is_set():
            self._logger.warning("Requested start but is already started; ignoring")
            return
        self._logger.info("Starting ...")
        self._src_stream = self._get_new_source_stream()
        self._logger.debug("Initialised source stream")
        # update session id from persistence, reset seq_num if needed
        await self._session_id_seq_num_updater.start_new_session()
        self._is_running_event.set()
        self._logger.info("Started")

    @abstractmethod
    def _get_new_source_stream(self) -> AsyncIterator[InputT]:
        pass

    async def stop(self) -> None:
        if self._has_shut_down_event.is_set():
            self._logger.warning("Requested stop but has been shut down; ignoring")
            return
        if not self._is_running_event.is_set():
            self._logger.warning("Requested stop but is not running; ignoring")
            return

        self._logger.info("Stopping ...")
        self._is_running_event.clear()
        self._is_stopping_event.set()
        # await self._is_ingestion_and_processing_idle_event.wait()
        await self._is_ingestion_loop_stopped_event.wait()
        await self._is_persistence_and_publishing_idle_event.wait()
        # reset the session id
        await self._emit_session_end_message_and_clear_session()
        self._src_stream = None
        self._is_stopping_event.clear()
        self._logger.info("Stopped")

    @abstractmethod
    def _build_session_end_message_from_last_emitted(self, msg_for_last_emitted: JobInProgressMsgT) -> JobInProgressMsgT:
        pass

    async def _emit_session_end_message_and_clear_session(self) -> None:
        """
        If it is determined that the last session must be ended
        (on stop/shutdown, as well as before processing a new item if so determined):
        wait for any persistence/publishing tasks to finish,
        then emit a session end message for the LAST emitted frame (if any was emitted)
        and reset the session ID and sequence number (and remove info about the last emitted frame).
        """
        if self._prev_emitted_item_msg is not None:
            msg: JobInProgressMsgT = self._build_session_end_message_from_last_emitted(
                self._prev_emitted_item_msg
            )
            # Wait for any persistence tasks / success messages to be emitted first
            # to preserve the sequential order of items.
            # NOTE: This can introduce latency, but it should in practice be limited
            # by the timeout on persistence and on sending messages to the MQ broker.
            await self._is_persistence_and_publishing_idle_event.wait()
            await self._mq_to_pipeline.publish_for_emitted_item(msg)
            self._logger.debug(f"Sent output message for publishing for frame: {msg.frame_id} -- SESSION END")
        # reset the last emitted item message
        self._prev_emitted_item_msg = None
        self._session_id_seq_num_updater.clear_session()

    async def shutdown(self) -> None:
        if self._is_shutting_down_event.is_set():
            self._logger.warning("Requested shutdown but is already shutting down; ignoring")
            return
        if self._has_shut_down_event.is_set():
            self._logger.warning("Requested shutdown but has already been shut down; ignoring")
            return

        self._logger.info("Shutting down ...")
        self._is_shutting_down_event.set()
        # wait until the ingestion loop exits
        # await self._wait_for_input_process_and_persist_task
        await self._is_ingestion_loop_stopped_event.wait()
        # IMPORTANT: emit a session end message for the pipeline

        # shut down the persistence tasks queue
        self._output_persist_tasks.shutdown()
        # wait until the output message publishing/dropping tasks finishes
        await self._wait_for_persisted_and_output_messages_task
        # reset the session id
        await self._emit_session_end_message_and_clear_session()
        self._has_shut_down_event.set()
        self._is_shutting_down_event.clear()
        self._src_stream = None
        self._logger.info("Has shut down")

    @abstractmethod
    def _must_end_prev_session(self, *, input_item: InputT, last_success_msg: JobInProgressMsgT) -> bool:
        """
        Called before processing every item in order to first determine
        whether the session must be reset first.
        If yes, then a session end message will be emitted first
        for the last emitted frame ID (stored in `last_success_msg`),
        then this message will be processed (regardless of whether it will later be dropped).
        """
        pass

    @abstractmethod
    async def _process(self, input_item: InputT) -> OutputT:
        """
        Process the input item to produce the output to persist.
        For example, from a raw frame, create a container with the frame and metadata,
        or decide to skip as needed.

        NOTE: Whether the session ID needs to be reset first (i. e. the old session needs to be marked as having ended),
        is evaluated by the implementation of `_must_end_prev_session`. By the time the item reaches `_process`,
        the session will have been reset if necessary.

        For items that are to be dropped (not passed to the pipeline,
        but for which a message will be sent to the broker for system health state monitoring purposes),
        **raise an `IngestionDroppedItemException`**, with a descriptive message (will be logged and passed in a message).

        All other exceptions raised from here will be treated as critical (will lead to the ingestion stage stopping),
        so any logic related to discontinuities and their handling, unless unrecoverable, needs to be handled here
        (and an `IngestionDroppedItemException` raised) and/or in `_must_end_prev_session`.
        """
        # WORKFLOW:
        # - assess the item: to drop or not (temporal discontinuities in the stream etc.)
        # - if needed, increment
        # - if needed, decide to drop and raise a `IngestionDroppedItemException`
        # - if the error is critical, raise a different type of exception (will result in shutdown of ingestion)
        #
        # NOTES:
        # - `self._session_id_seq_num_updater.get_current()` to get the current session ID, seq num

        pass

    @abstractmethod
    def _create_message_success(self, output_item: OutputT) -> JobInProgressMsgT:
        pass

    @abstractmethod
    def _get_frame_id_from_message(self, msg: JobInProgressMsgT) -> str:
        pass

    @abstractmethod
    def _get_dropped_job_mq_message(self, description: str) -> DroppedItemMsgT:
        pass

    @abstractmethod
    def _get_critical_error_mq_message(self, exc: Exception) -> CriticalErrMsgT:
        pass

    async def _handle_dropped_job(self, description: str) -> None:
        # log
        self._logger.warning(f"Dropped job: {description}; dropping frame")
        # emit a message to the MQ broker (to the dedicated dropped items queue)
        mq_msg: DroppedItemMsgT = self._get_dropped_job_mq_message(description)
        await self._mq_to_pipeline.publish_for_dropped_item(mq_msg)

    async def _handle_critical_exception(self, exc: Exception) -> None:
        self._logger.critical(f"Critical exception")
        self._logger.debug(f"Critical exception:\n", exc_info=exc)
        mq_msg: CriticalErrMsgT = self._get_critical_error_mq_message(exc)
        await self._mq_to_pipeline.report_critical_error(mq_msg)

    async def _persist_output(self, *, frame_id: str, output: OutputT) -> None:
        """
        Attempt to persist the output (using the frame_id as the unique key) in the output storage;
        with a timeout if configured.
        """
        try:
            async with asyncio.timeout(self._config.output_persist_timeout):
                # May raise a TimeoutError.
                self._logger.debug(f"Attempting to persist outputs for frame {frame_id}")
                await self._data_persistence.store(output=output,
                                                   timeout=self._config.output_persist_timeout)
                self._logger.debug(f"Successfully persisted outputs for frame {frame_id}")
        except TimeoutError as exc:
            self._logger.debug(f"Timed out waiting to persist outputs for frame {frame_id}")
            raise OutputPersistenceTimeout() from exc

    async def _wait_until_resumed_or_shutting_down(self) -> Tuple[bool, bool]:
        assert not self._is_running_event.is_set()
        is_resumed_task: Task[Literal[True]] = asyncio.create_task(self._is_running_event.wait())
        is_shutting_down_task: Task[Literal[True]] = asyncio.create_task(self._is_shutting_down_event.wait())
        done, pending = await asyncio.wait(
            {is_resumed_task, is_shutting_down_task},
            return_when=asyncio.FIRST_COMPLETED
        ) # type: Set[Task[Literal[True]]], Set[Task[Literal[True]]]
        for task in pending: # type: Task[Literal[True]]
            task.cancel()
        is_resumed: bool = is_resumed_task in done
        is_shutting_down: bool = is_shutting_down_task in done
        assert any((is_resumed, is_shutting_down))
        return is_resumed, is_shutting_down

    async def _wait_for_control_events_while_consuming(self) -> Tuple[bool, bool]:
        is_stopping_task: Task[Literal[True]] = asyncio.create_task(self._is_stopping_event.wait())
        is_shutting_down_task: Task[Literal[True]] = asyncio.create_task(self._is_shutting_down_event.wait())
        done, pending = await asyncio.wait(
            {is_stopping_task, is_shutting_down_task},
            return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Literal[True]]], Set[Task[Literal[True]]]
        for task in pending: # type: Task[Literal[True]]
            task.cancel()
        is_stopping: bool = is_stopping_task in done
        is_shutting_down: bool = is_shutting_down_task in done
        assert any((is_stopping, is_shutting_down))
        return is_stopping, is_shutting_down

    async def _process_and_schedule_persistence(self, item: InputT) -> None:
        try:
            # if there is information about the last emitted item...
            if self._prev_emitted_item_msg is not None:
                # determine whether the session for that item must be ended first
                # (e. g. because of a temporal discontinuity being detected)
                must_end_prev_session: bool = self._must_end_prev_session(
                    input_item=item, last_success_msg=self._prev_emitted_item_msg
                )
                if must_end_prev_session:
                    # if yes: wait for persistence/emittance tasks to end,
                    # emit a session end message for the last stored frame,
                    # then set the reference to null
                    await self._emit_session_end_message_and_clear_session()
                    # request the new session id from persistence and set seq num to 1
                    await self._session_id_seq_num_updater.start_new_session()
            # process the item
            output_item: OutputT = await self._process(item)
            # success
            msg: JobInProgressMsgT = self._create_message_success(output_item)
            frame_id: str = self._get_frame_id_from_message(msg)
            # - increment seq num for the next item
            self._session_id_seq_num_updater.increment_seq_num()
            persist_task_item: OutputPersistTaskItem[JobInProgressMsgT] = OutputPersistTaskItem(
                frame_id=frame_id, persist_task=asyncio.create_task(
                    self._persist_output(frame_id=frame_id, output=output_item)
                ), msg=msg
            )
            await self._output_persist_tasks.put(persist_task_item)
        except IngestionDroppedItemException as exc:
            await self._handle_dropped_job(exc.description)

    async def _get_next_from_stream(self) -> InputT:
        if self._src_stream is None:
            raise RuntimeError("Source stream not initialised")
        return await anext(self._src_stream)
        # NOTE: task cancelled on stop/shutdown -- destroys the current async iterator

    async def _wait_for_input_process_and_persist(self) -> None:
        while True:
            self._is_ingestion_loop_stopped_event.set()
            if self._has_shut_down_event.is_set():
                raise RuntimeError("Can't start the ingestion/processing loop: has shut down")
            if not self._is_running_event.is_set():
                is_resumed, is_shutting_down = await self._wait_until_resumed_or_shutting_down()
                if is_shutting_down:
                    break
                assert is_resumed
            self._is_ingestion_loop_stopped_event.clear()
            # wait until is running if is not running, or until is shut down
            # if shut down: break
            # if is running: proceed
            try:
                # wait until getting the next item, or until is stopping command, or until is shut down
                # on get: proceed, on stop: continue to next iteration, on shutdown: break
                get_next_item_task: Task[InputT] = asyncio.create_task(
                    self._get_next_from_stream()
                )
                control_events_task: Task[Tuple[bool, bool]] = asyncio.create_task(
                    self._wait_for_control_events_while_consuming()
                )
                done, pending = await asyncio.wait(
                    {get_next_item_task, control_events_task},
                    return_when=asyncio.FIRST_COMPLETED
                )  # type: Set[Task[Any]], Set[Task[Any]]
                for task in pending: # type: Task[Any]
                    task.cancel()
                if control_events_task in done:
                    is_stopping: bool
                    is_stopping, is_shutting_down = await control_events_task
                    if is_stopping:
                        # will wait until resumed on the next iteration
                        continue
                    if is_shutting_down:
                        break
                else:
                    assert get_next_item_task in done
                    # not idle any more
                    # self._is_ingestion_and_processing_idle_event.clear()
                    input_item: InputT = await get_next_item_task
                    await self._process_and_schedule_persistence(input_item)
                    self._is_persistence_and_publishing_idle_event.clear()
            except StopAsyncIteration:
                # behaviour: warn, stop and break
                # self._logger.info("The stream has ended; set a new stream before restarting the ingestion stage")
                self._logger.info("The stream has ended; shutting down")
                # current behaviour: shutdown (for simplicity)
                # TODO: implement a different on stream end behaviour
                asyncio.create_task(self.shutdown())
                break
            except Exception as exc:
                await self._handle_critical_exception(exc)
                break
            # finally:
            #     self._is_ingestion_and_processing_idle_event.set()
        self._is_ingestion_loop_stopped_event.set()

    async def _wait_for_persisted_and_output_messages(self) -> None:
        # preserves the order
        while True:
            try:
                if self._output_persist_tasks.empty():
                    self._is_persistence_and_publishing_idle_event.set()
                persist_task_item: OutputPersistTaskItem[JobInProgressMsgT] = await self._output_persist_tasks.get()
                self._is_persistence_and_publishing_idle_event.clear()
                frame_id: str = persist_task_item.frame_id
                try:
                    # await the completion of persistence task
                    await persist_task_item.persist_task
                    msg: JobInProgressMsgT = persist_task_item.msg
                    await self._mq_to_pipeline.publish_for_emitted_item(msg)
                    self._logger.debug(f"Sent output message for publishing for frame: {frame_id}")
                    self._prev_emitted_item_msg = msg
                except OutputPersistenceTimeout:
                    # log, emit a message
                    await self._handle_dropped_job(f"Frame ID: {frame_id}")
                except Exception as exc:
                    await self._handle_critical_exception(exc)
                finally:
                    self._output_persist_tasks.task_done()
            except QueueShutDown:
                assert self._is_shutting_down_event.is_set()
                assert self._is_persistence_and_publishing_idle_event.is_set()
                self._logger.debug("All persistence tasks completed and output messages sent "
                                   "to the message queue broker adapter")
                break