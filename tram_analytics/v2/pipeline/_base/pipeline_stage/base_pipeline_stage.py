import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Task, QueueShutDown
from copy import copy
from logging import Logger
from typing import Dict, Self, NamedTuple, Type, Set, Tuple

from pydantic import BaseModel

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.common.exceptions import OutputPersistenceTimeout, InputFetchTimeout, \
    ItemProcessingException
from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, MessageWithAckFuture,
    WorkerInputMessageWrapper, WorkerJobID,
    BaseDroppedJobMessage, BaseCriticalErrorMessage,
    NegativeAcknowledgementException
)
from tram_analytics.v2.pipeline._base.mq.base_mq import BaseOutputMQMultiChannel
from tram_analytics.v2.pipeline._base.pipeline_stage.adapters.output_adapters.mq.to_workers import \
    StageMQToWorkersOutputPort
from tram_analytics.v2.pipeline._base.pipeline_stage.processor_with_reordering import (
    ProcessorOutputItemWithTimeout, ProcessorWithTimeoutConfig, ProcessorWithTimeoutAndReordering,
    ProcessorTimeoutException
)


class BasePipelineStageConfig(BaseModel, ABC):
    camera_id: str
    is_ordered_worker: bool
    processing: ProcessorWithTimeoutConfig

class PipelineStageShutdownException(Exception):
    # To be raised when attempting to call `on_receive` on a pipeline stage instance
    # that is shutting down or has been shut down.
    pass

class BasePipelineStage[
    ConfigT: BasePipelineStageConfig,
    JobInProgressMsgT: BaseFrameJobInProgressMessage,
    DroppedJobMsgT: BaseDroppedJobMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage
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

    def __init__(self,
                 *, config: ConfigT,
                 # pipeline stage to workers
                 mq_to_workers_adapter: StageMQToWorkersOutputPort[JobInProgressMsgT, JobInProgressMsgT],
                 # pipeline stage to pipeline
                 mq_to_pipeline_adapter: BaseOutputMQMultiChannel[
                     JobInProgressMsgT, DroppedJobMsgT, CriticalErrMsgT
                 ]) -> None:
        self._config: ConfigT = config
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        # Used for idempotency purposes:
        # - Storing the session ID and the frame's sequence number for the last frame seen,
        # to prevent the processing of frames already processed earlier
        # if input messages with the same sequence number arrive again,
        # which may happen if the broker goes down and then re-delivers messages that had been accepted
        # but not acknowledged before it went down.
        # - If the session ID has changed but an "is_session_end" message for that session ID
        # was not received, issue an "is_session_end" message here for the worker, and only then pass the new one.
        # NOTE: It is for the input message queue broker adapter
        # to ensure that messages are not attempted to be acknowledged twice (!!!) --
        # this is the implementation detail of the adapter, not of this core application.
        self._last_seen_message: JobInProgressMsgT | None = None

        # A buffer to hold the input messages for the jobs currently in progress.
        # Rationale: to keep track of input messages that need to be acknowledged.
        # Keyed by (frame ID, is_session_end) as the unique identifier;
        # mapped is the input message (in order to construct the output message from it)
        # and the futures to fulfil when the input message is ready to be acknowledged
        # by the calling message queue broker adapter.
        self._received_input_messages: Dict[WorkerJobID, MessageWithAckFuture[JobInProgressMsgT]] = dict()
        self._processor_with_timeout: ProcessorWithTimeoutAndReordering[JobInProgressMsgT, JobInProgressMsgT] = (
            ProcessorWithTimeoutAndReordering(mq_to_worker_adapter=mq_to_workers_adapter,
                                              config=self._config.processing)
        )
        self._mq_to_pipeline: BaseOutputMQMultiChannel[
            JobInProgressMsgT, DroppedJobMsgT, CriticalErrMsgT
        ] = mq_to_pipeline_adapter

        self._retrieve_from_processor_task: Task[None] = asyncio.create_task(
            self._retrieve_from_processor()
        )

        self._is_running: bool = False
        # Whether this instance has been signalled to shut down.
        self._is_shutting_down: bool = False

        self._non_critical_exception_types: Tuple[Type[Exception], ...] = self._get_non_critical_exception_types()

    def _get_non_critical_exception_types(self) -> Tuple[Type[Exception], ...]:
        exceptions: Set[Type[Exception]] = {
            ProcessorTimeoutException, InputFetchTimeout, OutputPersistenceTimeout
        }
        if not self._config.is_ordered_worker:
            exceptions.add(ItemProcessingException)
        return tuple(exceptions)

    async def start(self) -> None:
        if self._is_running:
            self._logger.warning("This instance is already running")
            return
        if self._is_shutting_down:
            self._logger.warning("Requested startup but this instance is shutting down")
            return
        self._logger.info("Startup initiated")
        self._is_running = True
        await self._startup_sequence()
        self._logger.info("Startup complete")

    async def _startup_sequence(self) -> None:
        # reserving if we do need to initiate actions at startup, but in the current implementation, nothing is needed
        # also for consistency (so that the broker can request startup from stages and worker servers in the same manner)
        # if need be, declare as an abstract class and implement in subclasses
        pass

    async def shutdown(self) -> None:
        # set the "shutting down" flag to true
        # - new calls to `on_receive` will raise a `PipelineStageShutdownException`
        # - the first part of the while condition in the loop in `._retrieve_from_processor()`
        #   is set to `True`, causing it to wait until `_frames_in_progress` empties out
        if not self._is_running:
            self._logger.warning("Requested shutdown but is not running")
            return
        if self._is_shutting_down:
            self._logger.warning("Requested a shutdown but is already shutting down")
            return
        self._logger.info("Shutdown initiated")
        self._is_shutting_down = True
        await self._processor_with_timeout.shutdown()
        # wait for the feeder from the processor to finish (with `_is_shutting_down` set to `True`,
        # will now stop iteration once all pending items have been processed).
        await self._retrieve_from_processor_task
        self._is_running = False
        self._is_shutting_down = False
        self._logger.info("Shutdown complete")


    class DropDecision(NamedTuple):
        to_drop: bool
        reason: str | None

        @classmethod
        def as_do_not_drop(cls) -> Self:
            return cls(to_drop=False, reason=None)

        @classmethod
        def as_drop(cls, reason: str) -> Self:
            return cls(to_drop=True, reason=reason)


    def _is_to_drop_message(self, received: JobInProgressMsgT) -> DropDecision:
        """
        Compares this message with the last received one and determines whether to drop it (with acknowledgement).
        Returns the boolean flag indicating whether to drop the message, and, if that is `True`,
        the reason part of the message to log.
        """
        last_seen: JobInProgressMsgT | None = self._last_seen_message
        if last_seen is None:
            # do not drop -- the first message seen
            return self.DropDecision.as_do_not_drop()
        if received.session_id < last_seen.session_id:
            # session ID decreased: invalid, drop
            return self.DropDecision.as_drop("The session ID has decreased")
        elif received.session_id > last_seen.session_id:
            # do not drop; whether a session end message will be emitted or not will be determined by the calling code
            return self.DropDecision(to_drop=False, reason=None)
        assert received.session_id == last_seen.session_id
        session_id_same_part: str = "The session ID is the same"
        if received.seq_num < last_seen.seq_num:
            # session ID is the same and the sequence number decreased: invalid, drop
            return self.DropDecision.as_drop(f"{session_id_same_part} and the sequence number has decreased")
        assert received.seq_num >= last_seen.seq_num
        if last_seen.is_session_end:
            # same session, but has already ended
            return self.DropDecision.as_drop(f"{session_id_same_part} and the session is stored as having ended")
        assert not last_seen.is_session_end
        if received.seq_num == last_seen.seq_num and not received.is_session_end:
            # same session, same seq num, not recorded as having ended, but this is not a session end message
            return self.DropDecision.as_drop(f"{session_id_same_part} and the sequence number is the same, "
                                                f"and the session is stored as having not ended, "
                                                f"but this message is not a session end message")
        # otherwise, do not drop
        return self.DropDecision.as_do_not_drop()

    def _is_to_end_previous_session_first(self, received: JobInProgressMsgT) -> bool:
        last_seen: JobInProgressMsgT | None = self._last_seen_message
        return (
            # there is a previously stored session id
            last_seen is not None
            # the session id has increased
            and received.session_id > last_seen.session_id
            # the previous message is not an end message
            and not last_seen.is_session_end
        )

    def _enqueue_manual_session_end_job(self) -> None:
        assert self._last_seen_message is not None
        if not self._config.is_ordered_worker:
            raise ValueError("Called _enqueue_manual_session_end_job for a worker "
                             "that is not specified as an ordered worker")
        manual_session_end_msg: JobInProgressMsgT = copy(self._last_seen_message)
        manual_session_end_job_id: WorkerJobID = WorkerJobID(self._last_seen_message.frame_id,
                                                             self._last_seen_message.is_session_end)
        self._processor_with_timeout.put(
            WorkerInputMessageWrapper(job_id=manual_session_end_job_id, inputs_msg=manual_session_end_msg)
        )
        self._logger.debug("Detected a session end unaccounted for; created and enqueued "
                           f"a manual session end job: {manual_session_end_job_id}")

    def on_receive(self, input_msg_with_ack_future: MessageWithAckFuture[JobInProgressMsgT]) -> None:
        """
        The method to be called in the input message queue broker adapter to handle one incoming message.
        """

        input_msg: JobInProgressMsgT = input_msg_with_ack_future.message
        frame_id: str = input_msg.frame_id
        is_session_end: bool = input_msg.is_session_end
        job_id: WorkerJobID = WorkerJobID(frame_id, is_session_end)

        self._logger.debug(f"Stage got message for job: {job_id}")

        # add the input message and the acknowledgement future to the buffer
        self._received_input_messages[job_id] = input_msg_with_ack_future

        if self._is_shutting_down:
            # can't accept more messages; nack and discard
            self._negatively_acknowledge_and_discard_input_msg(job_id)
            return

        # for non-ordered workers, dropping session end messages -- this is a normal flow
        # (these are intended for stages with stateful workers)
        #
        # NOTE: no, need to pipe them through so that the downstream can receive them; commenting out for now

        # if input_msg.is_session_end and not self._config.is_ordered_worker:
        #     self._logger.debug(f"Dropping the input message for job {job_id}: not an ordered worker, no action needed")
        #     input_msg_with_ack_future.is_ready_to_acknowledge.set_result(None)
        #     self._last_seen_message = input_msg
        #     return

        # idempotency guard
        drop_decision: BasePipelineStage.DropDecision = self._is_to_drop_message(input_msg)
        if drop_decision.to_drop:
            assert drop_decision.reason is not None
            self._logger.warning(f"Dropping the input message for job {job_id}, reason: {drop_decision.reason}")
            # acknowledge the message...
            self._mark_for_acknowledgement_and_discard_input_msg(job_id)
            # ... but do not process it
            return

        # NOTE: Sequence numbers *can* be skipped.
        # Whether they are skipped or not is the concern of any upstream modules and the message queue broker.
        # In any module, only the sequential order of the frames accepted for processing is ensured.
        # If a skipped frame arrives out of order later, it will be discarded (see immediately above).

        # Determine whether it is needed to manually end the last stored session before processing the received message.
        # If yes, generate a job to that end, and send the corresponding message ahead of the received one.
        # This is only done for ordered (sequential, stateful) workers.

        if self._is_to_end_previous_session_first(input_msg) and self._config.is_ordered_worker:
            # send a manual job to end the session first
            self._enqueue_manual_session_end_job()

        self._logger.debug(f"Job from an input message accepted for processing: {job_id}")
        self._processor_with_timeout.put(
            WorkerInputMessageWrapper(job_id=job_id, inputs_msg=input_msg)
        )

        self._last_seen_message = input_msg

    def _mark_for_acknowledgement_and_discard_input_msg(self, job_id: WorkerJobID) -> None:
        # - get the input message and the acknowledgement future (discarding them from the buffer)
        input_msg_with_ack_future: MessageWithAckFuture[JobInProgressMsgT] = (
            self._received_input_messages.pop(job_id)
        )
        # - set the result of the ack future (i. e. signal to acknowledge the input message)
        input_msg_with_ack_future.ack_future.set_result(None)
        self._logger.debug(f"Marked input message for job: {job_id} as ready for acknowledgement")

    def _negatively_acknowledge_and_discard_input_msg(self, job_id: WorkerJobID) -> None:
        # nack and discard; used for critical exception or for new messages when the server is shutting down
        input_msg_with_ack_future: MessageWithAckFuture[JobInProgressMsgT] = self._received_input_messages.pop(job_id)
        input_msg_with_ack_future.ack_future.set_exception(NegativeAcknowledgementException())
        self._logger.warning(f"Negatively acknowledged input message for job: {job_id}")

    @abstractmethod
    def _get_dropped_job_mq_message(self, job_id: WorkerJobID, exc: Exception) -> DroppedJobMsgT:
        pass

    @abstractmethod
    def _get_critical_error_mq_message(self, job_id: WorkerJobID | None, exc: Exception) -> CriticalErrMsgT:
        pass

    async def _handle_noncritical_exception(self, job_id: WorkerJobID, exc: Exception) -> None:
        self._logger.warning(f"Processor returned a non-critical exception for job {job_id}: "
                             f"type {type(exc).__name__}; dropping frame")
        self._logger.debug(f"Exception for job {job_id}:\n", exc_info=exc)
        mq_msg: DroppedJobMsgT = self._get_dropped_job_mq_message(job_id, exc)
        await self._mq_to_pipeline.publish_for_failed_job(mq_msg)

    async def _handle_critical_exception(self, job_id: WorkerJobID | None, exc: Exception) -> None:
        job_str: str = str(job_id) if job_id is not None else "n/a"
        self._logger.critical(f"Critical exception | When in job? {job_str}")
        self._logger.debug(f"Critical exception: job {job_str}\n", exc_info=exc)
        mq_msg: CriticalErrMsgT = self._get_critical_error_mq_message(job_id, exc)
        await self._mq_to_pipeline.report_critical_error(mq_msg)

    async def _retrieve_from_processor(self) -> None:
        """
        A worker for retrieving the processed items from the processor,
        sending results for persistence or logging exceptions,
        and sending output messages to be published.

        Meant to be run as a separate task.
        """
        # while not (self._is_shutting_down and len(self._frames_in_progress) == 0):
        while True:
            try:
                # wait for the processor to produce the next output
                processing_result: ProcessorOutputItemWithTimeout[JobInProgressMsgT] = (
                    await self._processor_with_timeout.out_queue.get()
                )
                job_id: WorkerJobID = processing_result.job_id
                self._logger.debug(f"Got an outcome from processor for job {job_id}, unwrapping")

                try:
                    output_msg: JobInProgressMsgT = await processing_result.get_output()
                    # push the output message to the output message queue broker
                    await self._mq_to_pipeline.publish_for_completed_job(output_msg)
                    self._logger.debug(f"Created and enqueued for publishing output message for job: {job_id}")
                except self._non_critical_exception_types as exc:
                    await self._handle_noncritical_exception(job_id, exc)
                except Exception as exc:
                    await self._handle_critical_exception(job_id, exc)
                    raise exc
                finally:
                    # in any outcome, acknowledge the input message and discard it (and the ack future) from the buffer
                    self._mark_for_acknowledgement_and_discard_input_msg(job_id)
                    self._processor_with_timeout.out_queue.task_done()
            except QueueShutDown:
                self._logger.debug("All items retrieved from processor")
                break

