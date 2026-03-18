import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Task
from logging import Logger
from typing import Dict

from pydantic import BaseModel, PositiveFloat

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.common.exceptions import OutputPersistenceTimeout, ItemProcessingException, \
    InputFetchTimeout
from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, MessageWithAckFuture, WorkerOutputMessageWrapper, WorkerJobID,
    BaseCriticalErrorMessage, NegativeAcknowledgementException
)
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.mq.to_stage import \
    BaseWorkerServerMQOutputPort
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.read_repo import \
    BaseWorkerServerReadRepo
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.write_repo import \
    BaseWriteRepo


class BaseWorkerServerShutdownException(Exception):
    pass

class BaseWorkerServerConfig(BaseModel, ABC):
    # How long to wait for inputs.
    # With a set timeout, a timeout will result in the frame being skipped.
    input_fetch_timeout: PositiveFloat | None
    # How long to wait for the outputs to have been stored successfully.
    # Likewise, a timeout will result in the frame being skipped.
    output_persist_timeout: PositiveFloat | None


class BaseWorkerServer[
    InputMsgT: BaseFrameJobInProgressMessage,
    OutputMsgT: BaseFrameJobInProgressMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage,
    InputT, OutputT,
    ConfigT: BaseWorkerServerConfig
](ABC):

    def __init__(self,
                 *,
                 read_repo: BaseWorkerServerReadRepo[InputT],
                 write_repo: BaseWriteRepo[OutputT],
                 mq_to_pipeline: BaseWorkerServerMQOutputPort[
                     OutputMsgT, CriticalErrMsgT
                 ],
                 config: ConfigT) -> None:
        self._config: ConfigT = config
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._read_repo_adapter: BaseWorkerServerReadRepo[InputT] = read_repo
        self._write_repo_adapter: BaseWriteRepo[OutputT] = write_repo
        self._mq_to_pipeline: BaseWorkerServerMQOutputPort[OutputMsgT, CriticalErrMsgT] = (
            mq_to_pipeline
        )

        self._jobs_in_progress: Dict[WorkerJobID, MessageWithAckFuture[InputMsgT]] = dict()

        self._is_running: bool = False
        self._is_shutting_down: bool = False

        self._wait_for_persisted_and_output_messages_task: Task[None] = asyncio.create_task(
            self._persist_and_output_messages()
        )
        self._wait_for_input_and_process_task: Task[None] = asyncio.create_task(
            self._retrieve_inputs_and_process()
        )

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

    @abstractmethod
    async def _startup_sequence(self) -> None:
        pass

    async def shutdown(self) -> None:
        if not self._is_running:
            self._logger.warning("Requested shutdown but is not running")
            return
        if self._is_shutting_down:
            self._logger.warning("This instance is already shutting down")
            return
        self._logger.info("Shutdown initiated")
        self._is_shutting_down = True
        await self._shutdown_sequence()
        self._is_running = False
        self._is_shutting_down = False
        self._logger.info("Shutdown complete")

    @abstractmethod
    async def _shutdown_sequence(self) -> None:
        pass

    def on_receive(self, input_msg_with_ack_future: MessageWithAckFuture[InputMsgT]) -> None:
        input_msg: InputMsgT = input_msg_with_ack_future.message
        job_id: WorkerJobID = input_msg.get_job_id()
        self._logger.debug(f"Got job: {job_id}")
        self._jobs_in_progress[job_id] = input_msg_with_ack_future

        if not self._is_running:
            self._logger.debug("Is not running; nacking and discarding the input message")
            # can't accept more messages; nack and discard
            self._negatively_acknowledge_and_discard_input_msg(job_id)
            return

        self._on_receive_sequence(job_id)

    @abstractmethod
    def _on_receive_sequence(self, job_id: WorkerJobID) -> None:
        pass

    @abstractmethod
    async def _process(self, input_item: InputT) -> OutputT:
        pass

    async def _process_with_exception(self, input_item: InputT) -> OutputT:
        try:
            return await self._process(input_item)
        except Exception as exc:
            raise ItemProcessingException() from exc

    async def _fetch_input(self, frame_id: str) -> InputT:
        """
        Given the frame ID, fetch the inputs from the input persistence (with a timeout if configured).
        """
        # NOTE: Inputs/outputs are identified by their frame_id uniquely.
        # For session end jobs, there are no inputs by definition (but there may be outputs).
        try:
            async with asyncio.timeout(self._config.input_fetch_timeout):
                # May raise a TimeoutError; is to be handled during the resolution of the task
                # in which this coroutine is running.
                self._logger.debug(f"Frame waiting for inputs to be fetched: {frame_id}")
                inputs: InputT = await self._read_repo_adapter.retrieve(frame_id,
                                                                        timeout=self._config.input_fetch_timeout)
                self._logger.debug(f"Frame inputs fetched successfully: {frame_id}")
                return inputs
        except TimeoutError as exc:
            self._logger.debug(f"Timed out waiting for inputs for frame: {frame_id}")
            raise InputFetchTimeout() from exc

    async def _persist_output(self, *, job_id: WorkerJobID, output: OutputT) -> None:
        """
        Attempt to persist the output (using the frame_id as the unique key) in the output storage;
        with a timeout if configured.
        """
        try:
            async with asyncio.timeout(self._config.output_persist_timeout):
                # May raise a TimeoutError.
                self._logger.debug(f"Attempting to persist outputs for job {job_id}")
                await self._write_repo_adapter.store(output=output,
                                                     timeout=self._config.output_persist_timeout)
                self._logger.debug(f"Successfully persisted outputs for job {job_id}")
        except TimeoutError as exc:
            self._logger.debug(f"Timed out waiting to persist outputs for job {job_id}")
            raise OutputPersistenceTimeout() from exc

    def _mark_for_acknowledgement_and_discard_input_msg(self, job_id: WorkerJobID) -> None:
        # - get the input message and the acknowledgement future (discarding them from the buffer)
        input_msg_with_ack_future: MessageWithAckFuture[InputMsgT] = self._jobs_in_progress.pop(job_id)
        # - set the result of the ack future (i. e. signal to acknowledge the input message)
        input_msg_with_ack_future.ack_future.set_result(None)
        self._logger.debug(f"Marked input message for job: {job_id} as ready for acknowledgement")

    def _negatively_acknowledge_and_discard_input_msg(self, job_id: WorkerJobID) -> None:
        # nack and discard; used for critical exception or for new messages when the server is shutting down
        input_msg_with_ack_future: MessageWithAckFuture[InputMsgT] = self._jobs_in_progress.pop(job_id)
        input_msg_with_ack_future.ack_future.set_exception(NegativeAcknowledgementException())
        self._logger.warning(f"Negatively acknowledged input message for job: {job_id}")

    @abstractmethod
    def _build_out_message(self, input_msg: InputMsgT) -> OutputMsgT:
        pass

    def _build_out_message_wrapped_success(self, job_id: WorkerJobID) -> WorkerOutputMessageWrapper[OutputMsgT]:
        input_msg: InputMsgT = self._jobs_in_progress[job_id].message
        out_msg: OutputMsgT = self._build_out_message(input_msg)
        wrapper: WorkerOutputMessageWrapper[OutputMsgT] = WorkerOutputMessageWrapper(job_id=job_id)
        wrapper.set_result(out_msg)
        return wrapper

    def _build_out_message_wrapped_exception(
            self, job_id: WorkerJobID, exc: Exception
    ) -> WorkerOutputMessageWrapper[OutputMsgT]:
        wrapper: WorkerOutputMessageWrapper[OutputMsgT] = WorkerOutputMessageWrapper(job_id=job_id)
        wrapper.set_exception(exc)
        return wrapper

    @abstractmethod
    def _build_critical_error_message(self, job_id: WorkerJobID, exc: Exception) -> CriticalErrMsgT:
        pass

    async def _handle_noncritical_exception(self, job_id: WorkerJobID, exc: Exception) -> None:
        # output message: build with the exception embedded
        out_msg: WorkerOutputMessageWrapper[OutputMsgT] = (
            WorkerOutputMessageWrapper(job_id=job_id).set_exception(exc)
        )
        # publish the output message
        await self._mq_to_pipeline.publish_for_completed_job(out_msg)
        # acknowledge and discard the input message
        self._mark_for_acknowledgement_and_discard_input_msg(job_id)

    async def _handle_critical_exception(self, job_id: WorkerJobID, exc: Exception) -> None:
        """
        Handle a critical exception (before raising in the calling code):
        log and send a message to the broker.
        """
        is_item_exc: bool = isinstance(exc, ItemProcessingException)
        logger_msg: str = f"Critical exception when processing job {job_id} | In worker when processing item?: {is_item_exc}"
        self._logger.critical(logger_msg)
        self._logger.debug(f"{logger_msg}:\n", exc_info=exc)
        self._negatively_acknowledge_and_discard_input_msg(job_id)
        broker_msg: CriticalErrMsgT = self._build_critical_error_message(job_id, exc)
        await self._mq_to_pipeline.report_critical_error(broker_msg)

    @abstractmethod
    async def _retrieve_inputs_and_process(self) -> None:
        pass

    @abstractmethod
    async def _persist_and_output_messages(self) -> None:
        pass


