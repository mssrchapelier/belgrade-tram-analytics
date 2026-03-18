import asyncio
from abc import ABC
from asyncio import Task, Queue, QueueShutDown
from typing import override, Dict, NamedTuple

from tram_analytics.v2.pipeline._base.common.exceptions import OutputPersistenceTimeout, ItemProcessingException, \
    InputFetchTimeout
from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, WorkerOutputMessageWrapper, WorkerJobID,
    BaseCriticalErrorMessage
)
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.mq.to_stage import \
    BaseWorkerServerMQOutputPort
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.read_repo import \
    BaseWorkerServerReadRepo
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.write_repo import \
    BaseWriteRepo
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import (
    BaseWorkerServerConfig, BaseWorkerServer
)
from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseOrderedAsyncWorker


class JobOutput[OutputT](NamedTuple):
    job_id: WorkerJobID
    output: OutputT | None

class BaseOrderedWorkerServer[
    InputMsgT: BaseFrameJobInProgressMessage,
    OutputMsgT: BaseFrameJobInProgressMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage,
    InputT, OutputT,
    ConfigT: BaseWorkerServerConfig
](
    BaseWorkerServer[InputMsgT, OutputMsgT, CriticalErrMsgT, InputT, OutputT, ConfigT],
    ABC
):

    # (1) sends inputs for processing in the order of ingestion
    # (2) sends output messages in the order of ingestion

    # Sends items for processing one by one, but decouples (creates background tasks for)
    # the fetching of inputs and the persistence of outputs.

    def __init__(self,
                 *,
                 worker: BaseOrderedAsyncWorker[InputT, OutputT],
                 read_repo: BaseWorkerServerReadRepo[InputT],
                 write_repo: BaseWriteRepo[OutputT],
                 mq_to_pipeline: BaseWorkerServerMQOutputPort[OutputMsgT, CriticalErrMsgT],
                 config: ConfigT) -> None:
        super().__init__(read_repo=read_repo,
                         write_repo=write_repo,
                         mq_to_pipeline=mq_to_pipeline,
                         config=config)

        self._worker: BaseOrderedAsyncWorker[InputT, OutputT] = worker

        # frame id -> task to fetch inputs
        self._input_fetch_tasks: Dict[str, Task[InputT]] = dict()
        # queue for jobs to be started (for alive sessions, will wait for inputs to be fetched first)
        self._jobs_to_start: Queue[WorkerJobID] = Queue()
        # job id -> task to persist outputs
        # self._output_persist_tasks: Dict[WorkerJobID, Task[None]] = dict()
        # queue for jobs to report as finished (for non-null outputs, will wait for them to be persisted first)
        self._jobs_to_persist_and_publish: Queue[JobOutput[OutputT]] = Queue()

    @override
    async def _process(self, input_item: InputT) -> OutputT:
        return await self._worker.process(input_item)

    async def _process_for_session_end(self) -> OutputT | None:
        return await self._worker.process_for_session_end()

    async def _process_for_session_end_with_exception(self) -> OutputT | None:
        try:
            return await self._process_for_session_end()
        except Exception as exc:
            raise ItemProcessingException() from exc

    @override
    async def _shutdown_sequence(self) -> None:
        # NOTE: a warning about shutting down this worker before the stage has been shut down!
        #
        # In the current implementation, the stage is responsible for issuing a session end job.
        # This worker server must therefore be shut down only after the stage has determined
        # that it has received replies from the worker for all job requests that it issued (or has timed out on them).
        # Otherwise, if the worker is expected to emit outputs for the session's end that are to be persisted,
        # none will be.
        self._jobs_to_start.shutdown()
        await self._jobs_to_start.join()
        await self._wait_for_input_and_process_task
        await self._wait_for_persisted_and_output_messages_task
        await self._worker.shutdown()

    @override
    async def _startup_sequence(self) -> None:
        await self._worker.start()

    @override
    def _on_receive_sequence(self, job_id: WorkerJobID) -> None:
        if not job_id.is_session_end:
            # Fetch inputs (create a task for that end), but only for jobs that do not end the session.
            # For session ends, the worker is called without any inputs.
            frame_id: str = job_id.frame_id
            fetch_task: Task[InputT] = asyncio.create_task(self._fetch_input(frame_id))
            self._input_fetch_tasks[frame_id] = fetch_task
        self._jobs_to_start.put_nowait(job_id)

    async def _retrieve_inputs_process_and_send_for_persistence_for_alive_session(
            self, job_id: WorkerJobID
    ):
        # for a single item, for alive session
        assert not job_id.is_session_end
        frame_id: str = job_id.frame_id
        assert frame_id in self._input_fetch_tasks
        fetch_task: Task[InputT] = self._input_fetch_tasks.pop(frame_id)
        self._logger.debug(f"Waiting for inputs for job {job_id}")
        input_item: InputT = await fetch_task
        self._logger.debug(f"Got inputs for job {job_id}, sending for processing")
        output_item: OutputT = await self._process_with_exception(input_item)
        self._logger.debug(f"Got outputs for job {job_id}, enqueueing for persisting and publishing")
        # persist_task: Task[None] = asyncio.create_task(
        #     self._persist_output(job_id=job_id, output=output_item)
        # )
        # self._output_persist_tasks[job_id] = persist_task
        await self._jobs_to_persist_and_publish.put(
            JobOutput(job_id=job_id, output=output_item)
        )

    async def _process_and_send_for_persistence_for_ended_session(self, job_id: WorkerJobID):
        # for a single item, for an ended session
        assert job_id.is_session_end
        self._logger.debug(f"Job {job_id} is for session end -- no inputs needed; "
                           f"sending for processing")
        output_item: OutputT | None = await self._process_for_session_end_with_exception()
        if output_item is not None:
            self._logger.debug(f"Got outputs for job {job_id}, "
                               f"enqueueing for persisting and publishing")
            # frame_id: str = job_id.frame_id
            # persist_task: Task[None] = asyncio.create_task(
            #     self._persist_output(job_id=job_id, output=output_item)
            # )
            # self._output_persist_tasks[job_id] = persist_task
        self._logger.debug(f"Got no outputs for job {job_id} (successful return but is null), "
                           f"enqueueing for persisting and publishing")
        await self._jobs_to_persist_and_publish.put(
            JobOutput(job_id=job_id, output=output_item)
        )

    @override
    async def _retrieve_inputs_and_process(self) -> None:
        # preserves the order
        while True:
            try:
                job_id: WorkerJobID = await self._jobs_to_start.get()
                try:
                    if job_id.is_session_end:
                        await self._process_and_send_for_persistence_for_ended_session(job_id)
                    else:
                        await self._retrieve_inputs_process_and_send_for_persistence_for_alive_session(job_id)
                    # signal the completion of processing of one item
                    self._jobs_to_start.task_done()
                except InputFetchTimeout as exc:
                    # input fetch timeouts are not raised but wrapped and passed to the stage
                    await self._handle_noncritical_exception(job_id, exc)
                    # signal the completion of processing of one item
                    self._jobs_to_start.task_done()
                except Exception as exc:
                    await self._handle_critical_exception(job_id, exc)
                    raise exc
            except QueueShutDown:
                # shutting down
                assert self._is_shutting_down
                break
        # shut down the jobs to publish queue (no more will be coming)
        # self._jobs_to_start.shutdown()
        self._jobs_to_persist_and_publish.shutdown()

    async def _persist_and_output_message_for_item(self, job_output: JobOutput[OutputT]) -> None:
        job_id: WorkerJobID = job_output.job_id
        output_item: OutputT | None = job_output.output
        if output_item is not None:
            # with persistence
            try:
                await self._persist_output(job_id=job_id, output=output_item)
                out_msg: WorkerOutputMessageWrapper[OutputMsgT] = (
                    self._build_out_message_wrapped_success(job_id)
                )
            except OutputPersistenceTimeout as exc:
                # input fetch timeouts are not raised but wrapped and passed to the stage
                # build an exception message for the stage
                await self._handle_noncritical_exception(job_id, exc)
                self._jobs_to_persist_and_publish.task_done()
                return
            except Exception as exc:
                # --- other exceptions are critical and are to be raised ---
                raise exc
        else:
            # without persistence (null outputs)
            # build a success message for the stage
            out_msg = self._build_out_message_wrapped_success(job_id)
        # send the success message for publishing
        await self._mq_to_pipeline.publish_for_completed_job(out_msg)
        self._logger.debug(f"Sent output message for publishing for job: {job_id}")
        # acknowledge the input message, discard from the buffer
        self._mark_for_acknowledgement_and_discard_input_msg(job_id)

    @override
    async def _persist_and_output_messages(self) -> None:
        # preserves the order
        while True:
            try:
                job_output: JobOutput[OutputT] = await self._jobs_to_persist_and_publish.get()
                await self._persist_and_output_message_for_item(job_output)
                # signal the completion of processing of one item
                self._jobs_to_persist_and_publish.task_done()
            except QueueShutDown:
                assert self._is_shutting_down
                self._logger.debug("All persistence tasks completed and output messages sent "
                                   "to the message queue broker adapter")
                break