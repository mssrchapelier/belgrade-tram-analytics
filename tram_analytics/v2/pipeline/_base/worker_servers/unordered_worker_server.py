import asyncio
from abc import ABC
from asyncio import Queue, QueueShutDown
from typing import NamedTuple, override

from tram_analytics.v2.pipeline._base.common.exceptions import OutputPersistenceTimeout, InputFetchTimeout, \
    ItemProcessingException
from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, WorkerOutputMessageWrapper,
    BaseCriticalErrorMessage, WorkerJobID
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
    BaseUnorderedAsyncWorker


class FetchedInputContainer[InputT](NamedTuple):
    job_id: WorkerJobID
    input_item: InputT

class BaseUnorderedWorkerServer[
    InputMsgT: BaseFrameJobInProgressMessage,
    OutputMsgT: BaseFrameJobInProgressMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage,
    InputT, OutputT,
    ConfigT: BaseWorkerServerConfig
](
    BaseWorkerServer[InputMsgT, OutputMsgT, CriticalErrMsgT, InputT, OutputT, ConfigT],
    ABC
):

    # "UNORDERED":
    # (1) sends inputs for processing one by one, but in the order the inputs have been fetched
    # (2) sends output messages in the order persistence tasks finish
    #
    # (i. e., in both cases, might not preserve the order of ingestion)
    # This may be suitable for e. g. detection, feature extraction
    # (particularly because the per-camera stage orchestrator manages reordering
    # and the underlying main processor here treats frames independently).

    # This worker server does not support receiving "on session end" messages,
    # as the underlying workers are stateless and do not emit anything in that case;
    # `BaseSequentialWorkerServer` should be used for stateful workers.

    def __init__(self,
                 *,
                 worker: BaseUnorderedAsyncWorker[InputT, OutputT],
                 read_repo: BaseWorkerServerReadRepo[InputT],
                 write_repo: BaseWriteRepo[OutputT],
                 mq_to_pipeline: BaseWorkerServerMQOutputPort[OutputMsgT, CriticalErrMsgT],
                 config: ConfigT) -> None:
        super().__init__(read_repo=read_repo,
                         write_repo=write_repo,
                         mq_to_pipeline=mq_to_pipeline,
                         config=config)

        self._worker: BaseUnorderedAsyncWorker[InputT, OutputT] = worker

        self._fetched_inputs: Queue[FetchedInputContainer[InputT]] = Queue()
        self._completed_persistence: Queue[WorkerJobID] = Queue()

        # used to track whether the corresponding queues can be shut down
        self._fetch_tasks_running: int = 0
        self._persist_tasks_running: int = 0

    @override
    async def _process(self, input_item: InputT) -> OutputT:
        return await self._worker.process(input_item)

    @override
    async def _startup_sequence(self) -> None:
        await self._worker.start()

    @override
    async def _shutdown_sequence(self) -> None:
        if self._fetch_tasks_running == 0:
            self._fetched_inputs.shutdown()
        await self._fetched_inputs.join()
        await self._wait_for_input_and_process_task
        if self._persist_tasks_running == 0:
            self._completed_persistence.shutdown()
        await self._completed_persistence.join()
        await self._wait_for_persisted_and_output_messages_task
        await self._worker.shutdown()

    async def _fetch_input_and_enqueue(self, job_id: WorkerJobID) -> None:
        self._fetch_tasks_running += 1
        frame_id: str = job_id.frame_id
        try:
            input_item: InputT = await self._fetch_input(frame_id)
            await self._fetched_inputs.put(FetchedInputContainer(job_id=job_id, input_item=input_item))
        except InputFetchTimeout as exc:
            # input fetch timeouts are not raised but wrapped and passed to the stage
            # input message: acknowledge, discard from buffer
            self._mark_for_acknowledgement_and_discard_input_msg(job_id)
            # output message: build with the exception embedded
            out_msg: WorkerOutputMessageWrapper[OutputMsgT] = (
                WorkerOutputMessageWrapper(job_id=job_id).set_exception(exc)
            )
            # publish the output message
            await self._mq_to_pipeline.publish_for_completed_job(out_msg)
        except Exception as exc:
            # other exceptions at this stage are critical
            # log, report the exception
            await self._handle_critical_exception(job_id, exc)
            raise exc
        finally:
            self._fetch_tasks_running -= 1
            if self._is_shutting_down and self._fetch_tasks_running == 0:
                self._fetched_inputs.shutdown()

    async def _persist_output_and_enqueue(self, *, job_id: WorkerJobID, output_item: OutputT) -> None:
        self._persist_tasks_running += 1
        try:
            # await the completion of the persistence task
            await self._persist_output(job_id=job_id, output=output_item)
            # mark persistence for this frame as successfully completed
            await self._completed_persistence.put(job_id)
        except OutputPersistenceTimeout as exc:
            # output persistence timeouts are not raised but wrapped and passed to the stage
            # input message: acknowledge, discard from buffer
            self._mark_for_acknowledgement_and_discard_input_msg(job_id)
            # output message: build with the exception embedded
            out_msg: WorkerOutputMessageWrapper[OutputMsgT] = (
                WorkerOutputMessageWrapper(job_id=job_id).set_exception(exc)
            )
            # publish the output message
            await self._mq_to_pipeline.publish_for_completed_job(out_msg)
        except Exception as exc:
            await self._handle_critical_exception(job_id, exc)
            raise exc
        finally:
            self._persist_tasks_running -= 1
            if self._is_shutting_down and self._persist_tasks_running == 0:
                self._completed_persistence.shutdown()

    @override
    def _on_receive_sequence(self, job_id: WorkerJobID) -> None:
        if job_id.is_session_end:
            # unordered worker, no such behaviour defined --> simply pipe through without actual processing
            asyncio.create_task(self._pipe_through_for_session_end(job_id))
        else:
            # create an input fetch task
            asyncio.create_task(self._fetch_input_and_enqueue(job_id))

    @override
    async def _retrieve_inputs_and_process(self) -> None:
        while True:
            try:
                # get the next available input
                # (a later input message for which inputs have been fetched faster will get priority)
                fetched_input_container: FetchedInputContainer[InputT] = await (
                    self._fetched_inputs.get()
                )
                job_id: WorkerJobID = fetched_input_container.job_id
                input_item: InputT = fetched_input_container.input_item
                try:
                    # process
                    output_item: OutputT = await self._process_with_exception(input_item)
                    # create a persistence task
                    asyncio.create_task(self._persist_output_and_enqueue(job_id=job_id,
                                                                         output_item=output_item))
                except ItemProcessingException as exc:
                    # NOTE: Here, for a stateless worker, item processing exceptions
                    # are NOT considered to be critical.
                    # They are wrapped into the output message which is sent to the stage
                    # (which will drop them and report as dropped),
                    # but the exception is not raised to not halt the processing.
                    # Rationale: possibly bad inputs can cause an error for one item, but not for the next one.
                    # NOTE: Monitor logs/system health (dropped messages from the calling stage)
                    # to see whether there are issues with the worker as such and not with individual inputs.
                    # input message: acknowledge, discard from buffer
                    self._mark_for_acknowledgement_and_discard_input_msg(job_id)
                    # output message: build with the exception embedded
                    out_msg: WorkerOutputMessageWrapper[OutputMsgT] = (
                        WorkerOutputMessageWrapper(job_id=job_id).set_exception(exc)
                    )
                    # publish the output message
                    await self._mq_to_pipeline.publish_for_completed_job(out_msg)
                except Exception as exc:
                    # the rest are critical
                    await self._handle_critical_exception(job_id, exc)
                    raise exc
                finally:
                    self._fetched_inputs.task_done()
            except QueueShutDown:
                assert self._is_shutting_down
                self._logger.debug("All pending items processed")
                break

    async def _pipe_through_for_session_end(self, job_id: WorkerJobID) -> None:
        assert job_id.is_session_end
        # Unordered (stateless) workers do not output anything when a session ends.
        # Since this worker server is UNORDERED, i. e. it does NOT attempt to preserve the order of inputs,
        # we can simply return a success message for the job so that it can simply pipe it through.
        # Actual processing for session ends is only needed (and defined) for ordered (stateful) workers.
        self._logger.debug(f"Job {job_id} is for a session end; this worker server is unordered -- "
                           f"no output, simply piping through. Returning a success message back to the stage")
        out_msg: WorkerOutputMessageWrapper[OutputMsgT] = self._build_out_message_wrapped_success(job_id)
        await self._mq_to_pipeline.publish_for_completed_job(out_msg)
        self._mark_for_acknowledgement_and_discard_input_msg(job_id)

    @override
    async def _persist_and_output_messages(self) -> None:
        while True:
            try:
                # get the next completed output persistence task
                # (if an output for a later frame has been persisted faster, it will get priority)
                job_id: WorkerJobID = await self._completed_persistence.get()
                try:
                    out_msg: WorkerOutputMessageWrapper[OutputMsgT] = self._build_out_message_wrapped_success(job_id)
                    await self._mq_to_pipeline.publish_for_completed_job(out_msg)
                    self._mark_for_acknowledgement_and_discard_input_msg(job_id)
                    self._completed_persistence.task_done()
                except Exception as exc:
                    await self._handle_critical_exception(job_id, exc)
            except QueueShutDown:
                assert self._is_shutting_down
                self._logger.debug("All persistence tasks completed and output messages sent "
                                   "to the message queue broker adapter")
                break
