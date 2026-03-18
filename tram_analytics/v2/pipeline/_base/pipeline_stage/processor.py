import asyncio
import logging
from asyncio import Queue, Event, Task, QueueShutDown
from logging import Logger
from typing import Literal

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.models.message import WorkerInputMessageWrapper, WorkerOutputMessageWrapper, \
    WorkerJobID
from tram_analytics.v2.pipeline._base.pipeline_stage.adapters.output_adapters.mq.to_workers import \
    StageMQToWorkersOutputPort


class Processor[InputMsgT, OutputMsgT]:

    """
    Message processor for a **single camera**.

    Consumes input messages to request work from a worker (load data and perform inference/calculations),
    passes them to the worker, consumes output messages from the worker specifying
    that work on a certain frame has been done and persisted.

    The queues contain wrappers around messages; both input and output containers
    contain a frame ID for easier cross-referencing re out-of-order/missing outputs in the calling code;
    the output containers wrap the output message into a future which will re-raise the exception (if any)
    raised during the processing of the respective item by the worker.

    The order of outputs is not guaranteed to be the same as that of inputs, nor are timeouts and missing items managed;
    see `ProcessorWithTimeoutAndReordering` for this.
    """

    def __init__(self, *, mq_to_worker_adapter: StageMQToWorkersOutputPort[InputMsgT, OutputMsgT]) -> None:
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        # (frame id -> input) to be processed
        self.in_queue: Queue[WorkerInputMessageWrapper[InputMsgT]] = Queue()
        # (frame id -> future with output/exception) that have been processed
        self.out_queue: Queue[WorkerOutputMessageWrapper[OutputMsgT]] = Queue()

        # the message queue broker adapter to communicate with the worker
        self._mq_to_worker: StageMQToWorkersOutputPort[InputMsgT, OutputMsgT] = mq_to_worker_adapter

        self._is_shutting_down: Event = Event()

        self._consume_inputs_task: Task[None] = asyncio.create_task(
            self._input_consumer()
        )
        self._produce_outputs_task: Task[None] = asyncio.create_task(
            self._output_producer()
        )

    async def shutdown(self) -> None:
        if self._is_shutting_down.is_set():
            self._logger.warning("The processor is already shutting down or has been shut down")
            return
        self._logger.info("Shutting down the processor")
        self._is_shutting_down.set()
        self.in_queue.shutdown()
        # wait until all items in out_queue have been consumed by consumers
        self._logger.info("Waiting for all messages from the worker to be consumed from the worker's output queue ...")
        await self.out_queue.join()
        self._logger.info("Processor shutdown complete")

    async def _input_consumer(self) -> None:
        """
        Consumes worker job requests from the input queue,
        forwards them to the injected message queue broker adapter.
        """
        while True:
            try:
                input_item: WorkerInputMessageWrapper[InputMsgT] = await self.in_queue.get()
                job_id: WorkerJobID = input_item.job_id
                self._logger.debug(f"Processor input queue consumer got message for job: {job_id}")
                await self._mq_to_worker.send_to_worker(input_item)
                self._logger.debug(f"Processor sent message for job to message queue broker adapter: {job_id}")
                self.in_queue.task_done()
            except QueueShutDown:
                self._logger.debug("Processor: input queue consumer exiting")
                break

    async def _output_producer(self) -> None:
        """
        Consumes "work done" messages from the injected message queue broker adapter,
        puts the messages into the output queue.
        """
        while True:
            # listen for either the next message from the worker, or the shutdown signal
            get_next_msg_from_worker_task: Task[WorkerOutputMessageWrapper[OutputMsgT]] = asyncio.create_task(
                self._mq_to_worker.get_next_message_from_worker()
            )
            get_shutdown_signal_task: Task[Literal[True]] = asyncio.create_task(
                self._is_shutting_down.wait()
            )

            done, pending = await asyncio.wait(
                {get_next_msg_from_worker_task, get_shutdown_signal_task},
                return_when=asyncio.FIRST_COMPLETED
            )
            if get_shutdown_signal_task in done:
                self.out_queue.shutdown()
                self._logger.debug("Processor: output queue producer exiting")
                break
            assert get_next_msg_from_worker_task in done
            output_item: WorkerOutputMessageWrapper[OutputMsgT] = await get_next_msg_from_worker_task
            job_id: WorkerJobID = output_item.job_id
            self._logger.debug(f"Processor got message from message queue broker adapter for job: {job_id}")
            await self.out_queue.put(output_item)
            self._logger.debug(f"Processor output queue producer put message for job: {job_id}")

    async def put_input_msg(self, item: WorkerInputMessageWrapper[InputMsgT]) -> None:
        # Enqueue the item for processing.
        await self.in_queue.put(item)
