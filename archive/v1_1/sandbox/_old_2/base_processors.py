from typing import NamedTuple, Dict, Any, override, Self
from abc import ABC, abstractmethod
import asyncio
from asyncio import Task, TaskGroup, Queue, Lock, QueueFull
from dataclasses import dataclass
from time import perf_counter

from pydantic import BaseModel

# --- synchronous processors ---

class BaseProcessorConfig(BaseModel):
    pass

class BaseSynchronousProcessor[
    ConfigT: BaseProcessorConfig, InputT, OutputT
](ABC):

    def __init__(self, config: ConfigT) -> None:
        self._config: ConfigT = config

    @abstractmethod
    def process_for_frame(self, inputs: InputT) -> OutputT:
        pass

# ... wrapper to launch a synchronous processor in a separate process,
# pass data to/from it through multiprocessing queues,
# and expose an async interface to the wrapper ...

# --- async wrappers ---

class BaseAsyncProcessor[
    ConfigT: BaseProcessorConfig, InputT, OutputT
](ABC):
    # Subclass this DIRECTLY when `process_for_frame` will be making an async call to a processor
    # (not necessarily a per-camera one, may be pooling from many cameras)
    # that is running in its own process and/or is a network resource.
    #
    # Essentially a thin client to call another processor.
    # Many processors of this type may be connected to the same resource.
    # Examples: detection, feature extraction.

    def __init__(self, config: ConfigT) -> None:
        self._config: ConfigT = config

    @abstractmethod
    async def process_for_frame(self, inputs: InputT) -> OutputT:
        pass

class BaseAsyncWrapperForSingleCameraSyncProcessor[
    ConfigT: BaseProcessorConfig, InputT, OutputT
](BaseAsyncProcessor[ConfigT, InputT, OutputT]):
    # Subclass this when the per-camera processor is truly synchronous by nature
    # and will NOT have a dedicated thread, but will instead be run in a blocking way on the wrapping async server.
    # If it is desirable that the synchronous per-camera processor does not block the event loop,
    # then it must be wrapped in its own async server to be run in a separate process (or thread, but hardly useful).
    #
    # Intended to be used for per-camera processors that perform CPU-bound work,
    # but not so much work per camera that the overhead introduced by dedicating a separate process
    # to every per-camera processor would be justified.
    # It is intended that the synchronous processor being connected to is a dedicated one for this camera.
    # Examples: all stateful processors calculating per-scene updateable characteristics
    # (multi-object tracking, reference point calculation etc., scene events and live state).

    def __init__(self, config: ConfigT) -> None:
        super().__init__(config)
        self._sync_processor: BaseSynchronousProcessor[ConfigT, InputT, OutputT] = (
            self._create_synchronous_processor(config)
        )

    @classmethod
    @abstractmethod
    def _create_synchronous_processor(cls, config: ConfigT) -> BaseSynchronousProcessor[ConfigT, InputT, OutputT]:
        pass

    @override
    async def process_for_frame(self, inputs: InputT) -> OutputT:
        return self._sync_processor.process_for_frame(inputs)

class BasePersistenceConfig(BaseModel):
    pass

class BaseProcessorWithPersistence[
    BasicConfigT: BaseProcessorConfig,
    PersistenceConfigT: BasePersistenceConfig,
    InputType, OutputType
](ABC):

    def __init__(self, *, config: BasicConfigT, persistence_config: PersistenceConfigT) -> None:
        self._processor: BaseAsyncProcessor[BasicConfigT, InputType, OutputType] = (
            self._create_inner_processor(config)
        )
        self._persistence_config: PersistenceConfigT = persistence_config
        self._persistence_queue: Queue[OutputType] = Queue()
        self._persistence_worker_task: Task[None] = asyncio.create_task(
            self._persistence_worker(), name="persistence-worker"
        )

    @classmethod
    @abstractmethod
    def _create_inner_processor(cls, config: BasicConfigT) -> BaseAsyncProcessor[BasicConfigT, InputType, OutputType]:
        pass

    @abstractmethod
    async def _persist_outputs(self, outputs: OutputType) -> None:
        pass

    async def _persistence_worker(self) -> None:
        while True:
            outputs: OutputType = await self._persistence_queue.get()
            # TODO: handle exceptions
            await self._persist_outputs(outputs)
            self._persistence_queue.task_done()

    @abstractmethod
    async def retrieve(self, frame_id: str) -> OutputType:
        pass

    async def process_for_frame(self, inputs: InputType) -> OutputType:
        outputs: OutputType = await self._processor.process_for_frame(inputs)
        # TODO: handle full queue, add a timeout possibly
        await self._persistence_queue.put(outputs)
        return outputs

# --- message processors ---

@dataclass(frozen=True, slots=True, kw_only=True)
class FrameMessage:
    """
    The format of messages to be passed for every item successfully processed by the upstream producer,
    to be fetched by the downstream consumer.
    """
    session_id: str
    # The sequence number of the frame (for this camera and this session).
    # Is meant to be strictly increasing.
    seq_num: int
    frame_id: str
    # The PTS of the frame, in POSIX seconds.
    frame_pts: float
    # The timestamp of the message's creation time.
    message_ts: float

    @classmethod
    def from_message(cls, msg: Self) -> Self:
        # copies msg, but inserts the current timestamp as message_ts
        return cls(session_id=msg.session_id,
                   seq_num=msg.seq_num,
                   frame_id=msg.frame_id,
                   frame_pts=msg.frame_pts,
                   message_ts=perf_counter())

class BaseMessageProcessor[
    BasicConfigT: BaseProcessorConfig,
    PersistenceConfigT: BasePersistenceConfig,
    InputDataT, OutputDataT
](ABC):

    def __init__(
            self, *, processor_config: BasicConfigT, persistence_config: PersistenceConfigT
    ) -> None:
        self._processor_with_persistence: BaseProcessorWithPersistence[
            BasicConfigT, PersistenceConfigT, InputDataT, OutputDataT
        ] = self._create_processor_with_persistence(processor_config=processor_config,
                                                    persistence_config=persistence_config)

    @classmethod
    @abstractmethod
    def _create_processor_with_persistence(
            cls, *, processor_config: BasicConfigT, persistence_config: PersistenceConfigT
    ) -> BaseProcessorWithPersistence[
        BasicConfigT, PersistenceConfigT, InputDataT, OutputDataT
    ]:
        pass

    @abstractmethod
    async def _retrieve_input(self, frame_id: str) -> InputDataT:
        # send a request to a different service
        pass

    async def process_for_frame(self, input_message: FrameMessage) -> FrameMessage:
        input_data: InputDataT = await self._retrieve_input(input_message.frame_id)
        output_data: OutputDataT = await self._processor_with_persistence.process_for_frame(input_data)
        out_message: FrameMessage = FrameMessage.from_message(input_message)
        return out_message

class BaseMessageQueueProcessorConfig(BaseModel):
    # The maximum amount of time elapsed since the PTS of the last frame
    # this processor has processed.
    # Above this threshold, the processor's internal state is reset
    # (if any reset steps are defined) before processing this frame.
    max_missed_time_s: float

class BaseMessageQueueHandler[MsgQueueConfigT: BaseMessageQueueProcessorConfig](ABC):

    def __init__(self, *, msg_queue_config: MsgQueueConfigT,
                 in_queue: Queue[FrameMessage],
                 out_queue: Queue[FrameMessage], out_queue_lock: Lock) -> None:
        self._config: MsgQueueConfigT = msg_queue_config

        self._in_queue: Queue[FrameMessage] = in_queue
        self._out_queue: Queue[FrameMessage] = out_queue
        self._out_queue_lock: Lock = out_queue_lock

        # The PTS of the last received frame.
        self._last_frame_pts: float | None = None
        self._last_session_id: str | None = None

    async def _put_into_out_queue_with_drop_head(self, output_msg: FrameMessage) -> None:
        """
        Attempt to put the message into the output queue.
        If it is full, drop the oldest item in the queue (the "drop-head" behaviour),
        then put the message.
        All operations are performed under the queue's lock.
        """
        # acquiring the lock: not strictly necessary because control
        # is not yielded back to the event loop until return,
        # but still doing this explicitly
        async with self._out_queue_lock:
            try:
                self._out_queue.put_nowait(output_msg)
            except QueueFull:
                oldest_item: FrameMessage = self._out_queue.get_nowait()
                # TODO: log the dropping of oldest_item (or send a message to the orchestrator)
                # should now succeed
                self._out_queue.put_nowait(output_msg)

    def _is_recent_frame(self, input_message: FrameMessage) -> bool:
        cur_pts: float = input_message.frame_pts
        last_pts: float | None = self._last_frame_pts
        if last_pts is None:
            return True
        if cur_pts <= last_pts:
            raise RuntimeError(f"Encountered non-increasing frame PTS: "
                               f"last processed {last_pts}, got current {cur_pts}")
        diff: float = cur_pts - last_pts
        is_above_threshold: bool = diff >= self._config.max_missed_time_s
        return not is_above_threshold

    def _is_changed_session_id(self, input_msg: FrameMessage) -> bool:
        return (self._last_session_id is not None
                and input_msg.session_id != self._last_session_id)

class BaseMessageQueueProcessor[
    BasicConfigT: BaseProcessorConfig,
    PersistenceConfigT: BasePersistenceConfig,
    MsgQueueConfigT: BaseMessageQueueProcessorConfig,
    InputDataT, OutputDataT
](BaseMessageQueueHandler[MsgQueueConfigT]):

    """
    A processor that consumes an incoming message from one queue, **waits until the item is processed**,
    then puts the output message into the other queue.
    """

    def __init__(self,
                 *, processor_config: BasicConfigT,
                 persistence_config: PersistenceConfigT,
                 msg_queue_config: MsgQueueConfigT,
                 in_queue: Queue[FrameMessage],
                 out_queue: Queue[FrameMessage], out_queue_lock: Lock) -> None:
        super().__init__(msg_queue_config=msg_queue_config,
                         in_queue=in_queue,
                         out_queue=out_queue,
                         out_queue_lock=out_queue_lock)
        self._processor: BaseMessageProcessor[
            BasicConfigT, PersistenceConfigT, InputDataT, OutputDataT
        ] = self._create_message_processor(processor_config=processor_config,
                                           persistence_config=persistence_config)

    @classmethod
    @abstractmethod
    def _create_message_processor(
            cls, *, processor_config: BasicConfigT, persistence_config: PersistenceConfigT
    ) -> BaseMessageProcessor[
        BasicConfigT, PersistenceConfigT, InputDataT, OutputDataT
    ]:
        pass

    @abstractmethod
    async def _reset_state(self) -> None:
        """
        Reset the underlying processor's state. The exact behaviour is to be specified in subclasses.
        - For processors that are not stateful (detection, feature extraction): does nothing.
        - For tracking, vehicle info: resets the internal state, but does not persist anything.
        - For events + live state: resetting the event processor's state emits end events
          which must be persisted.
        """
        pass

    async def _reset_state_and_log(self) -> None:
        """
        Reset the underlying processor's state. The exact behaviour is to be specified in subclasses.
        - For processors that are not stateful (detection, feature extraction): does nothing.
        - For tracking, vehicle info: resets the internal state, but does not persist anything.
        - For events + live state: resetting the event processor's state emits end events
          which must be persisted.
        """
        await self._reset_state()
        # TODO: log all resets

    async def _check_and_reset_state(self, input_msg: FrameMessage) -> None:
        """
        Resets this processor's state if any of the following conditions hold:

        1. The difference between this frame's PTS and the stored PTS of the last processed frame
        (if any frames have been processed) is larger than the specified threshold `max_missed_time_s`
        (in this processor's config).

        2. The session ID has changed (if any frames have been processed).
        """
        if self._is_changed_session_id(input_msg) or not self._is_recent_frame(input_msg):
            await self._reset_state_and_log()

    async def run(self) -> None:
        while True:
            input_msg: FrameMessage = await self._in_queue.get()
            await self._check_and_reset_state(input_msg)
            output_msg: FrameMessage = await self._processor.process_for_frame(input_msg)
            await self._put_into_out_queue_with_drop_head(output_msg)
            self._last_frame_pts = input_msg.frame_pts
