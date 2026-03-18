from typing import NamedTuple, Any, Dict, override, Iterator
from dataclasses import dataclass
from abc import ABC, abstractmethod
from multiprocessing import Queue as MpQueue
from multiprocessing.queues import Queue as MpQueueType
import asyncio
from asyncio import Queue as AsyncQueue, Task, TaskGroup

class BaseSocket[T](ABC):

    def __init__(self, address: str) -> None:
        self._address: str = address

class Socket[T](BaseSocket[T]):

    def send(self, payload: T) -> None:
        raise NotImplementedError()

    def recv(self) -> T:
        raise NotImplementedError()

class AsyncSocket[T](BaseSocket[T]):

    async def send(self, payload: T) -> None:
        raise NotImplementedError()

    async def recv(self) -> T:
        raise NotImplementedError()

class MessagePayload(NamedTuple):
    camera_id: str
    seq_num: int
    frame_id: str

class RawFrame:
    pass

class SingleCameraFrameGetterConfig:
    pass

class FrameGetterPayload(NamedTuple):
    camera_id: str
    frame_id: str
    raw_frame: RawFrame

class SingleCameraFrameGetter:

    def __init__(self, config: SingleCameraFrameGetterConfig, *, out_address: str) -> None:
        self._config: SingleCameraFrameGetterConfig = config
        self._out_socket: Socket[MessagePayload] = Socket(out_address)

    # worker process
    def run(self) -> None:
        raise NotImplementedError()

class FrameGetterConfig:
    pass

class FrameGetter:

    def __init__(self, config: FrameGetterConfig, *, out_address: str) -> None:
        self._config: FrameGetterConfig = config
        # camera id -> processor
        self._processors: Dict[str, SingleCameraFrameGetter] = dict()
        self._out_address: str = out_address

    def add_camera(self, *, camera_id: str, config: SingleCameraFrameGetterConfig) -> None:
        self._processors[camera_id] = SingleCameraFrameGetter(config, out_address=self._out_address)

    def start_camera(self, camera_id: str) -> None:
        raise NotImplementedError()

    def stop_camera(self, camera_id: str) -> None:
        raise NotImplementedError()

# --- downstream processors ---

class BaseSingleCameraProcessorConfig:
    pass

class BaseSingleCameraProcessor[
    Config: BaseSingleCameraProcessorConfig,
    InputType, OutputType
](ABC):

    def __init__(self, config: Config) -> None:
        self._config: Config = config

    @abstractmethod
    async def process_for_frame(self, inputs: InputType) -> OutputType:
        pass

class BaseProcessorConfig:
    pass

class BaseProcessor[
    SingleCameraProcessorConfig: BaseSingleCameraProcessorConfig,
    ProcessorConfig: BaseProcessorConfig,
    InputType, OutputType
](ABC):

    def __init__(self, config: ProcessorConfig) -> None:
        # camera id -> processor
        self._config: ProcessorConfig = config
        self._camera_processors: Dict[
            str, BaseSingleCameraProcessor[SingleCameraProcessorConfig, InputType, OutputType]
        ] = dict()

    @classmethod
    @abstractmethod
    def _get_new_single_camera_processor(
            cls, config: SingleCameraProcessorConfig
    ) -> BaseSingleCameraProcessor[SingleCameraProcessorConfig, InputType, OutputType]:
        pass

    def add_for_camera(self, *, camera_id: str, config: SingleCameraProcessorConfig) -> None:
        new_processor: BaseSingleCameraProcessor[SingleCameraProcessorConfig, InputType, OutputType] = (
            self._get_new_single_camera_processor(config)
        )
        self._camera_processors[camera_id] = new_processor

    async def process_for_frame(self, inputs: InputType, *, camera_id: str) -> OutputType:
        return await self._camera_processors[camera_id].process_for_frame(inputs)

class BaseContinuousProcessor[
    SingleCameraProcessorConfig: BaseSingleCameraProcessorConfig,
    ProcessorConfig: BaseProcessorConfig,
    InputType, OutputType
](BaseProcessor[SingleCameraProcessorConfig, ProcessorConfig, InputType, OutputType], ABC):

    def __init__(self, config: ProcessorConfig,
                 *, in_queue: AsyncQueue[MessagePayload],
                 out_queue: AsyncQueue[MessagePayload]) -> None:
        super().__init__(config)
        self._in_queue: AsyncQueue[InputType] = in_queue
        self._out_queue: AsyncQueue[OutputType] = out_queue

    async def run(self) -> None:
        while True:
            inputs: InputType = await self._in_queue.get()
            outputs: OutputType = await self.process_for_frame(inputs)
            await self._out_queue.put(outputs)

# --- detection ---

class DetectionsTracksSingleCameraProcessorConfig(BaseSingleCameraProcessorConfig):
    pass

class DetectionsTracks:
    pass

class DetectionsTracksSingleCameraProcessor(
    BaseSingleCameraProcessor[DetectionsTracksSingleCameraProcessorConfig, RawFrame, DetectionsTracks]
):

    @override
    async def process_for_frame(self, inputs: RawFrame) -> DetectionsTracks:
        # send to appropriate workers, get detections
        raise NotImplementedError()

class DetectionsTracksProcessorConfig(BaseProcessorConfig):
    pass

class DetectionsTracksProcessor(
    BaseProcessor[DetectionsTracksSingleCameraProcessorConfig, DetectionsTracksProcessorConfig, RawFrame, DetectionsTracks]
):

    @override
    @classmethod
    def _get_new_single_camera_processor(
            cls, config: DetectionsTracksSingleCameraProcessorConfig
    ) -> DetectionsTracksSingleCameraProcessor:
        return DetectionsTracksSingleCameraProcessor(config)

# --- derived info ---
# reference points, speeds, zone assignments

class DerivedVehicleInfoForFrame:
    pass

class DerivedVehicleInfoSingleCameraProcessorConfig(BaseSingleCameraProcessorConfig):
    pass

class DerivedVehicleInfoSingleCameraProcessor(
    BaseSingleCameraProcessor[
        DerivedVehicleInfoSingleCameraProcessorConfig, DetectionsTracks, DerivedVehicleInfoForFrame
    ]
):

    @override
    def process_for_frame(self, inputs: DetectionsTracks) -> DerivedVehicleInfoForFrame:
        raise NotImplementedError()

class DerivedVehicleInfoProcessorConfig(BaseProcessorConfig):
    pass

class DerivedVehicleInfoProcessor(
    BaseProcessor[
        DerivedVehicleInfoSingleCameraProcessorConfig, DerivedVehicleInfoProcessorConfig,
        DetectionsTracks, DerivedVehicleInfoForFrame
    ]
):

    @override
    @classmethod
    def _get_new_single_camera_processor(
            cls, config: DerivedVehicleInfoSingleCameraProcessorConfig
    ) -> DerivedVehicleInfoSingleCameraProcessor:
        return DerivedVehicleInfoSingleCameraProcessor(config)

# --- events and live state ---

class EventsForFrame:
    pass

class LiveStateForFrame:
    pass

class EventsAndLiveStateForFrame(NamedTuple):
    events: EventsForFrame
    live_state: LiveStateForFrame

class SceneStateSingleCameraProcessorConfig(BaseSingleCameraProcessorConfig):
    pass

class SceneStateSingleCameraProcessor(
    BaseSingleCameraProcessor[
        SceneStateSingleCameraProcessorConfig, DerivedVehicleInfoForFrame, EventsAndLiveStateForFrame
    ]
):

    @override
    def process_for_frame(self, inputs: DerivedVehicleInfoForFrame) -> EventsAndLiveStateForFrame:
        raise NotImplementedError()

class SceneStateProcessorConfig(BaseProcessorConfig):
    pass

class SceneStateProcessor(
    BaseProcessor[
        SceneStateSingleCameraProcessorConfig, SceneStateProcessorConfig,
        DerivedVehicleInfoForFrame, EventsAndLiveStateForFrame
    ]
):
    @override
    @classmethod
    def _get_new_single_camera_processor(
            cls, config: SceneStateSingleCameraProcessorConfig
    ) -> SceneStateSingleCameraProcessor:
        return SceneStateSingleCameraProcessor(config)

# --- visualisation ---

class VisualiserInputs(NamedTuple):
    raw_frame: RawFrame
    detections_tracks: DetectionsTracks
    derived_vehicle_info: DerivedVehicleInfoForFrame
    live_state: LiveStateForFrame

class AnnotatedImage:
    pass

class SingleCameraVisualiserConfig(BaseSingleCameraProcessorConfig):
    pass

class SingleCameraVisualiser(
    BaseSingleCameraProcessor[SingleCameraVisualiserConfig, VisualiserInputs, AnnotatedImage]
):

    @override
    def process_for_frame(self, inputs: VisualiserInputs) -> AnnotatedImage:
        raise NotImplementedError()

class VisualiserConfig(BaseProcessorConfig):
    pass

class Visualiser(
    BaseProcessor[SingleCameraVisualiserConfig, VisualiserConfig, VisualiserInputs, AnnotatedImage]
):
    @override
    @classmethod
    def _get_new_single_camera_processor(
            cls, config: SingleCameraVisualiserConfig
    ) -> SingleCameraVisualiser:
        return SingleCameraVisualiser(config)

# --- pipeline ---

class PipelineConfig:
    detections_tracks: DetectionsTracksProcessorConfig
    derived_info: DerivedVehicleInfoProcessorConfig
    scene_state: SceneStateProcessorConfig

class PipelineStages(NamedTuple):
    detections_tracks: DetectionsTracksProcessor
    derived_info: DerivedVehicleInfoProcessor
    scene_state: SceneStateProcessor

class PipelineQueues(NamedTuple):
    ingestion_to_detections_tracks: AsyncQueue[RawFrame]
    detections_tracks_to_derived_info: AsyncQueue[DetectionsTracks]
    derived_info_to_scene_state: AsyncQueue[DerivedVehicleInfoForFrame]
    from_scene_state: AsyncQueue[EventsAndLiveStateForFrame]

class Pipeline:
    # DetectionsTracksProcessor
    # DerivedVehicleInfoProcessor
    # SceneStateProcessor

    def __init__(self, config: PipelineConfig, *, in_address: str, out_address: str) -> None:
        self._in_socket: AsyncSocket[FrameGetterPayload] = AsyncSocket(in_address)
        self._out_socket: AsyncSocket[LiveStateForFrame] = AsyncSocket(out_address)

        self._stages: PipelineStages = PipelineStages(
            detections_tracks=DetectionsTracksProcessor(config.detections_tracks),
            derived_info=DerivedVehicleInfoProcessor(config.derived_info),
            scene_state=SceneStateProcessor(config.scene_state)
        )
        self._queues: PipelineQueues = PipelineQueues(
            ingestion_to_detections_tracks=AsyncQueue(),
            detections_tracks_to_derived_info=AsyncQueue(),
            derived_info_to_scene_state=AsyncQueue(),
            from_scene_state=AsyncQueue()
        )

    async def _forward_from_ingestion_to_detections_tracks(self) -> None:
        while True:
            payload: FrameGetterPayload = await self._in_socket.recv()
            await self._queues.ingestion_to_detections_tracks.put(payload.raw_frame)

    async def _dequeue_and_send_results(self) -> None:
        while True:
            results: EventsAndLiveStateForFrame = await self._queues.from_scene_state.get()
            live_state: LiveStateForFrame = results.live_state
            await self._out_socket.send(live_state)

class Pipeline_Old:

    def __init__(self, *, in_address: str, out_address: str):
        self._in_socket: AsyncSocket[FrameGetterPayload] = AsyncSocket(in_address)
        self._out_socket: AsyncSocket[LiveStateForFrame] = AsyncSocket(out_address)
        # ready results
        self._result_queue: AsyncQueue[LiveStateForFrame] = AsyncQueue()

    async def _process_frame(self, *, camera_id: str, raw_frame: RawFrame) -> LiveStateForFrame:
        raise NotImplementedError()

    async def _process_frame_and_enqueue(self, *, camera_id: str, raw_frame: RawFrame):
        live_state: LiveStateForFrame = await self._process_frame(camera_id=camera_id, raw_frame=raw_frame)
        await self._result_queue.put(live_state)

    async def _recv_and_enqueue(self) -> None:
        while True:
            inputs: FrameGetterPayload = await self._in_socket.recv()
            asyncio.create_task(
                self._process_frame_and_enqueue(camera_id=inputs.camera_id, raw_frame=inputs.raw_frame)
            )

    async def _dequeue_and_send(self) -> None:
        while True:
            result: LiveStateForFrame = await self._result_queue.get()
            await self._out_socket.send(result)

    async def run(self) -> None:
        async with TaskGroup() as tg: # type: TaskGroup
            recv_enqueue_task: Task[None] = asyncio.create_task(self._recv_and_enqueue())
            dequeue_send_task: Task[None] = asyncio.create_task(self._dequeue_and_send())