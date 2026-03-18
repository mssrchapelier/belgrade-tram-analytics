from typing import override, NamedTuple

from archive.v1_1.pipeline.stages.base.servers.server import (
    BaseProcessorConfig, BaseProcessor, BasePersistenceConfig, BaseProcessorServer
)
from archive.v1_1.pipeline.stages.base.message import FrameMessage, RealtimeRetrievalRequest
from archive.v1_1.pipeline.stages.frame_ingestion import RawFrame
from archive.v1_1.pipeline.stages.detection import Detections
from archive.v1_1.pipeline.stages.tracking import Tracks
from archive.v1_1.pipeline.stages.vehicle_info import VehicleInfo
from archive.v1_1.pipeline.stages.scene_state.scene_state import LiveState

class AnnotatedFrame:
    pass

class VisualisationInputs(NamedTuple):
    raw_frame: RawFrame
    detections: Detections
    tracks: Tracks
    vehicle_info: VehicleInfo
    live_state: LiveState

class VisualisationConfig(BaseProcessorConfig):
    pass

class VisualisationProcessor(BaseProcessor[VisualisationInputs, AnnotatedFrame, VisualisationConfig]):

    @override
    async def predict(self, input_item: VisualisationInputs) -> AnnotatedFrame:
        raise NotImplementedError()

class VisualisationPersistenceConfig(BasePersistenceConfig):
    pass

class VisualisationProcessorServer(
    BaseProcessorServer[
        VisualisationInputs, AnnotatedFrame, # input, output type
        VisualisationConfig, VisualisationPersistenceConfig,
        FrameMessage, FrameMessage, # request, response type for prediction
        RealtimeRetrievalRequest, AnnotatedFrame # request, response type for retrieval
    ]
):
    @override
    async def _retrieve_inputs(self, request: FrameMessage) -> VisualisationInputs:
        raise NotImplementedError()

    @override
    async def _persist_result(self, prediction_request: FrameMessage, result: AnnotatedFrame) -> None:
        raise NotImplementedError()

    @override
    async def _build_response(self, request: FrameMessage, result: AnnotatedFrame) -> FrameMessage:
        raise NotImplementedError()

    @override
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequest) -> AnnotatedFrame:
        raise NotImplementedError()