from typing import override

from archive.v1_1.pipeline.stages.base.servers.server import (
    BaseProcessorConfig, BaseProcessor, BasePersistenceConfig, BaseProcessorServer,
    BaseKeyedRingBufferPersistenceClient
)
from archive.v1_1.pipeline.stages.base.message import FrameMessage, RealtimeRetrievalRequest
from archive.v1_1.pipeline.stages.detection import Detections

class Tracks:
    pass

class TrackingConfig(BaseProcessorConfig):
    pass

class TrackingProcessor(BaseProcessor[Detections, Tracks, TrackingConfig]):

    @override
    async def predict(self, input_item: Detections) -> Tracks:
        raise NotImplementedError()

class TrackingPersistenceConfig(BasePersistenceConfig):
    pass

class TrackingKeyedRingBufferPersistenceClient(BaseKeyedRingBufferPersistenceClient[Tracks]):

    async def retrieve(self, frame_id: str) -> Tracks:
        return self._buffer.get(frame_id)

class TrackingProcessorServer(
    BaseProcessorServer[
        Detections, Tracks, # input, output type
        TrackingConfig, TrackingPersistenceConfig,
        FrameMessage, FrameMessage, # request, response type for prediction
        RealtimeRetrievalRequest, Tracks # request, response type for retrieval
    ]
):

    def __init__(self, *, processor: BaseProcessor[Detections, Tracks, TrackingConfig],
                 persistence_config: TrackingPersistenceConfig,
                 persistence_client: TrackingKeyedRingBufferPersistenceClient) -> None:
        super().__init__(processor=processor, persistence_config=persistence_config)
        self._persistence_client: TrackingKeyedRingBufferPersistenceClient = persistence_client

    @override
    async def _retrieve_inputs(self, request: FrameMessage) -> Detections:
        raise NotImplementedError()

    @override
    async def _persist_result(self, prediction_request: FrameMessage, result: Tracks) -> None:
        await self._persistence_client.store(frame_id=prediction_request.frame_id,
                                             result=result)

    @override
    async def _build_response(self, request: FrameMessage, result: Tracks) -> FrameMessage:
        return FrameMessage.from_message(request)

    @override
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequest) -> Tracks:
        return await self._persistence_client.retrieve(request)