from typing import override

from archive.v1_1.pipeline.stages.base.servers.server import (
    BaseProcessorConfig, BaseProcessor, BasePersistenceConfig, BaseProcessorServer,
    BaseKeyedRingBufferPersistenceClient
)
from archive.v1_1.pipeline.stages.base.message import FrameMessage, RealtimeRetrievalRequest
from archive.v1_1.pipeline.stages.frame_ingestion import RawFrame

class Detections:
    pass

class DetectionConfig(BaseProcessorConfig):
    pass

class DetectionProcessor(BaseProcessor[RawFrame, Detections, DetectionConfig]):

    @override
    async def predict(self, input_item: RawFrame) -> Detections:
        raise NotImplementedError()

class DetectionKeyedRingBufferPersistenceClient(BaseKeyedRingBufferPersistenceClient[Detections]):

    async def retrieve(self, frame_id: str) -> Detections:
        return self._buffer.get(frame_id)

class DetectionPersistenceConfig(BasePersistenceConfig):
    pass

class DetectionProcessorServer(
    BaseProcessorServer[
        RawFrame, Detections, # input, output type
        DetectionConfig, DetectionPersistenceConfig,
        FrameMessage, FrameMessage, # request, response type for prediction
        RealtimeRetrievalRequest, Detections # request, response type for retrieval
    ]
):

    def __init__(self, *, processor: BaseProcessor[RawFrame, Detections, DetectionConfig],
                 persistence_config: DetectionPersistenceConfig,
                 persistence_client: DetectionKeyedRingBufferPersistenceClient) -> None:
        super().__init__(processor=processor, persistence_config=persistence_config)
        self._persistence_client: DetectionKeyedRingBufferPersistenceClient = persistence_client

    @override
    async def _retrieve_inputs(self, request: FrameMessage) -> RawFrame:
        raise NotImplementedError()

    @override
    async def _persist_result(self, prediction_request: FrameMessage, result: Detections) -> None:
        await self._persistence_client.store(frame_id=prediction_request.frame_id,
                                             result=result)

    @override
    async def _build_response(self, request: FrameMessage, result: Detections) -> FrameMessage:
        return FrameMessage.from_message(request)

    @override
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequest) -> Detections:
        return await self._persistence_client.retrieve(request)