from typing import override

from archive.v1_1.pipeline.stages.base.servers.server import (
    BaseProcessorConfig, BaseProcessor, BasePersistenceConfig, BaseProcessorServer,
    BaseKeyedRingBufferPersistenceClient
)
from archive.v1_1.pipeline.stages.base.message import FrameMessage, RealtimeRetrievalRequest
from archive.v1_1.pipeline.stages.tracking import Tracks

class VehicleInfo:
    pass

class VehicleInfoConfig(BaseProcessorConfig):
    pass

class VehicleInfoProcessor(BaseProcessor[Tracks, VehicleInfo, VehicleInfoConfig]):

    @override
    async def predict(self, input_item: Tracks) -> VehicleInfo:
        raise NotImplementedError()

class VehicleInfoPersistenceConfig(BasePersistenceConfig):
    pass

class VehicleInfoKeyedRingBufferPersistenceClient(BaseKeyedRingBufferPersistenceClient[VehicleInfo]):

    async def retrieve(self, frame_id: str) -> VehicleInfo:
        return self._buffer.get(frame_id)

class VehicleInfoProcessorServer(
    BaseProcessorServer[
        Tracks, VehicleInfo, # input, output type
        VehicleInfoConfig, VehicleInfoPersistenceConfig,
        FrameMessage, FrameMessage, # request, response type for prediction
        RealtimeRetrievalRequest, VehicleInfo # request, response type for retrieval
    ]
):

    def __init__(self, *, processor: BaseProcessor[Tracks, VehicleInfo, VehicleInfoConfig],
                 persistence_config: VehicleInfoPersistenceConfig,
                 persistence_client: VehicleInfoKeyedRingBufferPersistenceClient) -> None:
        super().__init__(processor=processor, persistence_config=persistence_config)
        self._persistence_client: VehicleInfoKeyedRingBufferPersistenceClient = persistence_client

    @override
    async def _retrieve_inputs(self, request: FrameMessage) -> Tracks:
        raise NotImplementedError()

    @override
    async def _persist_result(self, prediction_request: FrameMessage, result: VehicleInfo) -> None:
        await self._persistence_client.store(frame_id=prediction_request.frame_id,
                                             result=result)

    @override
    async def _build_response(self, request: FrameMessage, result: VehicleInfo) -> FrameMessage:
        return FrameMessage.from_message(request)

    @override
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequest) -> VehicleInfo:
        return await self._persistence_client.retrieve(request)