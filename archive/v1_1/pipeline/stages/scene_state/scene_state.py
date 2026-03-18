from typing import override, NamedTuple

from archive.v1_1.pipeline.stages.base.servers.server import (
    BaseProcessorConfig, BaseProcessor, BasePersistenceConfig, BaseProcessorServer,
    BaseKeyedRingBufferPersistenceClient
)
from archive.v1_1.pipeline.stages.base.message import FrameMessage, RealtimeRetrievalRequest
from archive.v1_1.pipeline.stages.vehicle_info import VehicleInfo
from archive.v1_1.pipeline.stages.scene_state.clients import VehicleInfoClient

class SceneEvents:
    # to persist in the database
    pass

class LiveState:
    # to simply return -- persisted only in the cache, NOT in the database
    pass

class SceneState(NamedTuple):
    events: SceneEvents
    live_state: LiveState

class SceneStateConfig(BaseProcessorConfig):
    pass

class SceneStateProcessor(BaseProcessor[VehicleInfo, SceneState, SceneStateConfig]):

    @override
    async def predict(self, input_item: VehicleInfo) -> SceneState:
        raise NotImplementedError()

class SceneStatePersistenceConfig(BasePersistenceConfig):
    pass

class SceneStateKeyedRingBufferPersistenceClient(BaseKeyedRingBufferPersistenceClient[SceneState]):

    async def retrieve_live_state(self, frame_id: str) -> LiveState:
        return self._buffer.get(frame_id).live_state


class SceneStateProcessorServer(
    BaseProcessorServer[
        VehicleInfo, SceneState, # input, output type
        SceneStateConfig, SceneStatePersistenceConfig,
        FrameMessage, FrameMessage, # request, response type for prediction
        RealtimeRetrievalRequest, LiveState # request, response type for retrieval
    ]
):

    def __init__(self, *, processor: BaseProcessor[VehicleInfo, SceneState, SceneStateConfig],
                 persistence_config: SceneStatePersistenceConfig,
                 persistence_client: SceneStateKeyedRingBufferPersistenceClient) -> None:
        super().__init__(processor=processor, persistence_config=persistence_config)
        self._persistence_client: SceneStateKeyedRingBufferPersistenceClient = persistence_client

    @override
    async def _retrieve_inputs(self, request: FrameMessage) -> VehicleInfo:
        raise NotImplementedError()

    @override
    async def _persist_result(self, prediction_request: FrameMessage, result: SceneState) -> None:
        await self._persistence_client.store(frame_id=prediction_request.frame_id,
                                             result=result)

    @override
    async def _build_response(self, request: FrameMessage, result: SceneState) -> FrameMessage:
        return FrameMessage.from_message(request)

    @override
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequest) -> LiveState:
        return await self._persistence_client.retrieve_live_state(request)