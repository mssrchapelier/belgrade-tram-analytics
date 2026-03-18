from typing import override, NamedTuple
from archive.v1_1.sandbox._old_2.base_processors import (
    BaseProcessorConfig, BaseAsyncProcessor, BaseProcessorWithPersistence
)

# vehicle info (synchronous)
# events and live state (synchronous)
# tracking
# - wrappers with persistence
# - common wrapper with persistence
# - common server

# --- objects ---

class DetectionsTracksForFrame:
    pass

class VehicleInfoForFrame:
    pass

class EventsForFrame:
    pass

class LiveStateForFrame:
    pass

class EventsAndLiveStateForFrame(NamedTuple):
    events: EventsForFrame
    live_state: LiveStateForFrame

# --- synchronous ---

class VehicleInfoProcessorConfig(BaseProcessorConfig):
    pass

class SingleCameraVehicleInfoProcessor(
    BaseAsyncProcessor[VehicleInfoProcessorConfig, DetectionsTracksForFrame, VehicleInfoForFrame]
):

    @override
    def process_for_frame(self, inputs: DetectionsTracksForFrame) -> VehicleInfoForFrame:
        raise NotImplementedError()

class SceneStateProcessorConfig(BaseProcessorConfig):
    pass

class SingleCameraSceneStateProcessor(
    BaseAsyncProcessor[SceneStateProcessorConfig, VehicleInfoForFrame, EventsAndLiveStateForFrame]
):

    @override
    def process_for_frame(self, inputs: VehicleInfoForFrame) -> EventsAndLiveStateForFrame:
        raise NotImplementedError()

# --- async with persistence ---

class SingleCameraVehicleInfoRetrievalRequestPayload:
    pass

class SingleCameraVehicleInfoProcessorPersistenceConfig:
    pass

class SingleCameraVehicleInfoProcessorWithPersistence(
    BaseProcessorWithPersistence[
        VehicleInfoProcessorConfig,
        SingleCameraVehicleInfoProcessorPersistenceConfig,
        DetectionsTracksForFrame, VehicleInfoForFrame,
        SingleCameraVehicleInfoRetrievalRequestPayload
    ]
):

    @override
    @classmethod
    def _create_inner_processor(
            cls, config: VehicleInfoProcessorConfig
    ) -> SingleCameraVehicleInfoProcessor:
        return SingleCameraVehicleInfoProcessor(config)

    @override
    async def _persist_outputs(self, outputs: VehicleInfoForFrame) -> None:
        raise NotImplementedError()

    @override
    async def retrieve(self, request: SingleCameraVehicleInfoRetrievalRequestPayload) -> VehicleInfoForFrame:
        raise NotImplementedError()
    
class SingleCameraSceneStateRetrievalRequestPayload:
    pass

class SingleCameraEventsRetrievalRequestPayload:
    pass

class SingleCameraLiveStateRetrievalRequestPayload:
    pass

class SingleCameraSceneStateProcessorPersistenceConfig:
    pass

class SingleCameraSceneStateProcessorWithPersistence(
    BaseProcessorWithPersistence[
        SceneStateProcessorConfig, SingleCameraSceneStateProcessorPersistenceConfig,
        VehicleInfoForFrame, EventsAndLiveStateForFrame,
        SingleCameraSceneStateRetrievalRequestPayload
    ]
):

    @override
    @classmethod
    def _create_inner_processor(
            cls, config: SceneStateProcessorConfig
    ) -> SingleCameraSceneStateProcessor:
        return SingleCameraSceneStateProcessor(config)

    @override
    async def _persist_outputs(self, outputs: EventsAndLiveStateForFrame) -> None:
        raise NotImplementedError()

    @override
    async def retrieve(self, request: SingleCameraSceneStateRetrievalRequestPayload) -> EventsAndLiveStateForFrame:
        raise NotImplementedError()

    async def retrieve_events(self, request: SingleCameraEventsRetrievalRequestPayload) -> EventsForFrame:
        raise NotImplementedError()

    async def retrieve_live_state(self, request: SingleCameraLiveStateRetrievalRequestPayload) -> LiveStateForFrame:
        raise NotImplementedError()

class DerivedInfoProcessor:
    # chains vehicle info and scene state processor synchronously,
    # each with its own persistence

    def __init__(self,
                 *, vehicle_info_processor_sync_config: VehicleInfoProcessorConfig,
                 vehicle_info_processor_persistence_config: SingleCameraVehicleInfoProcessorPersistenceConfig,
                 scene_state_processor_sync_config: SceneStateProcessorConfig,
                 scene_state_processor_persistence_config: SingleCameraSceneStateProcessorPersistenceConfig) -> None:
        self._vehicle_info_processor: SingleCameraVehicleInfoProcessorWithPersistence = (
            SingleCameraVehicleInfoProcessorWithPersistence(
                sync_config=vehicle_info_processor_sync_config,
                persistence_config=vehicle_info_processor_persistence_config
            )
        )
        self._scene_state_processor: SingleCameraSceneStateProcessorWithPersistence = (
            SingleCameraSceneStateProcessorWithPersistence(
                sync_config=scene_state_processor_sync_config,
                persistence_config=scene_state_processor_persistence_config
            )
        )

    async def process_for_frame(self, inputs: DetectionsTracksForFrame) -> LiveStateForFrame:
        vehicle_info_for_frame: VehicleInfoForFrame = await (
            self._vehicle_info_processor.process_for_frame(inputs)
        )
        events_and_live_state_for_frame: EventsAndLiveStateForFrame = await (
            self._scene_state_processor.process_for_frame(vehicle_info_for_frame)
        )
        live_state: LiveStateForFrame = events_and_live_state_for_frame.live_state
        return live_state

    async def retrieve_vehicle_info(
            self, request: SingleCameraVehicleInfoRetrievalRequestPayload
    ) -> VehicleInfoForFrame:
        return await self._vehicle_info_processor.retrieve(request)

    async def retrieve_events(self, request: SingleCameraEventsRetrievalRequestPayload) -> EventsForFrame:
        return await self._scene_state_processor.retrieve_events(request)

    async def retrieve_live_state(self, request: SingleCameraLiveStateRetrievalRequestPayload) -> LiveStateForFrame:
        return await self._scene_state_processor.retrieve_live_state(request)