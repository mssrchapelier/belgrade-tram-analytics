from datetime import datetime

from pydantic import BaseModel

from tram_analytics.v1.models.components.scene_state.live_state.vehicles import AgnosticVehiclesContainer, \
    VehiclesContainer
from tram_analytics.v1.models.components.scene_state.live_state.zones import AgnosticZonesContainer, ZonesContainer
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import (
    SceneEventsConfig
)


# --- master data model for the live state ---

class LiveStateMetadata(BaseModel):
    # the unique ID of the camera
    camera_id: str
    # the unique ID of the current frame
    frame_id: str
    # the timestamp associated with the current frame
    frame_timestamp: datetime
    # the server-side computation settings used for this frame
    server_settings: SceneEventsConfig

class BaseLiveAnalyticsState(BaseModel):
    api_version: str = "1.0.0"
    metadata: LiveStateMetadata

class AgnosticLiveAnalyticsState(BaseLiveAnalyticsState):
    zones: AgnosticZonesContainer
    vehicles: AgnosticVehiclesContainer

class LiveAnalyticsState(BaseLiveAnalyticsState):
    zones: ZonesContainer
    vehicles: VehiclesContainer
