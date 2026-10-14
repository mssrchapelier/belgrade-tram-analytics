from datetime import datetime

from pydantic import BaseModel, Field

from tram_analytics.v1.models.components.scene_state.live_state.vehicles import AgnosticVehiclesContainer, \
    VehiclesContainer
from tram_analytics.v1.models.components.scene_state.live_state.zones import AgnosticZonesContainer, ZonesContainer
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import (
    SceneEventsConfig
)


# --- master data model for the live state ---

class LiveStateMetadata(BaseModel):
    """
    Metadata for the live state object associated with a specific frame.
    """
    camera_id: str = Field(description="The unique ID of the camera.")
    frame_id: str = Field(description="The unique ID of the frame with which this state is associated.")
    frame_timestamp: datetime = Field(description="The timestamp associated with the frame.")
    server_settings: SceneEventsConfig = Field(description="The server-side computation settings used for this frame.")

class BaseLiveAnalyticsState(BaseModel):
    api_version: str = "1.0.0"
    metadata: LiveStateMetadata

class AgnosticLiveAnalyticsState(BaseLiveAnalyticsState):
    """
    Live state object that does not distinguish between the types of vehicles 
    (i. e. trams/cars) and zones (i. e. tram stops, tram tracks, car intrusion zones)
    in its structure.
    Contains the same data, just in a structure that is flatter in that regard.
    """
    zones: AgnosticZonesContainer
    vehicles: AgnosticVehiclesContainer

class LiveAnalyticsState(BaseLiveAnalyticsState):
    """
    A container for the analytical data regarding the state of the scene
    as associated with a specific camera frame.
    """
    zones: ZonesContainer
    vehicles: VehiclesContainer
