from pydantic import BaseModel

from archive.v1.src.api_server.models.scene_state_settings import SpeedType
from tram_analytics.v1.pipeline.components.scene_state.live_state_updater.config.zones_config import ZonesConfig


class MotionStatusDeterminationSettings(BaseModel):
    speed_type_for_motion_status_determination: SpeedType
    # speed, in meters per second
    # TODO: rename the "ms" part, can be confused with milliseconds
    is_stationary_speed_threshold_ms: float

class SceneEventsConfig(BaseModel):
    stationary_global: MotionStatusDeterminationSettings


class SceneStateUpdaterConfig(BaseModel):
    scene_events: SceneEventsConfig
    zones: ZonesConfig
