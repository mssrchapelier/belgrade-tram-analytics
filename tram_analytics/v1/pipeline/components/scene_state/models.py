from pydantic import BaseModel

from tram_analytics.v1.models.components.scene_state.events.scene_events import (
    FrameMetadataForEvents, SceneEventsWrapper
)
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState


class SceneState(BaseModel):
    frame_metadata: FrameMetadataForEvents
    scene_events: SceneEventsWrapper
    live_state: LiveAnalyticsState