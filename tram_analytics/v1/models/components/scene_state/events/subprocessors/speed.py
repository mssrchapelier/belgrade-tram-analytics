from pydantic import BaseModel, NonNegativeFloat

from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEvent, \
    BaseStatusVehicleEventsContainer


# --- speed events (speed updates) ---

class SpeedsWrapper(BaseModel):
    """
    A container for the calculated raw and smoothed speed values for this vehicle in the associated frame.
    """
    raw: NonNegativeFloat | None
    smoothed: NonNegativeFloat | None


class SpeedUpdateEvent(BaseStatusVehicleEvent):
    """
    A container for the vehicle's speeds plus its matched status,
    i. e. whether the state emitted for this vehicle by the tracking module
    is associated with an actual detection produced by the object detection module for this frame.
    """
    speeds: SpeedsWrapper
    is_matched: bool

class SpeedUpdatesContainer(BaseStatusVehicleEventsContainer[SpeedUpdateEvent]):
    pass
