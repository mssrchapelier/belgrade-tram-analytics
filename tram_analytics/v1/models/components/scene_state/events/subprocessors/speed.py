from pydantic import BaseModel, NonNegativeFloat, Field

from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEvent, \
    BaseStatusVehicleEventsContainer


# --- speed events (speed updates) ---

class SpeedsWrapper(BaseModel):
    """
    A container for the calculated raw and smoothed speed values for this vehicle in the associated frame.
    """
    raw: NonNegativeFloat | None = Field(
        description="The raw (non-smoothed) speed for this vehicle."
    )
    smoothed: NonNegativeFloat | None = Field(
        description="The smoothed speed for this vehicle (as per the speed smoothing settings for the pipeline.)"
    )


class SpeedUpdateEvent(BaseStatusVehicleEvent):
    """
    A container for the vehicle's speeds plus its matched status,
    i. e. whether the state emitted for this vehicle by the tracking module
    is associated with an actual detection produced by the object detection module for this frame.
    """
    speeds: SpeedsWrapper = Field(description="The container for the speed values.")
    is_matched: bool = Field(
        description=(
            "Whether the track state for this vehicle was associated with a raw detection "
            "from the detection module for this frame. Speeds for an unmatched track state "
            "are determined based on the predicted position and are thus fully hypothetical, "
            "being interpolated from recent movement."
        )
    )

class SpeedUpdatesContainer(BaseStatusVehicleEventsContainer[SpeedUpdateEvent]):
    pass
