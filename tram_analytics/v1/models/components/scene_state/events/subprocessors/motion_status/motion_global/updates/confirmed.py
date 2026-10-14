from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.base import (
    BaseMotionStatusUpdate
)


class ConfirmedMotionStatusUpdate(BaseMotionStatusUpdate):
    """
    An update representing the confirmed motion status, 
    i. e. one determined based on whether the vehicle's presence has been confirmed. 
    For example, a vehicle that has appeared only recently can have 
    a momentary status of "moving", but an undefined confirmed motion status. 
    """
    pass

class ConfirmedMotionStatusUpdatesContainer(BaseStatusVehicleEventsContainer[ConfirmedMotionStatusUpdate]):
    """
    A container for confirmed motion status updates.
    """
    pass