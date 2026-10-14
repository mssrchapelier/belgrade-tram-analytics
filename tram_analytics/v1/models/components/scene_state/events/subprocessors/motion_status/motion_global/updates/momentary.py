from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.base import (
    BaseMotionStatusUpdate
)


class MomentaryMotionStatusUpdate(BaseMotionStatusUpdate):
    """
    An update representing the momentary motion status, 
    i. e. one determined irrespective of the vehicle's movement history. 
    For example, a vehicle that has appeared only recently can have 
    a momentary status of "moving", but an undefined confirmed motion status. 
    """
    pass

class MomentaryMotionStatusUpdatesContainer(BaseStatusVehicleEventsContainer[MomentaryMotionStatusUpdate]):
    """
    A container for momentary motion status updates.
    """
    pass