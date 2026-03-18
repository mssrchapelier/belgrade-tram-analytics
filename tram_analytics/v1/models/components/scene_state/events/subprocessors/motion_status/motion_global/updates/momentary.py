from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.base import (
    BaseMotionStatusUpdate
)


class MomentaryMotionStatusUpdate(BaseMotionStatusUpdate):
    pass

class MomentaryMotionStatusUpdatesContainer(BaseStatusVehicleEventsContainer[MomentaryMotionStatusUpdate]):
    pass