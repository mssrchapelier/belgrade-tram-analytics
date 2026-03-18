from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEvent

class BaseMotionStatusUpdate(BaseStatusVehicleEvent):
    motion_status: MotionStatus
