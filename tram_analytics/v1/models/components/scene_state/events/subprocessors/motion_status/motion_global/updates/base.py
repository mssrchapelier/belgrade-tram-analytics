from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import BaseStatusVehicleEvent

from pydantic import Field

class BaseMotionStatusUpdate(BaseStatusVehicleEvent):
    motion_status: MotionStatus = Field(
        description="The motion status assigned by this update to this vehicle."
    )
