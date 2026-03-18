from typing import override, Self

from tram_analytics.v1.models.components.scene_state.events.base import BaseSceneEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.motion_global_events import \
    GlobalMotionStatusEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed import (
    InZoneMotionStatusPeriodBoundaryEventsContainer
)


class MotionStatusEventsContainer(BaseSceneEventsContainer):
    motion_global: GlobalMotionStatusEventsContainer
    motion_in_zone: InZoneMotionStatusPeriodBoundaryEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(
            motion_global=GlobalMotionStatusEventsContainer.create_empty_container(),
            motion_in_zone=InZoneMotionStatusPeriodBoundaryEventsContainer.create_empty_container()
        )
