from typing import override, Self

from tram_analytics.v1.models.components.scene_state.events.base import BaseSceneEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed import (
    GlobalMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.motion_global_updates import (
    MotionStatusUpdatesContainer
)


class GlobalMotionStatusEventsContainer(BaseSceneEventsContainer):
    updates: MotionStatusUpdatesContainer
    # Note: Not including or implementing events for periods based on momentary status;
    #   judged not as useful.
    period_boundary_events_for_confirmed_status: GlobalMotionStatusPeriodBoundaryEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(
            updates=MotionStatusUpdatesContainer.create_empty_container(),
            period_boundary_events_for_confirmed_status=GlobalMotionStatusPeriodBoundaryEventsContainer.create_empty_container()
        )