from typing import override, Self

from tram_analytics.v1.models.components.scene_state.events.base import BaseSceneEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdatesContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.momentary import (
    MomentaryMotionStatusUpdatesContainer
)


class MotionStatusUpdatesContainer(BaseSceneEventsContainer):
    momentary: MomentaryMotionStatusUpdatesContainer
    confirmed: ConfirmedMotionStatusUpdatesContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(momentary=MomentaryMotionStatusUpdatesContainer.create_empty_container(),
                   confirmed=ConfirmedMotionStatusUpdatesContainer.create_empty_container())