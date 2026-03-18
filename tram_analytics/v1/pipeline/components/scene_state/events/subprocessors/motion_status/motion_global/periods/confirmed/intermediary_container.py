from abc import abstractmethod
from dataclasses import dataclass
from typing import List, override, Self, Sequence

from common.utils.misc_utils import concatenate_sequences
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed import (
    BaseMotionStatusPeriodBoundaryEvent,
    StationaryStartEvent, StationaryEndEvent, MovingStartEvent, MovingEndEvent,
    BaseMotionStatusPeriodBoundaryEventsContainer, MotionStatusStartEventsContainer, MotionStatusEndEventsContainer,
    GlobalMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_intermediary_container import \
    BaseIntermediaryEventsContainer


@dataclass(slots=True, kw_only=True)
class BaseMotionStatusPeriodBoundaryIntermediaryEventsContainer[
    StationaryEvent: BaseMotionStatusPeriodBoundaryEvent,
    MovingEvent: BaseMotionStatusPeriodBoundaryEvent
](BaseIntermediaryEventsContainer):
    """
    An intermediary container for all start events OR for all end events
    for periods defined by vehicles' preserved confirmed motion status
    that were registered for the frame.
    """
    stationary: List[StationaryEvent]
    moving: List[MovingEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(stationary=[], moving=[])

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        all_stationary: List[StationaryEvent] = concatenate_sequences(
            tuple(c.stationary for c in containers)
        )
        all_moving: List[MovingEvent] = concatenate_sequences(
            tuple(c.moving for c in containers)
        )
        return cls(stationary=all_stationary, moving=all_moving)

    @abstractmethod
    def to_final_container(self) -> BaseMotionStatusPeriodBoundaryEventsContainer[StationaryEvent, MovingEvent]:
        pass

# for start events
class MotionStatusPeriodStartIntermediaryEventsContainer(
    BaseMotionStatusPeriodBoundaryIntermediaryEventsContainer[StationaryStartEvent, MovingStartEvent]
):

    @override
    def to_final_container(self) -> MotionStatusStartEventsContainer:
        return MotionStatusStartEventsContainer(stationary=self.stationary,
                                                moving=self.moving)

# for end events
class MotionStatusPeriodEndIntermediaryEventsContainer(
    BaseMotionStatusPeriodBoundaryIntermediaryEventsContainer[StationaryEndEvent, MovingEndEvent]
):
    @override
    def to_final_container(self) -> MotionStatusEndEventsContainer:
        return MotionStatusEndEventsContainer(stationary=self.stationary,
                                              moving=self.moving)

# for all events
@dataclass(slots=True, kw_only=True)
class MotionStatusPeriodBoundaryIntermediaryEventsContainer(BaseIntermediaryEventsContainer):

    """
    An intermediary container for all start AND end events
    for periods defined by vehicles' preserved confirmed motion status
    that were registered for the frame.
    """

    start: MotionStatusPeriodStartIntermediaryEventsContainer
    end: MotionStatusPeriodEndIntermediaryEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(start=MotionStatusPeriodStartIntermediaryEventsContainer.create_empty_container(),
                   end=MotionStatusPeriodEndIntermediaryEventsContainer.create_empty_container())

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        return cls(
            start=MotionStatusPeriodStartIntermediaryEventsContainer.concatenate(tuple(c.start for c in containers)),
            end=MotionStatusPeriodEndIntermediaryEventsContainer.concatenate(tuple(c.end for c in containers))
        )

    def to_final_container(self) -> GlobalMotionStatusPeriodBoundaryEventsContainer:
        return GlobalMotionStatusPeriodBoundaryEventsContainer(
            start=self.start.to_final_container(),
            end=self.end.to_final_container()
        )