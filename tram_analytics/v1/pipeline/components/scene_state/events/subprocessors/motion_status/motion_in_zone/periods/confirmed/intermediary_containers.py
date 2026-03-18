from dataclasses import dataclass, field
from typing import List, override, Self, Sequence, Iterator

from common.utils.misc_utils import concatenate_sequences
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed import (
    BaseInZoneMotionStatusPeriodBoundaryEvent,
    InZoneStationaryStartEvent, InZoneMovingStartEvent, InZoneStationaryEndEvent, InZoneMovingEndEvent,
    InZoneMotionStatusStartEventsContainer, InZoneMotionStatusEndEventsContainer,
    InZoneMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_intermediary_container import \
    BaseIntermediaryEventsContainer


@dataclass(slots=True, kw_only=True)
class BaseInZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer[
    StationaryEvent: BaseInZoneMotionStatusPeriodBoundaryEvent,
    MovingEvent: BaseInZoneMotionStatusPeriodBoundaryEvent
](BaseIntermediaryEventsContainer):
    stationary: List[StationaryEvent] = field(default_factory=list)
    moving: List[MovingEvent] = field(default_factory=list)

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        all_stationary: List[StationaryEvent] = concatenate_sequences(
            tuple(c.stationary for c in containers)
        )
        all_moving: List[MovingEvent] = concatenate_sequences(
            tuple(c.moving for c in containers)
        )
        concatenated: Self = cls(stationary=all_stationary,
                                 moving=all_moving)
        return concatenated

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls()


@dataclass(slots=True, kw_only=True)
class InZoneMotionStatusStartEventsIntermediaryContainer(
    BaseInZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer[InZoneStationaryStartEvent, InZoneMovingStartEvent]
):
    pass


@dataclass(slots=True, kw_only=True)
class InZoneMotionStatusEndEventsIntermediaryContainer(
    BaseInZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer[InZoneStationaryEndEvent, InZoneMovingEndEvent]
):
    pass


@dataclass(slots=True, kw_only=True)
class InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer(BaseIntermediaryEventsContainer):
    start: InZoneMotionStatusStartEventsIntermediaryContainer = field(
        default_factory=InZoneMotionStatusStartEventsIntermediaryContainer
    )
    end: InZoneMotionStatusEndEventsIntermediaryContainer = field(
        default_factory=InZoneMotionStatusEndEventsIntermediaryContainer
    )

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls()

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        return cls(
            start=InZoneMotionStatusStartEventsIntermediaryContainer.concatenate(tuple(c.start for c in containers)),
            end=InZoneMotionStatusEndEventsIntermediaryContainer.concatenate(tuple(c.end for c in containers))
        )

    def iter_over_zone_ids(self) -> Iterator[str]:
        for event in (*self.start.stationary, *self.start.moving,
                        *self.end.stationary, *self.end.moving): # type: BaseInZoneMotionStatusPeriodBoundaryEvent
            yield event.zone_id

    def get_total_num_events(self) -> int:
        return sum((len(self.start.stationary),
                    len(self.start.moving),
                    len(self.end.stationary),
                    len(self.end.moving)))

    def to_final_container(self) -> InZoneMotionStatusPeriodBoundaryEventsContainer:
        out_container: InZoneMotionStatusPeriodBoundaryEventsContainer = InZoneMotionStatusPeriodBoundaryEventsContainer(
            start=InZoneMotionStatusStartEventsContainer(stationary=self.start.stationary,
                                                         moving=self.start.moving),
            end=InZoneMotionStatusEndEventsContainer(stationary=self.end.stationary,
                                                     moving=self.end.moving)
        )
        return out_container
