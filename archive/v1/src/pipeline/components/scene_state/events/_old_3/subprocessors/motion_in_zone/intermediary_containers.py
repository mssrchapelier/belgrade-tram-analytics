from typing import List, override, Self, NamedTuple, Sequence, Iterator
from dataclasses import dataclass, field

from common.utils.misc_utils import concatenate_sequences
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_intermediary_container import BaseIntermediaryEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneEntranceEvent, ZoneExitEvent
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_global.events import MotionStatusUpdate
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_in_zone.events import BaseInZoneMotionStatusPeriodBoundaryEvent, \
    InZoneStationaryStartEvent, InZoneMovingStartEvent, InZoneStationaryEndEvent, InZoneMovingEndEvent, \
    InZoneMotionEventsContainer, InZoneMotionStatusStartEventsContainer, InZoneMotionStatusEndEventsContainer


@dataclass(slots=True, kw_only=True)
class IntermediaryZoneEventsContainer(BaseIntermediaryEventsContainer):
    start: List[ZoneEntranceEvent] = field(default_factory=list)
    end: List[ZoneExitEvent] = field(default_factory=list)

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls()

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        all_entrances: List[ZoneEntranceEvent] = concatenate_sequences(
            tuple(c.start for c in containers)
        )
        all_exits: List[ZoneExitEvent] = concatenate_sequences(
            tuple(c.end for c in containers)
        )
        return cls(start=all_entrances, end=all_exits)

# NOTE: Separated input containers for alive vs dead vehicles to facilitate static type checking
#       as opposed to runtime checks on the internal consistency of the containers.

class InputEventsForAliveVehicle(NamedTuple):
    # all events present for a single vehicle in the update
    current_confirmed_motion_status_update_event: MotionStatusUpdate
    zone_occupancy_events: IntermediaryZoneEventsContainer


class InputEventsForDeadVehicle(NamedTuple):
    zone_exit_events: List[ZoneExitEvent]


class InputForAliveVehicleWithVehicleID(NamedTuple):
    vehicle_id: str
    events: InputEventsForAliveVehicle


class InputForDeadVehicleWithVehicleID(NamedTuple):
    vehicle_id: str
    events: InputEventsForDeadVehicle


class InputForVehicles(NamedTuple):
    alive: List[InputForAliveVehicleWithVehicleID]
    dead: List[InputForDeadVehicleWithVehicleID]


@dataclass(slots=True, kw_only=True)
class BaseIntermediaryOutputSingleBoundaryTypeEventsContainer[
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
class IntermediaryOutputStartEventsContainer(
    BaseIntermediaryOutputSingleBoundaryTypeEventsContainer[InZoneStationaryStartEvent, InZoneMovingStartEvent]
):
    pass


@dataclass(slots=True, kw_only=True)
class IntermediaryOutputEndEventsContainer(
    BaseIntermediaryOutputSingleBoundaryTypeEventsContainer[InZoneStationaryEndEvent, InZoneMovingEndEvent]
):
    pass


@dataclass(slots=True, kw_only=True)
class IntermediaryOutputEventsContainer(BaseIntermediaryEventsContainer):
    start: IntermediaryOutputStartEventsContainer = field(default_factory=IntermediaryOutputStartEventsContainer)
    end: IntermediaryOutputEndEventsContainer = field(default_factory=IntermediaryOutputEndEventsContainer)

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls()

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        return cls(
            start=IntermediaryOutputStartEventsContainer.concatenate(tuple(c.start for c in containers)),
            end=IntermediaryOutputEndEventsContainer.concatenate(tuple(c.end for c in containers))
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

    def to_final_container(self) -> InZoneMotionEventsContainer:
        out_container: InZoneMotionEventsContainer = InZoneMotionEventsContainer(
            start=InZoneMotionStatusStartEventsContainer(stationary=self.start.stationary,
                                                         moving=self.start.moving),
            end=InZoneMotionStatusEndEventsContainer(stationary=self.end.stationary,
                                                     moving=self.end.moving)
        )
        return out_container
