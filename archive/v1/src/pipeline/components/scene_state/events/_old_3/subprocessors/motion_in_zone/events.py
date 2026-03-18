from typing import List, Set, Self, Literal, Tuple, Sequence, override

from pydantic import model_validator

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import (
    BasePeriodBoundaryVehicleEvent, EventBoundaryType, BaseSceneEventsContainer
)

# base
class BaseInZoneMotionStatusPeriodBoundaryEvent(BasePeriodBoundaryVehicleEvent):

    """
    An event for the start or the end of an "in-zone" motion status period:
    a derived period during which the given vehicle maintains the same confirmed motion status
    whilst being assigned to the given zone.

    Example: a tram's stop at a platform, as defined for a specific tram and a specific platform.
    """

    confirmed_motion_status: MotionStatus
    zone_id: str

def _extract_vehicle_zone_id_pairs(events: Sequence[BaseInZoneMotionStatusPeriodBoundaryEvent]) -> Set[Tuple[str, str]]:
    return {(e.vehicle_id, e.zone_id) for e in events}

def _have_unique_vehicle_zone_id_pairs(events: Sequence[BaseInZoneMotionStatusPeriodBoundaryEvent]) -> bool:
    """
    Check whether all `(vehicle_id, zone_id)` pairs in `events` are unique.
    """
    vehicle_zone_id_pairs: Set[Tuple[str, str]] = _extract_vehicle_zone_id_pairs(events)
    return len(vehicle_zone_id_pairs) == len(events)

# base for starts
class BaseInZoneMotionStatusPeriodStartEvent(BaseInZoneMotionStatusPeriodBoundaryEvent):
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START

# base for ends
class BaseInZoneMotionStatusPeriodEndEvent(BaseInZoneMotionStatusPeriodBoundaryEvent):
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

# stationary start
class InZoneStationaryStartEvent(BaseInZoneMotionStatusPeriodStartEvent):
    confirmed_motion_status: Literal[MotionStatus.STATIONARY] = MotionStatus.STATIONARY

# stationary end
class InZoneStationaryEndEvent(BaseInZoneMotionStatusPeriodEndEvent):
    confirmed_motion_status: Literal[MotionStatus.STATIONARY] = MotionStatus.STATIONARY

# moving start
class InZoneMovingStartEvent(BaseInZoneMotionStatusPeriodStartEvent):
    confirmed_motion_status: Literal[MotionStatus.MOVING] = MotionStatus.MOVING
    
# moving end
class InZoneMovingEndEvent(BaseInZoneMotionStatusPeriodEndEvent):
    confirmed_motion_status: Literal[MotionStatus.MOVING] = MotionStatus.MOVING

# --- containers ---

# start or end (generic to make use of common validation over vehicle IDs)
class BaseInZoneMotionStatusBoundaryEventsContainer[
    StationaryEvent: BaseInZoneMotionStatusPeriodBoundaryEvent,
    MovingEvent: BaseInZoneMotionStatusPeriodBoundaryEvent
](BaseSceneEventsContainer):

    """
    A container for all start events OR for all end events for in-zone motion status periods
    produced for the frame.
    """

    stationary: List[StationaryEvent]
    moving: List[MovingEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(stationary=[], moving=[])

    def _check_non_overlapping_vehicle_zone_id_pairs(self) -> None:
        # for the same vehicle-zone pair, a start for either stationary or moving -- not both
        ids_in_stationary: Set[Tuple[str, str]] = {
            (event.vehicle_id, event.zone_id) for event in self.stationary
        }
        ids_in_moving: Set[Tuple[str, str]] = {
            (event.vehicle_id, event.zone_id) for event in self.moving
        }
        if len(set.intersection(ids_in_stationary, ids_in_moving)) > 0:
            raise ValueError("Start/end events for in-zone (confirmed) motion status: "
                             "The sub-containers for stationary and moving must have "
                             "non-overlapping sets of (vehicle ID, zone ID) pairs, "
                             "but encountered an overlap")

    def _check_unique_vehicle_ids_inside_subcontainers(self) -> None:
        # (vehicle ID, zone ID) pairs must be unique inside stationary and inside moving
        if not (_have_unique_vehicle_zone_id_pairs(self.stationary)
                and _have_unique_vehicle_zone_id_pairs(self.moving)):
            raise ValueError("Start/end events for in-zone (confirmed) motion status: "
                             "(vehicle ID, zone ID) pairs must be unique in each of the sub-containers "
                             "for stationary and moving, but encountered non-unique pairs")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_non_overlapping_vehicle_zone_id_pairs()
        self._check_unique_vehicle_ids_inside_subcontainers()
        return self

# for start events
class InZoneMotionStatusStartEventsContainer(
    BaseInZoneMotionStatusBoundaryEventsContainer[InZoneStationaryStartEvent, InZoneMovingStartEvent]
):
    pass

# for end events
class InZoneMotionStatusEndEventsContainer(
    BaseInZoneMotionStatusBoundaryEventsContainer[InZoneStationaryEndEvent, InZoneMovingEndEvent]
):
    pass

# master
class InZoneMotionEventsContainer(BaseSceneEventsContainer):

    """
    A container for all start AND end events for in-zone motion status periods
    produced for the frame.
    """

    start: InZoneMotionStatusStartEventsContainer
    end: InZoneMotionStatusEndEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(start=InZoneMotionStatusStartEventsContainer.create_empty_container(),
                   end=InZoneMotionStatusEndEventsContainer.create_empty_container())

    def _check_non_overlapping_vehicle_zone_id_pairs(self) -> None:
        # for the same vehicle-zone pair, for stationary and moving, either a start or an end -- not both
        ids_start_stationary: Set[Tuple[str, str]] = _extract_vehicle_zone_id_pairs(self.start.stationary)
        ids_end_stationary: Set[Tuple[str, str]] = _extract_vehicle_zone_id_pairs(self.end.stationary)
        ids_start_moving: Set[Tuple[str, str]] = _extract_vehicle_zone_id_pairs(self.start.moving)
        ids_end_moving: Set[Tuple[str, str]] = _extract_vehicle_zone_id_pairs(self.end.moving)
        overlap_for_stationary: Set[Tuple[str, str]] = set.intersection(ids_start_stationary, ids_end_stationary)
        overlap_for_moving: Set[Tuple[str, str]] = set.intersection(ids_start_moving, ids_end_moving)

        if len(overlap_for_stationary) > 0 or len(overlap_for_moving) > 0:
            raise ValueError("Change events for in-zone (confirmed) motion status: "
                             "Cannot have overlapping sets of (vehicle ID, zone ID) pairs for start and end "
                             "for the same type of the changed motion status (stationary/moving), "
                             "but encountered an overlap")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_non_overlapping_vehicle_zone_id_pairs()
        return self