from typing import Literal, TypeAlias, List, override, Self, Set

from pydantic import model_validator

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import (
    BasePeriodBoundaryVehicleEvent, EventBoundaryType, BaseSceneEventsContainer, have_unique_vehicle_ids
)


# Start/end events for periods defined by changes to and from stationary and moving.

# - For continuous periods defined by the vehicle having a defined (stationary or moving) confirmed status,
#   start and end events are triggered respectively.
# - At the vehicle's lifetime end, the end event for the current confirmed status period is also emitted.

# base
class BaseMotionStatusPeriodBoundaryEvent(BasePeriodBoundaryVehicleEvent):
    """
    Represents the start or the end of a continuous period
    during which the vehicle's confirmed motion status stays the same.
    """
    confirmed_motion_status: MotionStatus

# base for starts
class BaseMotionStatusPeriodStartEvent(BaseMotionStatusPeriodBoundaryEvent):
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START

# base for ends
class BaseMotionStatusPeriodEndEvent(BaseMotionStatusPeriodBoundaryEvent):
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

# stationary start
class StationaryStartEvent(BaseMotionStatusPeriodStartEvent):
    confirmed_motion_status: Literal[MotionStatus.STATIONARY] = MotionStatus.STATIONARY

# stationary end
class StationaryEndEvent(BaseMotionStatusPeriodEndEvent):
    confirmed_motion_status: Literal[MotionStatus.STATIONARY] = MotionStatus.STATIONARY

# moving start
class MovingStartEvent(BaseMotionStatusPeriodStartEvent):
    confirmed_motion_status: Literal[MotionStatus.MOVING] = MotionStatus.MOVING

# moving end
class MovingEndEvent(BaseMotionStatusPeriodEndEvent):
    confirmed_motion_status: Literal[MotionStatus.MOVING] = MotionStatus.MOVING

StationaryPeriodBoundaryEvent: TypeAlias = StationaryStartEvent | StationaryEndEvent
MovingPeriodBoundaryEvent: TypeAlias = MovingStartEvent | MovingEndEvent
MotionStatusPeriodBoundaryEvent: TypeAlias = StationaryPeriodBoundaryEvent | MovingPeriodBoundaryEvent
MotionStatusPeriodStartEvent: TypeAlias = StationaryStartEvent | MovingStartEvent
MotionStatusPeriodEndEvent: TypeAlias = StationaryEndEvent | MovingEndEvent

# --- containers ---

# start or end (generic to make use of common validation over vehicle IDs)
class BaseMotionStatusPeriodBoundaryEventsContainer[
    StationaryEvent: BaseMotionStatusPeriodBoundaryEvent,
    MovingEvent: BaseMotionStatusPeriodBoundaryEvent
](BaseSceneEventsContainer):

    """
    A container for all start events OR for all end events
    for periods defined by vehicles' preserved confirmed motion status
    that were registered for the frame.
    """

    stationary: List[StationaryEvent]
    moving: List[MovingEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(stationary=[], moving=[])

    def _check_non_overlapping_vehicle_ids(self) -> None:
        # for the same vehicle, a start for either stationary or moving -- not both
        ids_in_stationary: Set[str] = {event.vehicle_id for event in self.stationary}
        ids_in_moving: Set[str] = {event.vehicle_id for event in self.moving}
        if len(set.intersection(ids_in_stationary, ids_in_moving)) > 0:
            raise ValueError("Start/end events for confirmed status: The sub-containers for stationary and moving "
                             "must have non-overlapping sets of vehicle IDs, but encountered an overlap")

    def _check_unique_vehicle_ids_inside_subcontainers(self) -> None:
        # vehicle IDs must be unique inside stationary and inside moving
        if not (have_unique_vehicle_ids(self.stationary) and have_unique_vehicle_ids(self.moving)):
            raise ValueError("Start/end events for confirmed status: Vehicle IDs must be unique in each of the sub-containers "
                             "for stationary and moving, but encountered non-unique IDs")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_non_overlapping_vehicle_ids()
        self._check_unique_vehicle_ids_inside_subcontainers()
        return self

# for start events
class MotionStatusStartEventsContainer(
    BaseMotionStatusPeriodBoundaryEventsContainer[StationaryStartEvent, MovingStartEvent]
):
    pass

# for end events
class MotionStatusEndEventsContainer(
    BaseMotionStatusPeriodBoundaryEventsContainer[StationaryEndEvent, MovingEndEvent]
):
    pass

# for all events
class GlobalMotionStatusPeriodBoundaryEventsContainer(BaseSceneEventsContainer):

    """
    A container for all start AND end events
    for periods defined by vehicles' preserved confirmed motion status
    that were registered for the frame.
    """

    start: MotionStatusStartEventsContainer
    end: MotionStatusEndEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(start=MotionStatusStartEventsContainer.create_empty_container(),
                   end=MotionStatusEndEventsContainer.create_empty_container())

    # for the same vehicle, cannot have both a start and an end event of the same confirmed motion status in the same frame
    def _check_non_overlapping_vehicle_ids(self) -> None:
        # for the same vehicle, for stationary and moving, either a start or an end -- not both
        ids_start_stationary: Set[str] = {event.vehicle_id for event in self.start.stationary}
        ids_end_stationary: Set[str] = {event.vehicle_id for event in self.end.stationary}
        ids_start_moving: Set[str] = {event.vehicle_id for event in self.start.moving}
        ids_end_moving: Set[str] = {event.vehicle_id for event in self.end.moving}
        overlap_for_stationary: Set[str] = set.intersection(ids_start_stationary, ids_end_stationary)
        overlap_for_moving: Set[str] = set.intersection(ids_start_moving, ids_end_moving)

        if len(overlap_for_stationary) > 0 or len(overlap_for_moving) > 0:
            raise ValueError("Events for confirmed status: Cannot have overlapping sets of vehicle IDs "
                             "for start and end for the same type of the changed motion status (stationary/moving), "
                             "but encountered an overlap")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_non_overlapping_vehicle_ids()
        return self
