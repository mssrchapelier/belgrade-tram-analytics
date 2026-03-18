from typing import Literal, List, override, Self

from pydantic import model_validator

from tram_analytics.v1.models.components.scene_state.events.base import BasePeriodBoundaryEvent, EventBoundaryType, \
    BaseSceneEventsContainer


class BaseZoneOccupancyEvent(BasePeriodBoundaryEvent):
    """
    Represents the start of end of a zone occupancy period, defined as
    a continuous period during which there is at least one vehicle in the zone.
    """

    zone_id: str

class ZoneOccupancyStartEvent(BaseZoneOccupancyEvent):
    """
    An event representing the start of a zone occupancy period
    (i. e. this zone being occupied by at least one vehicle
    after there were no vehicles in the zone).
    """

    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START

class ZoneOccupancyEndEvent(BaseZoneOccupancyEvent):
    """
    An event representing the end of a zone occupancy period
    (i. e. no vehicles now being in the zone after there was at least one;
    also a forced end to an ongoing period when processing is ended).
    """
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

class ZoneOccupancyEventsContainer(BaseSceneEventsContainer):
    """
    A container for all registered zone occupancy events for this frame.
    """

    start: List[ZoneOccupancyStartEvent]
    end: List[ZoneOccupancyEndEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(start=[], end=[])

    def _check_unique_zone_ids(self) -> None:
        # can't have both a start and an end event for the same zone in the same container
        num_zone_ids: int = len(
            set(event.zone_id for event in (*self.start, *self.end))
        )
        num_events: int = len(self.start) + len(self.end)
        if num_zone_ids != num_events:
            raise ValueError("Events cannot have duplicate zone IDs")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_unique_zone_ids()
        return self