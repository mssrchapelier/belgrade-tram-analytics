from typing import Literal, List, override, Self, Set, Tuple

from pydantic import model_validator

from tram_analytics.v1.models.components.scene_state.events.base import BasePeriodBoundaryVehicleEvent, \
    EventBoundaryType, BaseSceneEventsContainer


# --- zone transit events ---

class BaseZoneTransitEvent(BasePeriodBoundaryVehicleEvent):
    """
    Represents the start or end of a vehicle's transit through a specific zone,
    i. e. a continuous period during which the vehicle
    is assigned to this zone at every moment (frame).
    """
    zone_id: str


class ZoneEntranceEvent(BaseZoneTransitEvent):
    """
    An event representing this vehicle's entrance into this zone.
    """
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START


class ZoneExitEvent(BaseZoneTransitEvent):
    """
    An event representing this vehicle's exit from this zone.
    """
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

# ZoneTransitEvent: TypeAlias = Annotated[
#     ZoneEntranceEvent | ZoneExitEvent,
#     Field(discriminator="boundary_type")
# ]

class ZoneTransitEventsContainer(BaseSceneEventsContainer):

    """
    A container for all registered zone transit events for this frame.
    """

    start: List[ZoneEntranceEvent]
    end: List[ZoneExitEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(start=[], end=[])

    def _check_id_combinations(self) -> None:
        # (vehicle id, zone id) tuples must be unique

        # (vehicle id, zone id)
        event_keys: Set[Tuple[str, str]] = {
            (e.vehicle_id, e.zone_id)
            for e in (*self.start, *self.end)
        }
        if len(event_keys) != len(self.start) + len(self.end):
            raise ValueError("Only one zone transit event can be defined for any combination "
                             "of vehicle ID and zone ID; encountered duplicates")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_id_combinations()
        return self
