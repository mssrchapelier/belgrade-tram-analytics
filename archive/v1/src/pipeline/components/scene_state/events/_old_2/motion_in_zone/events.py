from typing import List, Set, Self, Tuple, Literal
from warnings import deprecated

from pydantic import BaseModel, model_validator

from tram_analytics.v1.models.components.scene_state.events.base import BasePeriodBoundaryVehicleEvent, EventBoundaryType


# --- in-zone stationary events ---

# Events derived from stationary (global) events and zone occupancy events.
# Consist in a vehicle becoming stationary or ceasing to be stationary (see definition in `stationary.events`)
# whilst assigned to a specific zone.
# Edge cases as handled by the event generator:
# - Because a vehicle can be considered stationary whilst moving very slowly, cases where
#   a vehicle slowly drifted from one zone to a different one whilst remaining stationary
#   are possible. In this case:
#     * An in-zone stationary end event must be emitted for the zone on the vehicle's exit from it
#       (even as the vehicle remains stationary in the global sense).
#     * An in-zone stationary start event must be emitted for the zone on the vehicle's entrance into it
#       (even though it had become stationary in the global sense a while ago).
# - On a vehicle's lifetime end, if it is registered as being stationary in a specific zone,
#   an in-zone stationary end event must be emitted.


class BaseInZoneStationaryEvent(BasePeriodBoundaryVehicleEvent):
    zone_id: str

class InZoneStationaryStartEvent(BaseInZoneStationaryEvent):
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START

class InZoneStationaryEndEvent(BaseInZoneStationaryEvent):
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

# InZoneStationaryEvent: TypeAlias = Annotated[
#     InZoneStationaryStartEvent | InZoneStationaryEndEvent,
#     Field(discriminator="boundary_type")
# ]

class InZoneStationaryEventsContainer(BaseModel):
    start: List[InZoneStationaryStartEvent]
    end: List[InZoneStationaryEndEvent]

    def _check_id_combinations(self) -> None:
        # (vehicle id, zone id) tuples must be unique

        # (vehicle id, zone id)
        event_keys: Set[Tuple[str, str]] = {
            (e.vehicle_id, e.zone_id)
            for e in (*self.start, *self.end)
        }
        if len(event_keys) != len(self.start) + len(self.end):
            raise ValueError("Only one in-zone stationary event can be defined for any combination "
                             "of vehicle ID and zone ID; encountered duplicates")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_id_combinations()
        return self

@deprecated("")
class InZoneStationaryEventsContainer_Old(BaseModel):
    stationary_in_zone: List[BaseInZoneStationaryEvent]

    # --- model validation functions ---

    def _check_events_ids(self) -> None:
        # (vehicle id, zone id) tuples must be unique

        # (vehicle id, zone id)
        event_keys: Set[Tuple[str, str]] = {
            (e.vehicle_id, e.zone_id)
            for e in self.stationary_in_zone
        }
        if len(event_keys) != len(self.stationary_in_zone):
            raise ValueError("Only one stationary-in-zone event can be defined "
                             "for any combination of vehicle ID and zone ID")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        # all vehicle ids must be unique
        self._check_events_ids()
        return self