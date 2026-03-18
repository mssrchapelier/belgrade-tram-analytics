from typing import List, Set, Self, override

from pydantic import model_validator

from tram_analytics.v1.models.components.scene_state.events.base import (
    BaseSceneEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import VehiclesLifetimeEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedUpdateEvent
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneTransitEventsContainer


class CanonicalEventsContainer(BaseSceneEventsContainer):
    """
    Holds all canonical events for a single frame:
    - vehicle lifetime events;
    - zone occupancy events;
    - speed updates for all vehicles that are alive.
    """

    lifetime: VehiclesLifetimeEventsContainer
    zone_transit: ZoneTransitEventsContainer
    speeds: List[SpeedUpdateEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(lifetime=VehiclesLifetimeEventsContainer.create_empty_container(),
                   zone_transit=ZoneTransitEventsContainer.create_empty_container(),
                   speeds=[])

    # --- model validation functions ---

    def _check_speeds_vehicle_ids_unique(self) -> None:
        speeds_vehicle_ids: Set[str] = {e.vehicle_id for e in self.speeds}
        if not len(speeds_vehicle_ids) == len(self.speeds):
            raise ValueError("Vehicle IDs in speeds events must be unique")

    def _check_consistency_with_alive_status(self) -> None:
        """
        Check that the set of events does not violate the following constraints:
        - speeds are defined for all vehicles that are alive;
        - speeds are not defined for dead vehicles;
        - zone entrance events can only be defined for alive vehicles.
        """

        ids_lifetime_start: Set[str] = set(map(lambda e: e.vehicle_id, self.lifetime.start))
        ids_lifetime_end: Set[str] = set(map(lambda e: e.vehicle_id, self.lifetime.end))
        ids_zone_entrance: Set[str] = set(map(lambda e: e.vehicle_id, self.zone_transit.start))
        # exit: all (alive and dead)
        ids_zone_exit_all: Set[str] = set(map(lambda e: e.vehicle_id, self.zone_transit.end))
        ids_speeds: Set[str] = set(map(lambda e: e.vehicle_id, self.speeds))

        # the IDs of vehicles for which a zone occupancy end event has been registered,
        # but not due to the lifetime end (i. e. which are still alive)
        ids_zone_exit_alive: Set[str] = set.difference(ids_zone_exit_all, ids_lifetime_end)

        # (1) All vehicles that are alive based on lifetime and zone occupancy events must be present in speeds
        ids_to_check: Set[str] = set.union(ids_lifetime_start, ids_zone_entrance, ids_zone_exit_alive)
        if not ids_speeds.issuperset(ids_to_check):
            raise ValueError("All alive vehicle IDs from lifetime and zone transit must appear in speeds")

        # (2) Vehicles in zone occupancy start events are all alive and must not appear in lifetime end
        if len(set.intersection(ids_zone_entrance, ids_lifetime_end)) > 0:
            raise ValueError("Vehicle IDs from zone entrance events cannot appear in lifetime end events: "
                             "these vehicles are no longer alive")

        # (3) Vehicles in speeds are all alive and must not appear in lifetime end
        if len(set.intersection(ids_speeds, ids_lifetime_end)) > 0:
            raise ValueError("Vehicle IDs from speed events cannot appear in lifetime end events: "
                             "these vehicles are no longer alive")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_speeds_vehicle_ids_unique()
        self._check_consistency_with_alive_status()
        return self

