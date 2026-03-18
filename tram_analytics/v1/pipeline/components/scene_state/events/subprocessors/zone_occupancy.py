from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Set, override, Type, Iterable, NamedTuple, DefaultDict

from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid, generate_period_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_occupancy import (
    ZoneOccupancyStartEvent, ZoneOccupancyEndEvent, ZoneOccupancyEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import (
    ZoneEntranceEvent, ZoneExitEvent, ZoneTransitEventsContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)


# --- generator state ---

class OngoingOccupancyState(NamedTuple):
    occupancy_period_id: str
    cur_num_vehicles: int

class ZoneOccupancyEventGeneratorState(BaseGeneratorState):

    def __init__(self):
        super().__init__()
        # for all zones with an ongoing occupancy:
        # zone ID -> ( period ID, current number of vehicles in the zone )
        self._zone_id_to_occupancy_state: Dict[str, OngoingOccupancyState] = dict()

    @override
    def _clear_own_state(self) -> None:
        self._zone_id_to_occupancy_state.clear()

    def get_zone_ids(self) -> Set[str]:
        return set(self._zone_id_to_occupancy_state.keys())

    def get_num_vehicles_for_zone(self, zone_id: str) -> int:
        if zone_id not in self._zone_id_to_occupancy_state:
            raise ValueError(f"Can't get the number of vehicles for zone {zone_id}: not registered")
        return self._zone_id_to_occupancy_state[zone_id].cur_num_vehicles

    def start_zone_occupancy(
            self, *, zone_id: str, cur_num_vehicles: int, occupancy_period_id: str
    ) -> None:
        if zone_id in self._zone_id_to_occupancy_state:
            raise ValueError(f"Can't add a new occupancy for zone {zone_id}: occupancy already registered")
        self._zone_id_to_occupancy_state[zone_id] = OngoingOccupancyState(
            occupancy_period_id=occupancy_period_id,
            cur_num_vehicles=cur_num_vehicles
        )

    def end_zone_occupancy_and_get_occupancy_period_id(self, zone_id: str) -> str:
        if zone_id not in self._zone_id_to_occupancy_state:
            raise ValueError(f"Can't end occupancy for zone {zone_id}: occupancy not registered")
        state: OngoingOccupancyState = self._zone_id_to_occupancy_state.pop(zone_id)
        return state.occupancy_period_id

    def update_zone_occupancy(self, *, zone_id: str, cur_num_vehicles: int) -> None:
        if zone_id not in self._zone_id_to_occupancy_state:
            raise ValueError(f"Can't update occupancy for zone {zone_id}: occupancy not registered")
        if cur_num_vehicles < 0:
            raise ValueError("Can't update occupancy for zone {zone_id}: "
                             f"the current number of vehicles must be positive (passed {cur_num_vehicles})")
        if cur_num_vehicles == 0:
            raise ValueError("Can't update occupancy for zone {zone_id}: "
                             f"passed 0 as the current number of vehicles. "
                             f"The zone occupancy should be ended instead in this case")
        prev_state: OngoingOccupancyState = self._zone_id_to_occupancy_state[zone_id]
        cur_state: OngoingOccupancyState = OngoingOccupancyState(
            occupancy_period_id=prev_state.occupancy_period_id,
            cur_num_vehicles=cur_num_vehicles
        )
        self._zone_id_to_occupancy_state[zone_id] = cur_state

    @staticmethod
    def _check_prev_num_vehicles(prev_num_vehicles: int) -> None:
        if prev_num_vehicles == 0:
            raise RuntimeError("Illegal state: prev_num_vehicles is 0 (did not remove the zone "
                               "from _zone_id_to_occupancy_state when the vehicle counter reached 0?)")

    @override
    def clear(self) -> None:
        self._zone_id_to_occupancy_state.clear()

# --- helper containers ---
# used by the generator to pass around computed info conveniently

# note: has to be mutable (populated dynamically from zone transit events), thus not a tuple
@dataclass(slots=True, kw_only=True)
class TransitCounters:
    vehicles_entered: int = 0
    vehicles_exited: int = 0

class ZoneToTransitCounters(NamedTuple):
    zone_id: str
    transit_counters: TransitCounters

class OccupancyStartInfo(NamedTuple):
    zone_id: str
    cur_num_vehicles: int

class OccupancyUpdateInfo(NamedTuple):
    zone_id: str
    cur_num_vehicles: int

# --- generator ---

class ZoneOccupancyEventGenerator(BaseFinalEventGenerator[
    ZoneTransitEventsContainer, ZoneOccupancyEventsContainer, ZoneOccupancyEventGeneratorState
]):

    @override
    @classmethod
    def _get_container_class(cls) -> Type[ZoneOccupancyEventsContainer]:
        return ZoneOccupancyEventsContainer

    @override
    @classmethod
    def _get_new_state(cls) -> ZoneOccupancyEventGeneratorState:
        return ZoneOccupancyEventGeneratorState()

    @staticmethod
    def _get_transit_counters_from_transit_events(
            input_data: ZoneTransitEventsContainer
    ) -> List[ZoneToTransitCounters]:
        # zone_id -> (vehicles_entered, vehicles_exited)
        zone_id_to_counters: DefaultDict[str, TransitCounters] = defaultdict(TransitCounters)
        for entrance_event in input_data.start: # type: ZoneEntranceEvent
            zone_id_to_counters[entrance_event.zone_id].vehicles_entered += 1
        for exit_event in input_data.end: # type: ZoneExitEvent
            zone_id_to_counters[exit_event.zone_id].vehicles_exited += 1
        # (zone_id, vehicles_entered, vehicles_exited)
        output: List[ZoneToTransitCounters] = [
            ZoneToTransitCounters(zone_id=zone_id, transit_counters=counters)
            for zone_id, counters in zone_id_to_counters.items()
        ]
        return output

    def _start_occupancies_and_get_events(
            self, input_data: Iterable[OccupancyStartInfo], *, system_state: ProcessingSystemState
    ) -> List[ZoneOccupancyStartEvent]:
        events: List[ZoneOccupancyStartEvent] = []
        for item in input_data: # type: OccupancyStartInfo
            occupancy_period_id: str = generate_period_uuid()
            self._state.start_zone_occupancy(zone_id=item.zone_id,
                                             cur_num_vehicles=item.cur_num_vehicles,
                                             occupancy_period_id=occupancy_period_id)
            event: ZoneOccupancyStartEvent = ZoneOccupancyStartEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                period_id=occupancy_period_id,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame,
                zone_id=item.zone_id
            )
            events.append(event)
        return events

    def _end_occupancies_and_get_events(
            self, zone_ids: Iterable[str], *, event_ts: datetime, truncate: bool
    ) -> List[ZoneOccupancyEndEvent]:
        events: List[ZoneOccupancyEndEvent] = []
        for zone_id in zone_ids: # type: str
            occupancy_period_id: str = self._state.end_zone_occupancy_and_get_occupancy_period_id(
                zone_id
            )
            event: ZoneOccupancyEndEvent = ZoneOccupancyEndEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                period_id=occupancy_period_id,
                event_ts=event_ts,
                truncated=truncate,
                zone_id=zone_id
            )
            events.append(event)
        return events

    def _update_occupancies(self, input_data: Iterable[OccupancyUpdateInfo]) -> None:
        for item in input_data: # type: OccupancyUpdateInfo
            self._state.update_zone_occupancy(zone_id=item.zone_id,
                                              cur_num_vehicles=item.cur_num_vehicles)

    @override
    def _update_and_get_events(
            self, input_data: ZoneTransitEventsContainer, *, system_state: ProcessingSystemState
    ) -> ZoneOccupancyEventsContainer:
        # for each zone, calculate the number of entrances and exits
        # (zone_id, (vehicles_entered, vehicles_exited))
        updates: List[ZoneToTransitCounters] = self._get_transit_counters_from_transit_events(input_data)

        # get all zones that are registered as having been occupied at the previous frame
        prev_occupied_zone_ids: Set[str] = self._state.get_zone_ids()

        # gather information to pass to event creators:

        # (1) - for new occupancies -
        # (zone_id, num_vehicles)
        new_occupancies_infos: List[OccupancyStartInfo] = []
        # (2) - for completed occupancies -
        # zone_id
        completed_occupancies_zone_ids: List[str] = []
        # (3) - for continued occupancies -
        # (zone_id, num_vehicles)
        continued_occupancies_infos: List[OccupancyUpdateInfo] = []

        # go through the zones that have entrances/exits registered
        for update in updates: # type: ZoneToTransitCounters
            zone_id: str = update.zone_id
            if zone_id not in prev_occupied_zone_ids:
                # not registered -> new occupancy
                if update.transit_counters.vehicles_exited > 0:
                    # can't have exits out of a zone that is not yet occupied
                    raise ValueError(f"Got exits for zone {zone_id} that is not registered as being occupied")
                new_occupancies_infos.append(
                    OccupancyStartInfo(zone_id=zone_id,
                                       cur_num_vehicles=update.transit_counters.vehicles_entered)
                )
            else:
                # registered -> existing occupancy
                prev_num_vehicles: int = self._state.get_num_vehicles_for_zone(zone_id)
                if prev_num_vehicles == 0:
                    # should have been removed
                    raise RuntimeError(f"Illegal state: prev_num_vehicles is 0 for zone {zone_id} "
                                       f"(did not remove the zone from _zone_id_to_occupancy_state "
                                       f"when the vehicle counter reached 0?)")
                # calculate the current number of vehicles in the zone
                cur_num_vehicles: int = (
                    prev_num_vehicles
                    + update.transit_counters.vehicles_entered
                    - update.transit_counters.vehicles_exited
                )
                if cur_num_vehicles < 0:
                    # the result is a negative number of vehicles -- by design, should never happen
                    raise RuntimeError(f"Got a negative new number of vehicles for zone {zone_id}: "
                                       f"previous {prev_num_vehicles}, "
                                       f"entered {update.transit_counters.vehicles_entered}, "
                                       f"exited {update.transit_counters.vehicles_exited}")
                if cur_num_vehicles == 0:
                    # no more vehicles in the zone --> end of occupancy
                    completed_occupancies_zone_ids.append(zone_id)
                else:
                    # vehicles still present in the zone --> occupancy continues; update the number of vehicles
                    continued_occupancies_infos.append(
                        OccupancyUpdateInfo(zone_id=zone_id,
                                            cur_num_vehicles=cur_num_vehicles)
                    )

        # register new occupancies, get start events
        start_events: List[ZoneOccupancyStartEvent] = self._start_occupancies_and_get_events(
            new_occupancies_infos, system_state=system_state
        )
        # deregister completed occupancies, get end events
        end_events: List[ZoneOccupancyEndEvent] = self._end_occupancies_and_get_events(
            completed_occupancies_zone_ids, event_ts=system_state.cur_frame_ts, truncate=system_state.is_first_frame
        )
        # update continued occupancies with the new number of vehicles
        self._update_occupancies(continued_occupancies_infos)

        container: ZoneOccupancyEventsContainer = ZoneOccupancyEventsContainer(start=start_events,
                                                                               end=end_events)
        return container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> ZoneOccupancyEventsContainer:
        completed_occupancies_zone_ids: Set[str] = self._state.get_zone_ids()
        end_events: List[ZoneOccupancyEndEvent] = self._end_occupancies_and_get_events(
            completed_occupancies_zone_ids, event_ts=event_ts, truncate=truncate_events
        )
        container: ZoneOccupancyEventsContainer = ZoneOccupancyEventsContainer(start=[],
                                                                               end=end_events)
        return container