from datetime import datetime
from typing import List, Dict, Set, override, Type, NamedTuple, Iterable

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import \
    VehiclesLifetimeEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdate, ConfirmedMotionStatusUpdatesContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.momentary import (
    MomentaryMotionStatusUpdatesContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)


class ConfirmedMotionStatusUpdatesInput(NamedTuple):
    lifetime_events: VehiclesLifetimeEventsContainer
    momentary_motion_status_updates: MomentaryMotionStatusUpdatesContainer

class VehicleIdAndMomentaryStatus(NamedTuple):
    vehicle_id: str
    momentary: MotionStatus

class ConfirmedMotionStatusUpdateGeneratorState(BaseGeneratorState):

    def __init__(self) -> None:
        super().__init__()
        self._vehicle_id_to_confirmed_status: Dict[str, MotionStatus] = dict()

    @override
    def _clear_own_state(self) -> None:
        self._vehicle_id_to_confirmed_status.clear()

    def get_vehicle_ids(self) -> Set[str]:
        return set(self._vehicle_id_to_confirmed_status.keys())

    def add_vehicle(self, *, vehicle_id: str, confirmed: MotionStatus) -> None:
        if vehicle_id in self._vehicle_id_to_confirmed_status:
            raise ValueError(f"Can't add vehicle {vehicle_id}: already registered")
        self._vehicle_id_to_confirmed_status[vehicle_id] = confirmed

    def get_confirmed_status(self, vehicle_id: str) -> MotionStatus:
        if vehicle_id not in self._vehicle_id_to_confirmed_status:
            raise ValueError(f"Can't get confirmed motion status for vehicle {vehicle_id}: not registered")
        return self._vehicle_id_to_confirmed_status[vehicle_id]

    def update_confirmed_status(self, *, vehicle_id: str, confirmed: MotionStatus) -> None:
        if vehicle_id not in self._vehicle_id_to_confirmed_status:
            raise ValueError(f"Can't update vehicle {vehicle_id}: not registered")
        prev_status: MotionStatus = self._vehicle_id_to_confirmed_status[vehicle_id]
        cur_status: MotionStatus = confirmed
        if prev_status is not MotionStatus.UNDEFINED and cur_status is MotionStatus.UNDEFINED:
            # defined -> undefined: illegal
            raise ValueError(
                f"Can't update vehicle {vehicle_id}: the previous confirmed motion status is {prev_status}, "
                f"the current one is {cur_status} -- illegal transition"
            )
        self._vehicle_id_to_confirmed_status[vehicle_id] = confirmed

    def remove_vehicle(self, vehicle_id: str) -> None:
        if vehicle_id not in self._vehicle_id_to_confirmed_status:
            raise ValueError(f"Can't update vehicle {vehicle_id}: not registered")
        self._vehicle_id_to_confirmed_status.pop(vehicle_id)


class ConfirmedMotionStatusUpdateGenerator(
    BaseFinalEventGenerator[
        ConfirmedMotionStatusUpdatesInput, ConfirmedMotionStatusUpdatesContainer, ConfirmedMotionStatusUpdateGeneratorState
    ]
):

    @override
    @classmethod
    def _get_container_class(cls) -> Type[ConfirmedMotionStatusUpdatesContainer]:
        return ConfirmedMotionStatusUpdatesContainer

    def _get_new_state(self) -> ConfirmedMotionStatusUpdateGeneratorState:
        return ConfirmedMotionStatusUpdateGeneratorState()

    @staticmethod
    def _compute_confirmed_status(
            *, prev_confirmed: MotionStatus, cur_momentary: MotionStatus
    ) -> MotionStatus:
        if cur_momentary is MotionStatus.UNDEFINED:
            # current momentary status is undefined -- does not change the previous confirmed status
            return prev_confirmed
        # current is defined -- assign the it as the confirmed one
        return cur_momentary

    def _add_new_vehicles_and_get_events(
            self, mappings: Iterable[VehicleIdAndMomentaryStatus], *, system_state: ProcessingSystemState
    ) -> List[ConfirmedMotionStatusUpdate]:
        events: List[ConfirmedMotionStatusUpdate] = []
        for mapping in mappings: # type: VehicleIdAndMomentaryStatus
            vehicle_id: str = mapping.vehicle_id
            momentary: MotionStatus = mapping.momentary
            # for new vehicles, use the computed momentary status as the confirmed one
            cur_confirmed: MotionStatus = momentary
            self._state.add_vehicle(vehicle_id=vehicle_id, confirmed=cur_confirmed)
            event: ConfirmedMotionStatusUpdate = ConfirmedMotionStatusUpdate(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame,
                vehicle_id=vehicle_id,
                motion_status=cur_confirmed
            )
            events.append(event)
        return events

    def _update_existing_vehicles_and_get_events(
            self, mappings: Iterable[VehicleIdAndMomentaryStatus], *, system_state: ProcessingSystemState
    ) -> List[ConfirmedMotionStatusUpdate]:
        events: List[ConfirmedMotionStatusUpdate] = []
        for mapping in mappings:  # type: VehicleIdAndMomentaryStatus
            vehicle_id: str = mapping.vehicle_id
            momentary: MotionStatus = mapping.momentary
            prev_confirmed: MotionStatus = self._state.get_confirmed_status(vehicle_id)
            cur_confirmed: MotionStatus = self._compute_confirmed_status(prev_confirmed=prev_confirmed,
                                                                         cur_momentary=momentary)
            self._state.update_confirmed_status(vehicle_id=vehicle_id, confirmed=cur_confirmed)
            event: ConfirmedMotionStatusUpdate = ConfirmedMotionStatusUpdate(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame,
                vehicle_id=vehicle_id,
                motion_status=cur_confirmed
            )
            events.append(event)
        return events

    def _remove_dead_vehicles(self, dead_vehicle_ids: Iterable[str]) -> None:
        for vehicle_id in dead_vehicle_ids: # type: str
            self._state.remove_vehicle(vehicle_id)

    @override
    def _update_and_get_events(
            self, input_data: ConfirmedMotionStatusUpdatesInput, *, system_state: ProcessingSystemState
    ) -> ConfirmedMotionStatusUpdatesContainer:

        vehicle_id_to_cur_momentary: Dict[str, MotionStatus] = {
            momentary_update.vehicle_id: momentary_update.motion_status
            for momentary_update in input_data.momentary_motion_status_updates.updates
        }

        new_vehicle_ids: Set[str] = {event.vehicle_id for event in input_data.lifetime_events.start}
        dead_vehicle_ids: Set[str] = {event.vehicle_id for event in input_data.lifetime_events.end}
        vehicle_ids_with_momentary_in_input: Set[str] = set(vehicle_id_to_cur_momentary.keys())
        vehicle_ids_to_update: Set[str] = set.difference(vehicle_ids_with_momentary_in_input,
                                                         new_vehicle_ids)
        events_for_new: List[ConfirmedMotionStatusUpdate] = self._add_new_vehicles_and_get_events(
            [VehicleIdAndMomentaryStatus(vehicle_id=v_id,
                                         momentary=vehicle_id_to_cur_momentary[v_id])
             for v_id in new_vehicle_ids],
            system_state=system_state
        )
        events_for_updated: List[ConfirmedMotionStatusUpdate] = self._update_existing_vehicles_and_get_events(
            [VehicleIdAndMomentaryStatus(vehicle_id=v_id,
                                         momentary=vehicle_id_to_cur_momentary[v_id])
             for v_id in vehicle_ids_to_update],
            system_state=system_state
        )
        events: List[ConfirmedMotionStatusUpdate] = [*events_for_new, *events_for_updated]
        container: ConfirmedMotionStatusUpdatesContainer = ConfirmedMotionStatusUpdatesContainer(
            updates=events
        )

        self._remove_dead_vehicles(dead_vehicle_ids)

        return container


    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> ConfirmedMotionStatusUpdatesContainer:
        # these are not period-related -- no end events are necessary;
        # just remove the existing vehicle IDs for consistency
        dead_vehicle_ids: Set[str] = self._state.get_vehicle_ids()
        self._remove_dead_vehicles(dead_vehicle_ids)
        container: ConfirmedMotionStatusUpdatesContainer = ConfirmedMotionStatusUpdatesContainer.create_empty_container()
        return container