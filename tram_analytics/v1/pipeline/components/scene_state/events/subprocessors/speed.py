from datetime import datetime
from typing import List, override, Type

from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedUpdateEvent, \
    SpeedsWrapper, SpeedUpdatesContainer
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, \
    VehicleInput
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)


class SpeedUpdateGeneratorState(BaseGeneratorState):

    @override
    def _clear_own_state(self) -> None:
        pass

class SpeedUpdateGenerator(
    BaseFinalEventGenerator[EventsInputData, SpeedUpdatesContainer, SpeedUpdateGeneratorState]
):

    @override
    @classmethod
    def _get_container_class(cls) -> Type[SpeedUpdatesContainer]:
        return SpeedUpdatesContainer

    @override
    @classmethod
    def _get_new_state(cls) -> SpeedUpdateGeneratorState:
        return SpeedUpdateGeneratorState()

    @override
    def _update_and_get_events(
            self, input_data: EventsInputData, *, system_state: ProcessingSystemState
    ) -> SpeedUpdatesContainer:
        events: List[SpeedUpdateEvent] = []
        for vehicle in input_data.vehicles:  # type: VehicleInput
            # generate a speed update event
            event: SpeedUpdateEvent = SpeedUpdateEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                vehicle_id=vehicle.vehicle_id,
                speeds=SpeedsWrapper(raw=vehicle.speeds.raw_ms,
                                     smoothed=vehicle.speeds.smoothed_ms),
                is_matched=vehicle.is_matched,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame
            )
            events.append(event)
        container: SpeedUpdatesContainer = SpeedUpdatesContainer(updates=events)
        return container

    @override
    def _get_events_for_end_of_processing(self,
                                          *, event_ts: datetime,
                                          truncate_events: bool) -> SpeedUpdatesContainer:
        # these are not period-related -- no end events are necessary
        return SpeedUpdatesContainer.create_empty_container()
