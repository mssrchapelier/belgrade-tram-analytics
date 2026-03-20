"""
A wrapper for the events pipeline and the live state updater to be chained together.
"""

from tram_analytics.v1.models.pipeline_artefacts import MainPipelineArtefacts
from tram_analytics.v1.models.components.scene_state.events.scene_events import EventsContainer
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneStateUpdaterConfig
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.converter_from_main_to_event_input import (
    convert_vehicle_info
)
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_pipeline import EventPipeline
from tram_analytics.v1.pipeline.components.scene_state.live_state_updater.live_state_updater import LiveStateUpdater
from tram_analytics.v1.pipeline.components.scene_state.models import SceneState


class SceneStateUpdater:

    def __init__(self, *, camera_id: str, config: SceneStateUpdaterConfig) -> None:
        self._config: SceneStateUpdaterConfig = config
        self._events_pipeline: EventPipeline = EventPipeline(
            config.scene_events, camera_id=camera_id
        )
        self._live_state_updater: LiveStateUpdater = LiveStateUpdater(
            camera_id=camera_id, zones_config=config.zones
        )

    def update_and_get_events(self, main_pipeline_artefacts: MainPipelineArtefacts) -> SceneState:
        events_pipeline_input: EventsInputData = convert_vehicle_info(main_pipeline_artefacts)
        events: EventsContainer = self._events_pipeline.update_and_get_events(events_pipeline_input)
        live_state: LiveAnalyticsState = self._live_state_updater.update_and_export_state(
            events=events, settings=self._config.scene_events
        )
        container: SceneState = SceneState(frame_metadata=events.metadata,
                                           scene_events=events.pipeline_steps,
                                           live_state=live_state)
        return container

    def end_processing_and_get_events(self) -> SceneState:
        events: EventsContainer = self._events_pipeline.end_processing_and_get_events()
        live_state: LiveAnalyticsState = self._live_state_updater.update_and_export_state(
            events=events, settings=self._config.scene_events
        )
        container: SceneState = SceneState(frame_metadata=events.metadata,
                                           scene_events=events.pipeline_steps,
                                           live_state=live_state)
        return container