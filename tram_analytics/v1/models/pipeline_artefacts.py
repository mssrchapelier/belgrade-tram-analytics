from datetime import datetime
from typing import List

from pydantic import BaseModel

from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.frame_ingestion import FrameMetadata
from tram_analytics.v1.models.components.scene_state.events.scene_events import SceneEventsWrapper
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState
from tram_analytics.v1.models.components.tracking import TrackState, DetectionToTrackState
from tram_analytics.v1.models.components.vehicle_info import VehicleInfo


class PipelineArtefacts(BaseModel):
    """
    The master DTO containing the output of all processing stages.
    """

    # When the pipeline started to process the associated frame.
    processing_start_ts: datetime
    # When the pipeline created this object.
    # Measures the time spent on core processing, but excludes rendering of the annotated image.
    artefacts_creation_ts: datetime

    frame_metadata: FrameMetadata

    # detection output
    detection: List[Detection]

    # tracking output
    track_states: List[TrackState]
    det_to_track_state: List[DetectionToTrackState]

    # derived vehicle info processor output
    vehicles_info: List[VehicleInfo]

    # scene state updater output
    scene_events: SceneEventsWrapper
    live_state: LiveAnalyticsState

# Intermediate DTO up to and including derived vehicle info
class MainPipelineArtefacts(BaseModel):
    frame_metadata: FrameMetadata
    track_states: List[TrackState]
    vehicles_info: List[VehicleInfo]

    detection: List[Detection]
    det_to_track_state: List[DetectionToTrackState]
