from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.frame_ingestion import FrameMetadata
from tram_analytics.v1.models.components.scene_state.events.scene_events import SceneEventsWrapper
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState
from tram_analytics.v1.models.components.tracking import TrackState, DetectionToTrackState
from tram_analytics.v1.models.components.vehicle_info import VehicleInfo


class PipelineArtefacts(BaseModel):
    """
    The master DTO containing the output of all processing stages of the pipeline
    for a single frame.
    """

    processing_start_ts: datetime = Field(
        description="The timestamp (UTC) at which the pipeline started to process the associated frame."
    )
    artefacts_creation_ts: datetime = Field(
        description=(
            "The timestamp (UTC) corresponding to the moment at which the pipeline created this object. "
            "Measures the time spent on core processing, but excludes rendering of the annotated image."
        )
    )

    frame_metadata: FrameMetadata = Field(
        description="Metadata for the associated frame."
    )

    # detection output
    detection: List[Detection] = Field(
        description="The list of raw object detection results for the frame."
    )

    # tracking output
    track_states: List[TrackState] = Field(
        description="The list of the outputs of the object tracking module (track states) for this frame."
    )
    det_to_track_state: List[DetectionToTrackState] = Field(
        description=(
            "The list of associations between the IDs of the track states "
            "and the IDs of raw detections present in this frame."
        )
    )

    # derived vehicle info processor output
    vehicles_info: List[VehicleInfo] = Field(
        description=(
            "The list of objects containing derived information about individual vehicles "
            "(speeds, computed reference points for zone assignment, computed zone IDs)."
        )
    )

    # scene state updater output
    scene_events: SceneEventsWrapper = Field(
        description=(
            "The list of computed scene events associated with this frame "
            "that are relevant to monitoring (zone entrance/exit, "
            "zone becoming occupied/empty, vehicles becoming stationary/moving)."
        )
    )
    live_state: LiveAnalyticsState = Field(
        description=(
            "A comprehensive description of the scene being monitored "
            "as of the update represented by this frame "
            "(detailed statistics for each vehicle and zone)."
        )
    )
