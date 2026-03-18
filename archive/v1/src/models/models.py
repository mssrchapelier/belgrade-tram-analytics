from typing import List, Self
from warnings import deprecated

from pydantic import BaseModel

from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.tracking import DetectionToTrackState, TrackState
from tram_analytics.v1.models.components.frame_ingestion import FrameMetadata
from tram_analytics.v1.models.components.vehicle_info import VehicleInfo


class FrameWithBytes(FrameMetadata):
    image: bytes

@deprecated("Deprecated, use BoundingBox instead")
class PixelBoundingBox(BaseModel):
    x1: int
    x2: int
    y1: int
    y2: int

    @classmethod
    def from_float_bbox(cls, bbox: BoundingBox) -> Self:
        return cls(x1=round(bbox.x1),
                   x2=round(bbox.x2),
                   y1=round(bbox.y1),
                   y2=round(bbox.y2))

@deprecated("Deprecated")
class Track(BaseModel):
    track_id: str
    camera_id: str
    class_id: int

@deprecated("Deprecated, use TrackState instead")
class TrackState_Old(BaseModel):
    track_state_id: str
    track_id: str
    frame_id: str
    # whether the associated track was a confirmed one
    # at the time this detection was processed by the tracker
    is_confirmed_track: bool
    bbox: BoundingBox

@deprecated("Deprecated, use TrackerStepOutput instead")
class TrackerStepOutput_Old(BaseModel):
    new_tracks: List[Track]
    track_states: List[TrackState_Old]
    track_state_to_detection_mappings: List[DetectionToTrackState]

@deprecated("Deprecated")
class TrackerStepOutputBuilder:

    def __init__(self):
        # self.frame_id: str = frame_id

        self.new_tracks: List[Track] = []
        self.track_states: List[TrackState_Old] = []
        self.track_state_to_detection_mappings: List[DetectionToTrackState] = []

    def export(self) -> TrackerStepOutput_Old:
        return TrackerStepOutput_Old(new_tracks=self.new_tracks,
                                     track_states=self.track_states,
                                     track_state_to_detection_mappings=self.track_state_to_detection_mappings)

@deprecated("Deprecated")
class ExperimentalTrackerEstimate(BaseModel):
    estimated_box_id: str
    frame_id: str
    class_id: int
    track_id: str
    bbox: BoundingBox

@deprecated("Deprecated")
class TrackAssignment(BaseModel):
    # Currently CAN be None, because the documentation for the SORT tracker appears to imply
    # that there can be situations where a track ID would not be assigned to a detection.
    # TODO: This should not happen at all. Analyse the Sort class
    #  to determine whether such situations are possible,
    #  and, if yes, ensure that a track ID is always returned for every box.
    #  Then, remove None from the set of possible types for track_id.
    track_id: str | None

    detection_id: str

# up to and including derived vehicle info
class MainPipelineArtefacts(BaseModel):
    frame_metadata: FrameMetadata
    track_states: List[TrackState]
    vehicles_info: List[VehicleInfo]

    detection: List[Detection]
    det_to_track_state: List[DetectionToTrackState]
