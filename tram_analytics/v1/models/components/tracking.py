from typing import List

from pydantic import BaseModel

from tram_analytics.v1.models.common_types import BoundingBox


class TrackState(BaseModel):
    track_state_id: str
    class_id: int
    # TODO: rename to vehicle_id?
    track_id: str

    # whether, at the time this detection was processed by the tracker,
    # - this track state was matched with a detection from the detection module
    is_matched: bool
    # - the associated track was a confirmed one
    is_confirmed_track: bool

    # the bounding box computed by the tracker
    bbox: BoundingBox


class TrackHistory(BaseModel):
    # TODO: rename to vehicle_id?
    # (this very UUID is what is called vehicle_id everywhere downstream from the tracking module)
    track_id: str
    class_id: int
    history: List[TrackState]


class DetectionToTrackState(BaseModel):
    track_state_id: str
    detection_id: str
