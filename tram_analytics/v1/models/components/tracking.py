from typing import List

from pydantic import BaseModel, Field

from tram_analytics.v1.models.common_types import BoundingBox, VehicleType


class TrackState(BaseModel):
    """
    A track state object, describing the current state (associated with a specific frame) 
    of the tracker with respect to a specific vehicle being tracked.

    As long as the vehicle is being tracked, from the moment it is detected as a purported track 
    and until the moment the track is discarded, every frame will have a track state assigned to it.
    Each such track is assigned a `track_id` which remains the same during the track's lifetime,
    and is, then, a time series consisting of individual track states.

    Each track state is assigned a unique `track_state_id`, and corresponds to a specific frame.

    At the moment of creation of the track, for a certain number of frames 
    determined by the tracker module's parameters (possibly zero), 
    the track is treated as unconfirmed (`is_confirmed_track` being set to `False`). 
    If a track has been marked as confirmed, it will not be marked as unconfirmed again 
    for the duration of its lifetime.

    If the tracker was able to match the track state with a raw detection 
    that came from the object detection module, it is marked as matched (`is_matched` set to `True`); 
    otherwise, as unmatched (`is_matched` set to `False`). 
    Tracks for which a certain number of consecutive track states have been marked as unmatched 
    are discarded, as per the module's logic.

    The tracker computes an interpolated purported position 
    for the object (the tracker bounding box), which is stored in `bbox`.
    This may be different from the bounding box associated with the raw detection.
    Normally, for any downstream analytics, the tracker bounding box 
    rather than the raw detection box is used. 
    """

    track_state_id: str = Field(
        description="The UUID assigned to this track state."
    )
    vehicle_type: VehicleType = Field(
        description="The vehicle class associated with this track state."
    )
    # TODO: rename to vehicle_id?
    track_id: str = Field(
        description="The UUID of the track to which this track state belongs."
    )

    is_matched: bool = Field(
        description=(
            "Whether this track state was matched with an actual raw detection "
            "from the detection module upstream."
        )
    )
    # - the associated track was a confirmed one
    is_confirmed_track: bool = Field(
        description="Whether the tracker has marked this track as confirmed."
    )

    # the bounding box computed by the tracker
    bbox: BoundingBox = Field(
        description=(
            "The interpolated bounding box (the tracker box) "
            "computed by the tracker module for the vehicle at this frame."
        )
    )


class TrackHistory(BaseModel):
    # TODO: rename to vehicle_id?
    # (this very UUID is what is called vehicle_id everywhere downstream from the tracking module)
    track_id: str
    vehicle_type: VehicleType
    history: List[TrackState]


class DetectionToTrackState(BaseModel):
    """
    A service object correlating track states to raw detections through their IDs.
    """

    track_state_id: str = Field(
        description="The ID of the track state being correlated."
    )
    detection_id: str = Field(
        description="The ID of the detection object being correlated."
    )
