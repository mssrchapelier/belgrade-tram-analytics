from typing import Dict

from pydantic import BaseModel, Field

from tram_analytics.v1.models.common_types import VehicleType


class SingleClassSortParams(BaseModel):

    # If the track has not been detected for `max_age` frames, terminate it.
    max_age: int = Field(ge=0,
                         default=1)

    # The minimum number of frames for which a track must have been detected
    # before it starts to be included in the output.
    #
    # NOTE: Setting `min_hits` to a value higher than 1 will probably result
    # in detections corresponding to new tracks to not be assigned track IDs
    # for `min-hits - 1` frames.
    # TODO: Downstream logic depends on every detection having an associated track.
    #  Should probably hardcode always initialising `Sort` with `min_hits` set to `0`
    #  and remove this field.
    min_hits: int = Field(ge=0,
                          default=3)

    # The minimum IoU between the observed bounding box and the predicted one
    # for the same track to be assigned to the object.
    iou_threshold: float = Field(ge=0.0, le=1.0,
                                 default=0.3)

# TODO: offload to a config file!!
TRACKER_PARAMS: Dict[VehicleType, SingleClassSortParams] = {
    VehicleType.TRAM: SingleClassSortParams(max_age=3, min_hits=2),
    VehicleType.CAR: SingleClassSortParams(max_age=3, min_hits=2)
}
