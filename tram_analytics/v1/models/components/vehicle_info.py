from datetime import datetime
from enum import Enum
from typing import TypeAlias, Set, Literal, Annotated

from pydantic import BaseModel, NonNegativeFloat, Field

from common.utils.custom_types import PlanarPosition
from tram_analytics.v1.models.common_types import VehicleType


# --- reference points ---

class PositionContainer(BaseModel):
    # The point's position in image (pixel) coordinates.
    image: PlanarPosition

    # The point's position in world coordinates.
    # Undefined if world coordinate conversion is turned off.
    world: PlanarPosition | None

class BaseRefPoints(BaseModel):
    bbox_centroid: PositionContainer
    bbox_lower_border_midpoint: PositionContainer

class TrackCentrelinePositions(BaseModel):
    # For the track with respect to which these reference points are calculated,
    # a set of points is found where the track's centreline intersects the border of the vehicle's bounding box.
    #
    # If there 0 or 1 such points, the reference points are undefined
    # (i. e. the track's centreline must intersect the bounding box and do so in at least two points).
    #
    # `start` is the point closest to the centreline's start point;
    # `end` -- the one closest to the centreline's end point.
    #
    # For the segment of the track's centreline between `start` and `end`, two more points are calculated:
    # - `centre_in_image_plane`: the midpoint of the segment as defined in the image plane;
    # - `centre_in_world_plane`: the midpoint of the segment as defined in the world plane
    #   (if world coordinate conversion is defined).
    #
    # These are rough proxies for the tram's position along the track's centreline.

    start: PositionContainer
    end: PositionContainer
    centre_in_image_plane: PositionContainer
    centre_in_world_plane: PositionContainer | None

class TramRefPoints(BaseRefPoints):
    # A container for points lying on the vehicle's centreline.
    vehicle_centreline: TrackCentrelinePositions | None

class CarRefPoints(BaseRefPoints):
    pass

class VehicleReferencePoint(str, Enum):
    OBJECT_BBOX_CENTROID = "object_bbox_centroid"
    OBJECT_BBOX_LOWER_BORDER_MIDPOINT = "object_bbox_lower_border_midpoint"
    TRAM_CENTRELINE_PROXY_START = "tram_centreline_proxy_start"
    TRAM_CENTRELINE_PROXY_END = "tram_centreline_proxy_end"
    TRAM_CENTRELINE_PROXY_CENTRE_IMAGEPLANE_ONLY = "tram_centreline_proxy_centre_imageplane_only"
    TRAM_CENTRELINE_PROXY_CENTRE_WORLDPLANE_IF_PRESENT = "tram_centreline_proxy_centre_worldplane_if_present"

# --- speeds ---

class Speeds(BaseModel):
    raw: NonNegativeFloat | None
    smoothed: NonNegativeFloat | None

# --- master container objects ---

class BaseVehicleInfoInternal[RefPoints: BaseRefPoints](BaseModel):
    # track_state_id: str

    # The track ID assigned to this state by the tracker module.
    # TODO: remove?
    vehicle_id: str

    # TODO: remove?
    frame_ts: datetime

    # Whether the track state is matched (i. e. has a detection associated with it
    # rather than being a continuation hypothesis for the tracklet).
    is_presence_confirmed: bool

    zone_ids: Set[str]
    speeds: Speeds
    reference_points: RefPoints

class TramInfoInternal(BaseVehicleInfoInternal[TramRefPoints]):
    pass

class CarInfoInternal(BaseVehicleInfoInternal[CarRefPoints]):
    pass

class BaseVehicleInfo(BaseModel):
    vehicle_id: str
    frame_ts: datetime

    # Whether the track state is matched (i. e. has a detection associated with it
    # rather than being a continuation hypothesis for the tracklet).
    is_matched: bool
    zone_ids: Set[str]
    speeds: Speeds
    reference_points: BaseRefPoints

class TramInfo(BaseVehicleInfo):
    vehicle_type: Literal[VehicleType.TRAM] = VehicleType.TRAM
    reference_points: TramRefPoints

class CarInfo(BaseVehicleInfo):
    vehicle_type: Literal[VehicleType.CAR] = VehicleType.CAR
    reference_points: CarRefPoints

VehicleInfo: TypeAlias = Annotated[
    TramInfo | CarInfo,
    Field(discriminator="vehicle_type")
]
