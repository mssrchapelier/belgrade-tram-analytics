from typing import List, TypeAlias, Tuple

from pydantic import BaseModel, ConfigDict

from common.utils.custom_types import PlanarPosition
from common.utils.pydantic.types_pydantic import OpenUnitIntervalValue


# --- zone type settings ---

class BaseZoneTypeSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

class IntrusionZoneSettings(BaseZoneTypeSettings):
    # The minimum fraction of the area of a vehicle's bounding box that needs to be inside the zone's polygon
    # for the zone to be assigned to this vehicle.
    min_area_frac_inside_zone: OpenUnitIntervalValue

class RailTrackSettings(BaseZoneTypeSettings):
    pass

class RailPlatformSettings(BaseZoneTypeSettings):
    pass

# --- coordinates configs ---

class BaseZoneCoordsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

class BasePolygonZoneCoordsConfig(BaseZoneCoordsConfig):
    polygon: List[PlanarPosition]

class IntrusionZoneCoordsConfig(BasePolygonZoneCoordsConfig):
    pass

class RailTrackCoordsConfig(BasePolygonZoneCoordsConfig):
    centreline: List[PlanarPosition]

RailPlatformEndpointSupportingLine: TypeAlias = Tuple[PlanarPosition, PlanarPosition]

class RailPlatformCoordsConfig(BaseZoneCoordsConfig):

    # The ID of the rail track ID with which this platform is associated.
    track_zone_id: str

    # Two polylines (each consisting of just two points) are defined;
    # the points at which they cross the centreline of the associated rail track
    # become the endpoints of the rail platform.
    #
    # One can imagine drawing these two lines in the GUI across the rail track's centreline
    # in order to define the bounds of the platform.
    #
    # Example: [
    #     ( (line1_x1, line1_y1), (line1_x2, line1_y2) ), # line 1 start and end
    #     ( (line2_x1, line2_y1), (line2_x2, line2_y2) )  # line 2 start and end
    # ]
    # -- intersection of line 1 and track centreline --> endpoint 1
    # -- intersection of line 2 and track centreline --> endpoint 2

    platform_endpoints_supporting_lines: Tuple[
        RailPlatformEndpointSupportingLine, RailPlatformEndpointSupportingLine
    ]

# --- single zone configs ---

class BaseSingleZoneConfig[C: BaseZoneCoordsConfig](BaseModel):
    zone_id: str
    # A numerical ID for the zone for human readability (zone type-specific, camera-specific).
    zone_numerical_id: int
    description: str
    coords: C

    # NOTE:
    #
    # Without setting the model's `frozen` attribute to `True`,
    # it is impossible to use generics with subtypes of this config
    # whilst preserving the subtyping relationship (i. e. the generics are not treated as covariant without this).
    # Note, however, that even an empty config causes the respective mypy warning to disappear:
    #
    # model_config = ConfigDict()
    #
    # This is NOT expected behaviour (should maybe leave a bug report to that effect).

    model_config = ConfigDict(frozen=True)

class BaseSinglePolygonZoneConfig[C: BasePolygonZoneCoordsConfig](BaseSingleZoneConfig[C]):
    pass

class SingleIntrusionZoneConfig(BaseSinglePolygonZoneConfig[IntrusionZoneCoordsConfig]):
    pass

class SingleRailTrackConfig(BaseSinglePolygonZoneConfig[RailTrackCoordsConfig]):
    pass

class SingleRailPlatformConfig(BaseSingleZoneConfig[RailPlatformCoordsConfig]):
    pass

# --- wrappers by zone type ---

class BaseZoneSetConfig[
    ZCConfig: BaseZoneCoordsConfig,
    # SZConfig: BaseSingleZoneConfig[BaseZoneCoordsConfig],
    ZTSettings: BaseZoneTypeSettings
](BaseModel):
    """
    A container for definitions of all zones of the same type,
    as well as for common settings related to this zone type.
    """

    assignment_settings: ZTSettings
    zones: List[BaseSingleZoneConfig[ZCConfig]]

    model_config = ConfigDict(frozen=True)

class IntrusionZoneConfig(BaseZoneSetConfig[IntrusionZoneCoordsConfig, IntrusionZoneSettings]):
    pass

class RailTrackConfig(BaseZoneSetConfig[RailTrackCoordsConfig, RailTrackSettings]):
    assignment_settings: RailTrackSettings = RailTrackSettings()

class RailPlatformConfig(BaseZoneSetConfig[RailPlatformCoordsConfig, RailPlatformSettings]):
    assignment_settings: RailPlatformSettings = RailPlatformSettings()

# --- master config ---

class ZonesConfig(BaseModel):
    tracks: RailTrackConfig
    platforms: RailPlatformConfig
    intrusion_zones: IntrusionZoneConfig

    model_config = ConfigDict(frozen=True)
