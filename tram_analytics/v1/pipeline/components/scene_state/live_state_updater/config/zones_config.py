from typing import Literal, TypeAlias, Annotated, List

from pydantic import BaseModel, Field

from tram_analytics.v1.models.common_types import ZoneType


class BaseZoneConfig(BaseModel):
    zone_id: str
    # numerical ID for the zone (camera-specific)
    zone_numerical_id: int
    description: str

class TrackConfig(BaseZoneConfig):
    zone_type: Literal[ZoneType.TRACK] = ZoneType.TRACK

class PlatformConfig(BaseZoneConfig):
    zone_type: Literal[ZoneType.PLATFORM] = ZoneType.PLATFORM
    # the zone id of the track to which this platform belongs to
    track_zone_id: str

class IntrusionZoneConfig(BaseZoneConfig):
    zone_type: Literal[ZoneType.INTRUSION_ZONE] = ZoneType.INTRUSION_ZONE

SingleZoneConfig: TypeAlias = Annotated[
    TrackConfig | PlatformConfig | IntrusionZoneConfig,
    Field(discriminator="zone_type")
]

class ZonesConfig(BaseModel):
    tracks: List[TrackConfig]
    platforms: List[PlatformConfig]
    intrusion_zones: List[IntrusionZoneConfig]
