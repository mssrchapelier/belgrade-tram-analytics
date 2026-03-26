from typing import List

from tram_analytics.v1.pipeline.components.scene_state.live_state_updater.config.zones_config import (
    ZonesConfig as DestZonesConfig, TrackConfig as DestTrackConfig, PlatformConfig as DestPlatformConfig,
    IntrusionZoneConfig as DestIntrusionZoneConfig
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.zones.zones_config import (
    ZonesConfig as MainPipelineZonesConfig
)


def convert_zones_config(src_config: MainPipelineZonesConfig) -> DestZonesConfig:
    """
    Build a zones config to be used by the live state updater
    from a zones config used for the main pipeline.
    """
    tracks: List[DestTrackConfig] = [
        DestTrackConfig(zone_id=src_track.zone_id,
                        zone_numerical_id=src_track.zone_numerical_id,
                        description=src_track.description)
        for src_track in src_config.tracks.zones
    ]
    platforms: List[DestPlatformConfig] = [
        DestPlatformConfig(zone_id=src_platform.zone_id,
                           zone_numerical_id=src_platform.zone_numerical_id,
                           description=src_platform.description,
                           track_zone_id=src_platform.coords.track_zone_id)
        for src_platform in src_config.platforms.zones
    ]
    intrusion_zones: List[DestIntrusionZoneConfig] = [
        DestIntrusionZoneConfig(zone_id=src_zone.zone_id,
                                zone_numerical_id=src_zone.zone_numerical_id,
                                description=src_zone.description)
        for src_zone in src_config.intrusion_zones.zones
    ]
    dest_config: DestZonesConfig = DestZonesConfig(
        tracks=tracks, platforms=platforms, intrusion_zones=intrusion_zones
    )
    return dest_config