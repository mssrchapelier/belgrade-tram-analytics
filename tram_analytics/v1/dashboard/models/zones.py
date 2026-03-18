from typing import List, Self, Dict

from pydantic import BaseModel

from tram_analytics.v1.models.components.scene_state.live_state.zones import (
    Track as SrcTrack, Platform as SrcPlatform,
    ZoneGroupsForTramContainer as SrcZoneGroupsForTramContainer,
    ZoneGroupsForCarContainer as SrcZoneGroupsForCarContainer, ZonesContainer as SrcZonesContainer
)


class TrackWithPlatforms(BaseModel):
    track: SrcTrack
    # Move platforms belonging to the track inside the track object
    platforms: List[SrcPlatform]

    @classmethod
    def from_source_track_and_platforms(
            cls, track: SrcTrack, platforms: List[SrcPlatform]
    ) -> Self:
        return cls(track=track,
                   platforms=platforms)

class ZoneGroupsForTramContainer(BaseModel):
    tracks_with_platforms: List[TrackWithPlatforms]

    @classmethod
    def from_source(cls, src_model: SrcZoneGroupsForTramContainer) -> Self:
        # correlate by track zone id
        tracks_with_platforms: List[TrackWithPlatforms] = list()
        track_id_to_track: Dict[str, SrcTrack] = {
            track.metadata.zone_id: track
            for track in src_model.tracks
        }
        # create a list of platforms for all tracks
        track_id_to_platform: Dict[str, List[SrcPlatform]] = {
            track_id: list()
            for track_id in track_id_to_track
        }
        # populate with platforms
        for platform in src_model.platforms: # type: SrcPlatform
            track_zone_id: str = platform.metadata.track_zone_id
            track_id_to_platform[track_zone_id].append(platform)
        # for each track and its contained platforms, create a new model
        for track_id in track_id_to_track: # type: str
            track: SrcTrack = track_id_to_track[track_id]
            # get all platforms on this track
            platforms: List[SrcPlatform] = track_id_to_platform[track_id]
            new_track_obj: TrackWithPlatforms = TrackWithPlatforms.from_source_track_and_platforms(
                track, platforms
            )
            tracks_with_platforms.append(new_track_obj)
        return cls(tracks_with_platforms=tracks_with_platforms)

class ZonesContainer(BaseModel):
    tram_zones: ZoneGroupsForTramContainer
    car_zones: SrcZoneGroupsForCarContainer

    @classmethod
    def from_source(cls, src_model: SrcZonesContainer) -> Self:
        return cls(
            tram_zones=ZoneGroupsForTramContainer.from_source(src_model.tram_zones),
            car_zones=src_model.car_zones
        )