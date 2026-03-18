from datetime import datetime
from typing import List, Self, Dict

from pydantic import BaseModel

from tram_analytics.v1.models.components.scene_state.live_state.speeds import LifetimeSpeeds
from tram_analytics.v1.models.components.scene_state.live_state.vehicles import (
    TrackInfoForVehicle, PlatformInfoForVehicle,
    ZoneInfosForTramContainer as SrcZoneInfosForTramContainer, MotionInfoContainer,
    Tram as SrcTram, Car as SrcCar,
    VehiclesContainer as SrcVehiclesContainer
)


class TrackWithPlatformsInfoForVehicle(BaseModel):
    track: TrackInfoForVehicle
    platforms: List[PlatformInfoForVehicle]

    @classmethod
    def from_source_track_and_platforms(
            cls,
            track: TrackInfoForVehicle,
            platforms: List[PlatformInfoForVehicle]
    ) -> Self:
        return cls(track=track, platforms=platforms)

class ZoneInfosForTramContainer(BaseModel):
    tracks_with_platforms: List[TrackWithPlatformsInfoForVehicle]

    @classmethod
    def from_source(cls, src_model: SrcZoneInfosForTramContainer) -> Self:
        # correlate by track zone id
        tracks_with_platforms: List[TrackWithPlatformsInfoForVehicle] = list()
        track_id_to_track: Dict[str, TrackInfoForVehicle] = {
            track.zone_id: track
            for track in src_model.tracks
        }
        # create a list of platforms for all tracks
        track_id_to_platform: Dict[str, List[PlatformInfoForVehicle]] = {
            track_id: list()
            for track_id in track_id_to_track
        }
        # populate with platforms
        for platform in src_model.platforms:  # type: PlatformInfoForVehicle
            track_zone_id: str = platform.track_zone_id
            track_id_to_platform[track_zone_id].append(platform)
        # for each track and its contained platforms, create a new model
        for track_id in track_id_to_track:  # type: str
            track: TrackInfoForVehicle = track_id_to_track[track_id]
            # get all platforms on this track
            platforms: List[PlatformInfoForVehicle] = track_id_to_platform[track_id]
            new_track_obj: TrackWithPlatformsInfoForVehicle = (
                TrackWithPlatformsInfoForVehicle.from_source_track_and_platforms(
                    track, platforms
                )
            )
            tracks_with_platforms.append(new_track_obj)
        return cls(tracks_with_platforms=tracks_with_platforms)

class Tram(BaseModel):
    vehicle_id: str
    present_since_ts: datetime
    speed: LifetimeSpeeds
    motion: MotionInfoContainer
    zones: ZoneInfosForTramContainer

    @classmethod
    def from_source(cls, src_model: SrcTram) -> Self:
        return cls(
            vehicle_id=src_model.vehicle_id,
            present_since_ts=src_model.present_since_ts,
            speed=src_model.speed,
            motion=src_model.motion,
            zones=ZoneInfosForTramContainer.from_source(src_model.zones)
        )

class VehiclesContainer(BaseModel):
    trams: List[Tram]
    cars: List[SrcCar]

    @classmethod
    def from_source(cls, src_model: SrcVehiclesContainer) -> Self:
        trams: List[Tram] = [Tram.from_source(src_tram)
                             for src_tram in src_model.trams]
        return cls(trams=trams,
                   cars=src_model.cars)