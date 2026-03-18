from typing import (
    List, Dict, Set, DefaultDict, NamedTuple
)
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime

from common.utils.time_utils import datetime_to_utc_posix, posix_to_utc_datetime
from tram_analytics.v1.models.common_types import ZoneType, VehicleType
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneEntranceEvent, ZoneExitEvent, \
    ZoneTransitEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import VehicleLifetimeStartEvent, VehicleLifetimeEndEvent
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedUpdateEvent
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_global.events import StationaryStartEvent_Old, StationaryEndEvent_Old, \
    StationaryEventsContainer_Old
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_in_zone.events import (
    InZoneStationaryStartEvent, InZoneStationaryEndEvent, InZoneStationaryEventsContainer
)
from archive.v1.src.pipeline.components.scene_state.events._old.events_pipeline import EventsContainer
from tram_analytics.v1.pipeline.components.scene_state.live_state_updater.config.zones_config import SingleZoneConfig, ZonesConfig
from archive.v1.src.pipeline.components.scene_state.live_state_updater._old_2.updaters.vehicle_updater import (
    VehicleState, ExportFromVehicleState, ExportFromVehicleZonesInfoItem
)
from archive.v1.src.pipeline.components.scene_state.live_state_updater._old_2.updaters.zone_updater import SingleEventTypePeriodUpdaters, ZoneState
from tram_analytics.v1.models.components.scene_state.live_state.live_state import (
    LiveAnalyticsState, AgnosticLiveAnalyticsState, LiveStateMetadata
)
from tram_analytics.v1.models.components.scene_state.live_state.vehicles import AgnosticZoneInfoForVehicle, TrackInfoForVehicle, \
    PlatformInfoForVehicle, IntrusionZoneInfoForVehicle, AgnosticZoneInfosForVehicleContainer, \
    ZoneInfosForTramContainer, ZoneInfosForCarContainer, AgnosticVehicle, Tram, Car, AgnosticVehiclesContainer, \
    VehiclesContainer
from tram_analytics.v1.models.components.scene_state.live_state.zones import PeriodsForZone, AgnosticZoneMetadata, TrackMetadata, \
    PlatformMetadata, IntrusionZoneMetadata, AgnosticZone, Track, Platform, IntrusionZone, ZoneGroupsForTramContainer, \
    ZoneGroupsForCarContainer, AgnosticZonesContainer, ZonesContainer
from archive.v1.src.api_server.models.scene_state_settings import ServerSettings

# --- live state updater ---

# --- internal mappings ---
# generated at master updater init time
# frozen, but the fields are mutable

# --- (1) for zones ---
# populated at init time, then read from
# TODO: wrap `Dict` fields in `types.MappingProxyType` for an immutable view?

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseZonesMappings:
    """
    Mappings agnostic wrt zone types (between zone IDs, zone types, numerical IDs, descriptions).
    """

    # ZoneType -> { zone ids }
    type_to_ids: Dict[ZoneType, Set[str]]
    # zone id -> ZoneType
    id_to_type: Dict[str, ZoneType]
    # zone id -> zone numerical id
    id_to_num_id: Dict[str, int]
    # id -> description
    id_to_description: Dict[str, str]

@dataclass(frozen=True, slots=True, kw_only=True)
class ZonesMappings(BaseZonesMappings):
    """
    Additional mappings whose semantics are specific to the currently specified zone types
    (rail platform IDs to the IDs of the tracks containing the platforms;
    zone occupancy state objects for each zone).
    """

    # platform id -> track id
    ids_platform_to_track: Dict[str, str]
    # track id -> { platform ids }
    ids_track_to_platforms: Dict[str, Set[str]]
    # zone id -> ZoneState
    states: Dict[str, ZoneState]

# --- (2) for vehicle ---
# created empty at init time (just initialised, in the case of `VehicleState` instances),
# then updated as events are processed (for which reason these mappings must be mutable)

@dataclass(frozen=True, slots=True, kw_only=True)
class VehiclesMappings:
    """
    Mappings between vehicle IDs, their types, and the state objects for each vehicle alive.
    """

    # VehicleType -> { vehicle ids }
    type_to_ids: Dict[VehicleType, Set[str]]
    # vehicle id -> VehicleType
    id_to_type: Dict[str, VehicleType]
    # vehicle id -> VehicleState
    states: Dict[str, VehicleState]

# --- updater's internal state ---

# (stores metadata: camera id, current frame id and timestamp, current server settings, etc.)

class InternalStateNotInitialisedException(Exception):
    """
    To be raised whenever trying to access an `UpdaterInternalState`'s attribute
    that has not yet been initialised (e. g. a current frame ID which has not yet been set
    because the updater has not started any processing).
    """
    pass

class UpdaterInternalState:

    """
    Stores the camera ID, tracks the previous and current frame timestamps and the current frame ID,
    and ensures consistency on updates for each frame.
    """

    def __init__(self, camera_id: str) -> None:
        self._camera_id: str = camera_id
        # timestamps stored as POSIX floats, UTC time
        self._prev_frame_ts: float | None = None
        self._cur_frame_ts: float | None = None
        self._cur_frame_id: str | None = None

        self._cur_server_settings: ServerSettings | None = None

    @property
    def camera_id(self) -> str:
        return self._camera_id

    @property
    def prev_frame_ts(self) -> float | None:
        return self._prev_frame_ts

    @property
    def cur_frame_ts(self) -> float:
        if self._cur_frame_ts is None:
            raise InternalStateNotInitialisedException()
        return self._cur_frame_ts

    @property
    def cur_frame_id(self) -> str:
        if self._cur_frame_id is None:
            raise InternalStateNotInitialisedException()
        return self._cur_frame_id

    @property
    def cur_server_settings(self) -> ServerSettings | None:
        return self._cur_server_settings

    def update_for_frame(self, *, events: EventsContainer, settings: ServerSettings) -> None:
        """
        Updates the internal state with data for the new frame (with input validation)
        and the current server settings.
        """
        # camera id must stay the same
        stored_cam_id: str | None = self._camera_id
        cur_cam_id: str = events.camera_id
        if stored_cam_id != cur_cam_id:
            raise ValueError("Passed an unexpected camera ID "
                             f"(expected {stored_cam_id}, got {cur_cam_id})")

        # the frame timestamp must have increased
        prev_ts: float | None = self._cur_frame_ts
        cur_ts: float = datetime_to_utc_posix(events.frame_ts)
        if prev_ts is not None and not prev_ts < cur_ts:
            raise ValueError("The current frame timestamp must be greater than the previous one "
                             f"(previous {prev_ts}, received current: {cur_ts})")
        cur_frame_id: str = events.frame_id

        # update the state
        self._prev_frame_ts = prev_ts
        self._cur_frame_ts = cur_ts
        self._cur_frame_id = cur_frame_id
        self._cur_server_settings = settings

# --- helpers ---

# convenience wrappers for call arguments for `update_with_vehicles`
# (events are arranged by zone, vehicle IDs to add to / remove from the zone updaters are extracted,
# then wrapped in these wrappers for convenient passing around)

@dataclass
class VehicleIdSetsForZoneStateUpdate:
    """
    A holder for vehicle IDs to add/remove to/from a specific zone sub-state
    (stationary or in-zone occupancy; both of these sub-states must be updated
    with all events of the respective type present for this zone).
    """

    # vehicle_ids ...
    to_add: Set[str] = field(default_factory=set)
    to_remove: Set[str] = field(default_factory=set)

class ZoneStateUpdate(NamedTuple):
    zone_id: str
    vehicle_ids: VehicleIdSetsForZoneStateUpdate

# --- master object ---

class LiveStateUpdater:

    def __init__(self,
                 *, camera_id: str,
                 zones_config: ZonesConfig
                 ) -> None:
        self._internal_state: UpdaterInternalState = UpdaterInternalState(camera_id)

        # zone states: maintained for all zones defined in `zones_config`
        self.zones: ZonesMappings = self._init_zones(zones_config)
        # vehicle states: maintained for currently existing vehicles
        self.vehicles: VehiclesMappings = self._init_vehicles_mappings()

    # --- state initialisation helpers ---

    @staticmethod
    def _init_zones(config: ZonesConfig) -> ZonesMappings:
        # ZoneType -> { zone ids }
        type_to_ids: DefaultDict[ZoneType, Set[str]] = defaultdict(set)
        # zone id -> ZoneType
        id_to_type: Dict[str, ZoneType] = dict()
        # zone id -> zone numerical id
        id_to_num_id: Dict[str, int] = dict()
        # id -> description
        id_to_description: Dict[str, str] = dict()
        # platform id -> track id
        ids_platform_to_track: Dict[str, str] = dict()
        # track id -> { platform ids }
        ids_track_to_platforms: DefaultDict[str, Set[str]] = defaultdict(set)
        # zone id -> ZoneState
        states: Dict[str, ZoneState] = dict()

        zone_configs: List[SingleZoneConfig] = [*config.tracks,
                                                *config.platforms,
                                                *config.intrusion_zones]

        for zone in zone_configs:  # type: SingleZoneConfig
            zone_id: str = zone.zone_id
            zone_type: ZoneType = zone.zone_type
            # zone type -> add zone id
            type_to_ids[zone_type].add(zone_id)
            # zone id -> zone type
            id_to_type[zone_id] = zone_type
            # zone id -> zone numerical id
            id_to_num_id[zone_id] = zone.zone_numerical_id
            # zone id -> description
            id_to_description[zone_id] = zone.description
            # zone id -> initialise zone state
            states[zone_id] = ZoneState()
            if zone.zone_type is ZoneType.PLATFORM:
                # --- for platforms ---
                track_id: str = zone.track_zone_id
                # platform id -> track id
                ids_platform_to_track[zone_id] = track_id
                # track id -> add platform id
                ids_track_to_platforms[track_id].add(zone_id)

        mappings: ZonesMappings = ZonesMappings(type_to_ids=type_to_ids,
                                                id_to_type=id_to_type,
                                                id_to_num_id=id_to_num_id,
                                                id_to_description=id_to_description,
                                                ids_platform_to_track=ids_platform_to_track,
                                                ids_track_to_platforms=ids_track_to_platforms,
                                                states=states)
        return mappings

    @staticmethod
    def _init_vehicles_mappings() -> VehiclesMappings:
        # VehicleType -> { vehicle ids }
        vehicle_type_to_ids: Dict[VehicleType, Set[str]] = {
            v_type: set()
            for v_type in VehicleType
        }
        # vehicle id -> VehicleType
        vehicle_id_to_type: Dict[str, VehicleType] = dict()
        # vehicle id -> VehicleState
        states: Dict[str, VehicleState] = dict()
        mappings: VehiclesMappings = VehiclesMappings(type_to_ids=vehicle_type_to_ids,
                                                      id_to_type=vehicle_id_to_type,
                                                      states=states)
        return mappings

    # --- convenience getters ---

    def _get_vehicle_state(self, vehicle_id: str) -> VehicleState:
        return self.vehicles.states[vehicle_id]

    def _get_zone_state(self, zone_id: str) -> ZoneState:
        return self.zones.states[zone_id]

    def _get_zone_type(self, zone_id: str) -> ZoneType:
        return self.zones.id_to_type[zone_id]

    def _get_vehicle_type(self, vehicle_id: str) -> VehicleType:
        if vehicle_id not in self.vehicles.id_to_type:
            raise ValueError(f"Can't get the vehicle type for vehicle {vehicle_id}: no such vehicle registered")
        return self.vehicles.id_to_type[vehicle_id]

    # --- state validation methods ---

    def _check_vehicle_not_initialised(self, vehicle_id: str) -> None:
        if vehicle_id in self.vehicles.states:
            raise RuntimeError(f"Vehicle {vehicle_id} has been initialised: "
                               f"already exists in vehicles.states")
        if vehicle_id in self.vehicles.id_to_type:
            raise RuntimeError(f"Vehicle {vehicle_id} has been initialised: "
                               f"already exists in vehicles.id_to_type")
        for vehicle_type in self.vehicles.type_to_ids: # type: VehicleType
            if vehicle_id in self.vehicles.type_to_ids[vehicle_type]:
                raise RuntimeError(f"Vehicle {vehicle_id} has been initialised: "
                                   f"already exists in vehicles.type_to_ids (type {vehicle_type})")

    def _check_vehicle_initialised(self, vehicle_id: str) -> None:
        if vehicle_id not in self.vehicles.states:
            raise RuntimeError(f"Vehicle {vehicle_id} has not been initialised: "
                               f"does not exist in vehicles.states")
        if vehicle_id not in self.vehicles.id_to_type:
            raise RuntimeError(f"Vehicle {vehicle_id} has not been initialised: "
                               f"does not exist in vehicles.id_to_type")
        if not any(
                vehicle_id in self.vehicles.type_to_ids[vehicle_type]
                for vehicle_type in self.vehicles.type_to_ids
        ):
            raise RuntimeError(f"Vehicle {vehicle_id} has not been initialised: "
                               f"does not exist in vehicles.type_to_ids")

    def _check_zone_registered(self, zone_id: str) -> None:
        if not zone_id in self.zones.id_to_type:
            # this should never happen if the zone config
            # contains all zones defined for the main pipeline
            raise ValueError(f"Zone {zone_id} not registered")

    # --- update methods for vehicle / zone states ---

    def _start_vehicle_lifetime(self,
                                *, frame_ts: float,
                                vehicle_type: VehicleType,
                                vehicle_id: str) -> None:
        self._check_vehicle_not_initialised(vehicle_id)

        # create a new vehicle state object for the given vehicle id
        self.vehicles.states[vehicle_id] = VehicleState(lifetime_start_ts=frame_ts)
        # register the mapping from the new vehicle id to the given vehicle type
        self.vehicles.id_to_type[vehicle_id] = vehicle_type
        # register the new vehicle id under the given vehicle type
        self.vehicles.type_to_ids[vehicle_type].add(vehicle_id)

    def _end_vehicle_lifetime(self, vehicle_id: str) -> None:
        self._check_vehicle_initialised(vehicle_id)

        # unregister the vehicle state
        self.vehicles.states.pop(vehicle_id)
        # unregister vehicle type mappings
        vehicle_type: VehicleType = self.vehicles.id_to_type.pop(vehicle_id)
        self.vehicles.type_to_ids[vehicle_type].remove(vehicle_id)

    def _register_entrances_and_exits_for_zone(
            self, *, frame_ts: float, zone_id: str,
            vehicles_to_add: Set[str], vehicles_to_remove: Set[str]
    ) -> None:
        self._check_zone_registered(zone_id)
        for v_to_add in vehicles_to_add: # type: str
            self._check_vehicle_initialised(v_to_add)
            vehicle_state: VehicleState = self._get_vehicle_state(v_to_add)
            # update this vehicle's state with the zone
            vehicle_state.add_zone(zone_id=zone_id, zone_entrance_ts=frame_ts)
        for v_to_remove in vehicles_to_remove: # type: str
            self._check_vehicle_initialised(v_to_remove)
            vehicle_state = self._get_vehicle_state(v_to_remove)
            # remove the zone from this vehicle's state
            vehicle_state.remove_zone(zone_id)
        # update the zone occupancy state
        updater: SingleEventTypePeriodUpdaters = self._get_zone_state(zone_id).occupancy
        updater.update_with_vehicles(event_ts=frame_ts,
                                     to_add=vehicles_to_add,
                                     to_remove=vehicles_to_remove)

    def _register_inzone_stationary_events_for_zone(
            self, *, frame_ts: float, zone_id: str,
            vehicles_to_add: Set[str], vehicles_to_remove: Set[str]
    ) -> None:
        self._check_zone_registered(zone_id)
        # check that all vehicles have been initialised
        map(lambda v_id: self._check_vehicle_initialised(v_id),
            set.union(vehicles_to_add, vehicles_to_remove))
        updater: SingleEventTypePeriodUpdaters = self._get_zone_state(zone_id).stationary_in_zone
        updater.update_with_vehicles(event_ts=frame_ts,
                                     to_add=vehicles_to_add,
                                     to_remove=vehicles_to_remove)

    def _update_vehicle_speeds(self, *,
                               vehicle_id: str,
                               speed_raw: float | None,
                               speed_smoothed: float | None,
                               is_matched: bool):
        self._check_vehicle_initialised(vehicle_id)

        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)

        vehicle_state.update_speeds(
            speed_raw=speed_raw, speed_smoothed=speed_smoothed, is_matched=is_matched
        )

    # --- event processing methods (by event type) ---

    # --- (1) lifetime ---

    def _update_from_lifetime_start_events(self, events: List[VehicleLifetimeStartEvent]) -> None:
        for e in events: # type: VehicleLifetimeStartEvent
            self._start_vehicle_lifetime(frame_ts=self._internal_state.cur_frame_ts,
                                         vehicle_type=e.vehicle_type,
                                         vehicle_id=e.vehicle_id)

    def _update_from_lifetime_end_events(self, events: List[VehicleLifetimeEndEvent]) -> None:
        for e in events: # type: VehicleLifetimeEndEvent
            self._end_vehicle_lifetime(e.vehicle_id)

    # --- (2) speed ---

    def _update_from_speed_events(self, events: List[SpeedUpdateEvent]) -> None:
        for e in events:  # type: SpeedUpdateEvent
            self._update_vehicle_speeds(vehicle_id=e.vehicle_id,
                                        speed_raw=e.speeds.raw,
                                        speed_smoothed=e.speeds.smoothed,
                                        is_matched=e.is_matched)

    # --- (3) stationary (global) ---

    def _update_from_global_stationary_events(self, events: StationaryEventsContainer_Old) -> None:
        for start_event in events.start:  # type: StationaryStartEvent_Old
            vehicle_state: VehicleState = self.vehicles.states[start_event.vehicle_id]
            vehicle_state.start_stationary(self._internal_state.cur_frame_ts)

            vehicle_id: str = start_event.vehicle_id
            if vehicle_id == 'd790a78078994398b2ef75310ead0090':
                print(f"Started global stationary for d790a7, frame ts: {self._internal_state.cur_frame_ts}")
        for end_event in events.end:  # type: StationaryEndEvent_Old
            vehicle_state = self.vehicles.states[end_event.vehicle_id]
            vehicle_state.end_stationary(self._internal_state.cur_frame_ts)

            vehicle_id: str = end_event.vehicle_id
            if vehicle_id == 'd790a78078994398b2ef75310ead0090':
                print(f"Ended global stationary for d790a7, frame ts: {self._internal_state.cur_frame_ts}")

    # --- (4) zone occupancy ---

    @staticmethod
    def _arrange_zone_occupancy_events_by_zone_id(
            events: ZoneTransitEventsContainer
    ) -> List[ZoneStateUpdate]:

        # zone_id -> (vehicle_ids_to_add, vehicle_ids_to_remove)
        mappings: DefaultDict[str, VehicleIdSetsForZoneStateUpdate] = defaultdict(lambda: VehicleIdSetsForZoneStateUpdate())
        for entrance_event in events.start: # type: ZoneEntranceEvent
            mappings[entrance_event.zone_id].to_add.add(entrance_event.vehicle_id)
        for exit_event in events.end: # type: ZoneExitEvent
            mappings[exit_event.zone_id].to_remove.add(exit_event.vehicle_id)
        # [ ( zone_id, (vehicle_ids_to_add, vehicle_ids_to_remove) ), ... ]
        output: List[ZoneStateUpdate] = [
            ZoneStateUpdate(zone_id=zone_id, vehicle_ids=zone_state_update)
            for zone_id, zone_state_update in mappings.items()
        ]
        return output

    def _update_from_zone_occupancy_events(self, events: ZoneTransitEventsContainer) -> None:
        # arrange by zone id
        # [ ( zone_id, (vehicle_ids_to_add, vehicle_ids_to_remove) ), ... ]
        by_zone_id: List[ZoneStateUpdate] = self._arrange_zone_occupancy_events_by_zone_id(events)
        for container in by_zone_id: # type: ZoneStateUpdate
            self._register_entrances_and_exits_for_zone(frame_ts=self._internal_state.cur_frame_ts,
                                                        zone_id=container.zone_id,
                                                        vehicles_to_add=container.vehicle_ids.to_add,
                                                        vehicles_to_remove=container.vehicle_ids.to_remove)

    # --- (5) in-zone stationary ---

    @staticmethod
    def _arrange_inzone_stationary_events_by_zone_id(
            events: InZoneStationaryEventsContainer
    ) -> List[ZoneStateUpdate]:

        # zone_id -> (vehicle_ids_to_add, vehicle_ids_to_remove)
        mappings: DefaultDict[str, VehicleIdSetsForZoneStateUpdate] = defaultdict(
            lambda: VehicleIdSetsForZoneStateUpdate())
        for start_event in events.start:  # type: InZoneStationaryStartEvent
            mappings[start_event.zone_id].to_add.add(start_event.vehicle_id)
        for end_event in events.end:  # type: InZoneStationaryEndEvent
            mappings[end_event.zone_id].to_remove.add(end_event.vehicle_id)
        # [ ( zone_id, (vehicle_ids_to_add, vehicle_ids_to_remove) ), ... ]
        output: List[ZoneStateUpdate] = [
            ZoneStateUpdate(zone_id=zone_id, vehicle_ids=zone_state_update)
            for zone_id, zone_state_update in mappings.items()
        ]
        return output

    def _update_from_inzone_stationary_events(self, events: InZoneStationaryEventsContainer) -> None:
        by_zone_id: List[ZoneStateUpdate] = self._arrange_inzone_stationary_events_by_zone_id(events)
        for container in by_zone_id:  # type: ZoneStateUpdate
            self._register_inzone_stationary_events_for_zone(frame_ts=self._internal_state.cur_frame_ts,
                                                             zone_id=container.zone_id,
                                                             vehicles_to_add=container.vehicle_ids.to_add,
                                                             vehicles_to_remove=container.vehicle_ids.to_remove)

    # --- state export (to public API) ---

    def _export_agnostic_zones(self) -> AgnosticZonesContainer:
        dest_zones: List[AgnosticZone] = []
        for zone_id, zone_state in self.zones.states.items(): # type: str, ZoneState
            zone_numerical_id: int = self.zones.id_to_num_id[zone_id]
            zone_description: str = self.zones.id_to_description[zone_id]
            metadata: AgnosticZoneMetadata = AgnosticZoneMetadata(
                zone_id=zone_id, zone_numerical_id=zone_numerical_id, description=zone_description
            )
            occupancy: PeriodsForZone = zone_state.occupancy.export_state()
            stops: PeriodsForZone = zone_state.stationary_in_zone.export_state()
            dest_zone: AgnosticZone = AgnosticZone(metadata=metadata,
                                                   occupancy=occupancy,
                                                   stops=stops)
            dest_zones.append(dest_zone)
        return AgnosticZonesContainer(all_zones=dest_zones)

    def _build_output_intrusion_zone(self, agnostic_zone: AgnosticZone) -> IntrusionZone:
        src_metadata: AgnosticZoneMetadata = agnostic_zone.metadata
        dest_metadata: IntrusionZoneMetadata = IntrusionZoneMetadata(zone_id=src_metadata.zone_id,
                                                                     zone_numerical_id=src_metadata.zone_numerical_id,
                                                                     description=src_metadata.description)
        return IntrusionZone(metadata=dest_metadata,
                             occupancy=agnostic_zone.occupancy,
                             stops=agnostic_zone.stops)
    
    def _build_output_track(self, agnostic_zone: AgnosticZone) -> Track:
        src_metadata: AgnosticZoneMetadata = agnostic_zone.metadata
        dest_metadata: TrackMetadata = TrackMetadata(zone_id=src_metadata.zone_id,
                                                     zone_numerical_id=src_metadata.zone_numerical_id,
                                                     description=src_metadata.description)
        return Track(metadata=dest_metadata,
                     occupancy=agnostic_zone.occupancy,
                     stops=agnostic_zone.stops)
    
    def _build_output_platform(self, agnostic_zone: AgnosticZone) -> Platform:
        src_metadata: AgnosticZoneMetadata = agnostic_zone.metadata

        # for platforms: also add the zone ID and numerical ID
        # for the TRACK to which this platform belongs
        zone_id: str = src_metadata.zone_id
        if not zone_id in self.zones.ids_platform_to_track:
            raise ValueError(f"Zone ID {zone_id} not found in platform-to-track "
                             f"zone ID mappings (not a platform?)")
        track_zone_id: str = self.zones.ids_platform_to_track[zone_id]
        track_zone_numerical_id: int = self.zones.id_to_num_id[track_zone_id]

        dest_metadata: PlatformMetadata = PlatformMetadata(zone_id=src_metadata.zone_id,
                                                           zone_numerical_id=src_metadata.zone_numerical_id,
                                                           description=src_metadata.description,
                                                           track_zone_id=track_zone_id,
                                                           track_zone_numerical_id=track_zone_numerical_id)
        return Platform(metadata=dest_metadata,
                        occupancy=agnostic_zone.occupancy,
                        stops=agnostic_zone.stops)

    def _agnostic_to_nonagnostic_zones(self, src: AgnosticZonesContainer) -> ZonesContainer:
        # src.all_zones -> tram_zones, car_zones -> tracks, platforms / intrusion_zones
        # ...platforms.metadata -> include track_zone_id, track_zone_numerical_id
        agnostic_zones: List[AgnosticZone] = src.all_zones
        tracks: List[Track] = []
        platforms: List[Platform] = []
        intrusion_zones: List[IntrusionZone] = []
        for agnostic_zone in agnostic_zones: # type: AgnosticZone
            zone_id: str = agnostic_zone.metadata.zone_id
            zone_type: ZoneType = self._get_zone_type(zone_id)
            match zone_type:
                case ZoneType.TRACK:
                    tracks.append(self._build_output_track(agnostic_zone))
                case ZoneType.PLATFORM:
                    platforms.append(self._build_output_platform(agnostic_zone))
                case ZoneType.INTRUSION_ZONE:
                    intrusion_zones.append(self._build_output_intrusion_zone(agnostic_zone))
                case _:
                    raise RuntimeError(f"No converter defined for zone type: {zone_type}")
        tram_zones: ZoneGroupsForTramContainer = ZoneGroupsForTramContainer(tracks=tracks,
                                                                            platforms=platforms)
        car_zones: ZoneGroupsForCarContainer = ZoneGroupsForCarContainer(intrusion_zones=intrusion_zones)
        return ZonesContainer(tram_zones=tram_zones, car_zones=car_zones)

    def _export_agnostic_vehicles(self) -> AgnosticVehiclesContainer:
        dest_vehicles: List[AgnosticVehicle] = []
        for vehicle_id, vehicle_state in self.vehicles.states.items(): # type: str, VehicleState
            # exporting the vehicle state to an intermediary object
            intermediary_vehicle: ExportFromVehicleState = vehicle_state.export_state()
            # repacking zone objects with zone id and zone numerical id
            dest_zones: List[AgnosticZoneInfoForVehicle] = []
            for intermediary_zone in intermediary_vehicle.zones.all_zones: # type: ExportFromVehicleZonesInfoItem
                zone_id: str = intermediary_zone.zone_id
                zone_numerical_id: int = self.zones.id_to_num_id[zone_id]
                dest_zone: AgnosticZoneInfoForVehicle = AgnosticZoneInfoForVehicle(
                    zone_id=zone_id, zone_numerical_id=zone_numerical_id,
                    present_in_zone_since_ts=intermediary_zone.present_in_zone_since_ts,
                    speed_in_zone_stats=intermediary_zone.speed_in_zone_stats
                )
                dest_zones.append(dest_zone)
            dest_zones_container: AgnosticZoneInfosForVehicleContainer = AgnosticZoneInfosForVehicleContainer(
                all_zones=dest_zones
            )
            # building the vehicle object
            dest_vehicle: AgnosticVehicle = AgnosticVehicle(
                vehicle_id=vehicle_id,
                present_since_ts=intermediary_vehicle.present_since_ts,
                speed=intermediary_vehicle.speed,
                stationary=intermediary_vehicle.stationary,
                zones=dest_zones_container
            )
            dest_vehicles.append(dest_vehicle)
        dest_container: AgnosticVehiclesContainer = AgnosticVehiclesContainer(
            all_vehicles=dest_vehicles
        )
        return dest_container

    def _build_output_track_info_for_vehicle(
            self, agnostic_zone_info: AgnosticZoneInfoForVehicle
    ) -> TrackInfoForVehicle:
        return TrackInfoForVehicle(zone_id=agnostic_zone_info.zone_id,
                                   zone_numerical_id=agnostic_zone_info.zone_numerical_id,
                                   present_in_zone_since_ts=agnostic_zone_info.present_in_zone_since_ts,
                                   speed_in_zone_stats=agnostic_zone_info.speed_in_zone_stats)

    def _build_output_platform_info_for_vehicle(
            self, agnostic_zone_info: AgnosticZoneInfoForVehicle
    ) -> PlatformInfoForVehicle:
        return PlatformInfoForVehicle(zone_id=agnostic_zone_info.zone_id,
                                      zone_numerical_id=agnostic_zone_info.zone_numerical_id,
                                      present_in_zone_since_ts=agnostic_zone_info.present_in_zone_since_ts,
                                      speed_in_zone_stats=agnostic_zone_info.speed_in_zone_stats)

    def _build_output_intrusion_zone_info_for_vehicle(
            self, agnostic_zone_info: AgnosticZoneInfoForVehicle
    ) -> IntrusionZoneInfoForVehicle:
        return IntrusionZoneInfoForVehicle(zone_id=agnostic_zone_info.zone_id,
                                           zone_numerical_id=agnostic_zone_info.zone_numerical_id,
                                           present_in_zone_since_ts=agnostic_zone_info.present_in_zone_since_ts,
                                           speed_in_zone_stats=agnostic_zone_info.speed_in_zone_stats)

    def _build_output_tram(self, agnostic_vehicle: AgnosticVehicle) -> Tram:
        tracks: List[TrackInfoForVehicle] = []
        platforms: List[PlatformInfoForVehicle] = []
        for agnostic_zone_info in agnostic_vehicle.zones.all_zones: # type: AgnosticZoneInfoForVehicle
            zone_id: str = agnostic_zone_info.zone_id
            zone_type: ZoneType = self._get_zone_type(zone_id)
            match zone_type:
                case ZoneType.TRACK:
                    tracks.append(self._build_output_track_info_for_vehicle(agnostic_zone_info))
                case ZoneType.PLATFORM:
                    platforms.append(self._build_output_platform_info_for_vehicle(agnostic_zone_info))
                case ZoneType.INTRUSION_ZONE:
                    raise RuntimeError(f"Got zone with ID {zone_id} that is of type {zone_type}, "
                                       f"but building output for a tram "
                                       f"(zone type {zone_type} wrongly assigned to a tram "
                                       f"or building tram output for a car?)")
                case _:
                    raise ValueError(f"No converter defined for zone type: {zone_type}")
        dest_zone_infos: ZoneInfosForTramContainer = ZoneInfosForTramContainer(
            tracks=tracks, platforms=platforms
        )
        return Tram(vehicle_id=agnostic_vehicle.vehicle_id,
                    present_since_ts=agnostic_vehicle.present_since_ts,
                    speed=agnostic_vehicle.speed,
                    stationary=agnostic_vehicle.stationary,
                    zones=dest_zone_infos)

    def _build_output_car(self, agnostic_vehicle: AgnosticVehicle) -> Car:
        intrusion_zones: List[IntrusionZoneInfoForVehicle] = []
        for agnostic_zone_info in agnostic_vehicle.zones.all_zones:  # type: AgnosticZoneInfoForVehicle
            zone_id: str = agnostic_zone_info.zone_id
            zone_type: ZoneType = self._get_zone_type(zone_id)
            match zone_type:
                case ZoneType.INTRUSION_ZONE:
                    intrusion_zones.append(self._build_output_intrusion_zone_info_for_vehicle(agnostic_zone_info))
                case ZoneType.TRACK | ZoneType.PLATFORM:
                    raise RuntimeError(f"Got zone with ID {zone_id} that is of type {zone_type}, "
                                       f"but building output for a car "
                                       f"(zone type {zone_type} wrongly assigned to a car "
                                       f"or building car output for a tram?)")
                case _:
                    raise ValueError(f"No converter defined for zone type: {zone_type}")
        dest_zone_infos: ZoneInfosForCarContainer = ZoneInfosForCarContainer(
            intrusion_zones=intrusion_zones
        )
        return Car(vehicle_id=agnostic_vehicle.vehicle_id,
                   present_since_ts=agnostic_vehicle.present_since_ts,
                   speed=agnostic_vehicle.speed,
                   stationary=agnostic_vehicle.stationary,
                   zones=dest_zone_infos)

    def _agnostic_to_nonagnostic_vehicles(self, src: AgnosticVehiclesContainer) -> VehiclesContainer:
        # src.all_vehicles -> trams / cars
        # src.vehicles.all_vehicles[idx].zones -> tracks / platforms
        agnostic_vehicles: List[AgnosticVehicle] = src.all_vehicles
        trams: List[Tram] = []
        cars: List[Car] = []
        for agnostic_vehicle in agnostic_vehicles: # type: AgnosticVehicle
            vehicle_id: str = agnostic_vehicle.vehicle_id
            vehicle_type: VehicleType = self._get_vehicle_type(vehicle_id)
            match vehicle_type:
                case VehicleType.TRAM:
                    trams.append(self._build_output_tram(agnostic_vehicle))
                case VehicleType.CAR:
                    cars.append(self._build_output_car(agnostic_vehicle))
                case _:
                    raise RuntimeError(f"No converter defined for vehicle type: {vehicle_type}")
        return VehiclesContainer(trams=trams, cars=cars)

    def _export_metadata(self) -> LiveStateMetadata:
        cur_state: UpdaterInternalState = self._internal_state

        if (
            cur_state.cur_server_settings is None
            or cur_state.cur_frame_id is None
            or cur_state.cur_frame_ts is None
        ):
            raise InternalStateNotInitialisedException(
                "This updater's internal state is not fully initialised (no frames received yet?)"
            )
        # convert the current time from POSIX seconds to a datetime object
        cur_ts_as_datetime: datetime = posix_to_utc_datetime(cur_state.cur_frame_ts)

        server_settings: ServerSettings = cur_state.cur_server_settings
        live_state_metadata: LiveStateMetadata = LiveStateMetadata(
            camera_id=cur_state.camera_id,
            frame_id=cur_state.cur_frame_id,
            frame_timestamp=cur_ts_as_datetime,
            server_settings=server_settings
        )
        return live_state_metadata

    def _export_agnostic_state(self) -> AgnosticLiveAnalyticsState:
        metadata: LiveStateMetadata = self._export_metadata()
        zones: AgnosticZonesContainer = self._export_agnostic_zones()
        vehicles: AgnosticVehiclesContainer = self._export_agnostic_vehicles()
        exported_state: AgnosticLiveAnalyticsState = AgnosticLiveAnalyticsState(
            metadata=metadata, zones=zones, vehicles=vehicles
        )
        return exported_state

    def _agnostic_to_nonagnostic_state(self, agnostic: AgnosticLiveAnalyticsState) -> LiveAnalyticsState:
        dest_zones: ZonesContainer = self._agnostic_to_nonagnostic_zones(agnostic.zones)
        dest_vehicles: VehiclesContainer = self._agnostic_to_nonagnostic_vehicles(agnostic.vehicles)
        exported_state: LiveAnalyticsState = LiveAnalyticsState(
            metadata=agnostic.metadata, zones=dest_zones, vehicles=dest_vehicles
        )
        return exported_state

    def export_state(self) -> LiveAnalyticsState:
        """
        Export the updater's current state with times defined with respect to `cur_ts`.
        """
        agnostic_state: AgnosticLiveAnalyticsState = self._export_agnostic_state()
        exported_state: LiveAnalyticsState = self._agnostic_to_nonagnostic_state(agnostic_state)
        return exported_state

    # --- master method ---

    def update_and_export_state(self, *, events: EventsContainer, settings: ServerSettings) -> LiveAnalyticsState:

        self._internal_state.update_for_frame(events=events, settings=settings)

        # NOTE: order DOES matter in that:
        # - lifetime start events must come first (initialise vehicle states);
        # - lifetime end events must be the last (destroy vehicle states).
        # The rest are designed to be independent of each other,
        # but ARE dependent on the existence of the vehicle states
        # for the vehicles concerned.

        # (1) lifetime start
        #     MUST COME FIRST
        #     - initialises vehicle states -- accessed by: speeds, global stationary status, zone occupancy
        self._update_from_lifetime_start_events(events.pipeline_steps.canonical.lifetime.start)

        # (2) speeds
        #     - updates:
        #         * vehicle states: for speed stats during the vehicle's lifetime
        #         * zone occupancy substates: for speed stats during the current zone occupancy
        self._update_from_speed_events(events.pipeline_steps.canonical.speeds)

        # (3) global stationary status
        #     - updates:
        #         * vehicle states: the previous stationary period start/end, the current stationary period start
        self._update_from_global_stationary_events(events.pipeline_steps.stationary_global)

        # (4) zone occupancy (entrances / exits)
        #     - updates:
        #         * zone occupancy substates: current and maximum vehicle counts, since when the zone has been occupied,
        #           the start and end of the previous occupancy period
        self._update_from_zone_occupancy_events(events.pipeline_steps.canonical.zone_transit)

        # (5) in-zone stationary status
        #     - updates:
        #         * zone stationary-in-zone substates:
        #           > the previous in-zone stationary period start (several timestamps,
        #             if ended for several vehicles at once) and end,
        #           > for every vehicle currently stationary in the zone, its in-zone stationary period start.
        self._update_from_inzone_stationary_events(events.pipeline_steps.stationary_in_zone)

        # (6) lifetime end
        #     MUST BE THE LAST
        #     - removes vehicle states for dead tracklets
        self._update_from_lifetime_end_events(events.pipeline_steps.canonical.lifetime.end)

        # export the state
        exported_state: LiveAnalyticsState = self.export_state()
        return exported_state