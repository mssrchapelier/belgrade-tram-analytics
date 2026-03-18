from typing import (
    List, Dict, Literal, Type, Set, Iterator, TypeAlias, Self, Annotated, DefaultDict
)
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections import defaultdict
from warnings import deprecated

from pydantic import BaseModel, Field, NonNegativeFloat, PositiveInt, model_validator

from tram_analytics.v1.models.common_types import ZoneType, VehicleType
from archive.v1.src.pipeline.components.scene_state.events._old.events_old import EventBoundaryType, LifetimeEvent, ZoneOccupancyEvent, SpeedUpdateEvent, EventsContainer
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState

# --- vehicle states ---

# --- (1) speed states (current and max, raw and smoothed) ---

class SpeedUpdateException(Exception):
    pass

class BaseSpeedsContainer(ABC):

    def __init__(self) -> None:

        # Whether a numerical current speed has been passed to this instance in `.update()`.
        # Used to enforce the following RESTRICTION:
        #     For every `n`, for every `speed_type` in `{ raw_kmh, smoothed_kmh }`,
        #     if `speed_type` was a `float` at frame `n`,
        #     then it must also be a `float` at frame `n+1`.
        #     In other words, once the speed value has been set to a numerical value,
        #     it cannot be set to None for subsequent frames.
        self._is_speeds_set: bool = False

        self.max: float | None = None

    def _validate_speed(self, speed: float | None) -> None:
        if speed is not None and not speed >= 0.0:
            raise ValueError("speed must be a non-negative float or None")
        if self._is_speeds_set and speed is None:
            raise SpeedUpdateException(
                "Cannot set the current speed to None: has already been assigned to a numerical value. "
                "Once the speed is set to a numerical value, it can only be set to numerical values "
                "subsequently."
            )

    def _update_max_speed(self, current: float | None) -> None:
        if self._is_speeds_set and current is None:
            raise RuntimeError("Invalid state: _is_speeds_set is set to True but current speed passed is None")
        if (not self._is_speeds_set) and (current is not None):
            raise RuntimeError("Invalid state: _is_speeds_set is set to False but current speed passed is not None "
                               "(update _is_speeds_set first)")
        if self._is_speeds_set and (self.max is None or current > self.max):
            self.max = current

    @abstractmethod
    def _update_state(self, current: float | None) -> None:
        pass

    def update(self, current: float | None) -> None:
        self._validate_speed(current)
        self._update_state(current)
        if current is not None and not self._is_speeds_set:
            self._is_speeds_set = True
        self._update_max_speed(current)

class MaxSpeedContainer(BaseSpeedsContainer):
    """
    A container for storing the maximum speed only.
    """

    def _update_state(self, current: float | None) -> None:
        pass

class SpeedsContainer(BaseSpeedsContainer):
    """
    A container for storing the current and maximum speed.
    """

    def __init__(self) -> None:
        self.current: float | None = None
        super().__init__()

    def _update_state(self, current: float | None) -> None:
        self.current = current

class BaseSpeeds(ABC):

    def __init__(self, speeds_container_type: Type[BaseSpeedsContainer]) -> None:
        # self._speeds_container_type: Type[BaseSpeedsContainer] = speeds_container_type

        self.raw: BaseSpeedsContainer = speeds_container_type()
        self.smoothed: BaseSpeedsContainer = speeds_container_type()

    def update(self, *, raw: float | None, smoothed: float | None) -> None:
        self.raw.update(raw)
        self.smoothed.update(smoothed)

class MaxSpeeds(BaseSpeeds):

    def __init__(self) -> None:
        super().__init__(MaxSpeedContainer)

class CurAndMaxSpeeds(BaseSpeeds):

    def __init__(self) -> None:
        super().__init__(SpeedsContainer)
        self.is_stationary: bool | None = None

    def _update_is_stationary(self) -> bool | None:
        # TODO: implement
        raise NotImplementedError()

    def update(self, *, raw: float | None, smoothed: float | None) -> None:
        super().update(raw=raw, smoothed=smoothed)
        self.is_stationary = self._update_is_stationary()

# --- (2) zone info (for zones in which the vehicle is currently present) ---

class BaseVehicleSingleZoneInfo:

    def __init__(self, *, vehicle_zone_enter_ts: float) -> None:

        # zone_id: str

        self.vehicle_zone_enter_ts: float = vehicle_zone_enter_ts
        self.max_speeds_in_zone: MaxSpeeds = MaxSpeeds()

    def update(self, *,
               speed_raw: float | None,
               speed_smoothed: float | None) -> None:
        self.max_speeds_in_zone.update(raw=speed_raw, smoothed=speed_smoothed)

class VehicleTrackInfo(BaseVehicleSingleZoneInfo):
    pass

class VehiclePlatformInfo(BaseVehicleSingleZoneInfo):
    pass

class VehicleIntrusionZoneInfo(BaseVehicleSingleZoneInfo):
    pass

class BaseVehicleZonesInfo(ABC):

    def __init__(self, defined_zone_types: Set[ZoneType]):
        self._defined_zone_types = defined_zone_types

    def _validate_zone_type(self, zone_type: ZoneType) -> None:
        if zone_type not in self._defined_zone_types:
            msg: str = "zone_type must be one of: {}".format(
                ", ".join(t.value for t in self._defined_zone_types)
            )
            raise ValueError(msg)

    @abstractmethod
    def _get_zone_dict_by_type(self, zone_type: ZoneType) -> Dict[str, BaseVehicleSingleZoneInfo]:
        pass

    def _get_zone_iterator(self) -> Iterator[BaseVehicleSingleZoneInfo]:
        """
        Iterate over all zones.
        """
        for zone_type in self._defined_zone_types: # type: ZoneType
            zone_dict: Dict[str, BaseVehicleSingleZoneInfo] = self._get_zone_dict_by_type(zone_type)
            for zone_info in zone_dict.values(): # type: BaseVehicleSingleZoneInfo
                yield zone_info

    @abstractmethod
    def _get_zone_info_subclass_by_type(self, zone_type: ZoneType) -> Type[BaseVehicleSingleZoneInfo]:
        pass

    def add_zone(self, *args, zone_type: ZoneType, zone_id: str, **kwargs) -> None:
        """
        Create a new zone of type `zone_type` and add to the respective `zone_dict`,
        using `zone_id` as the key.
        """
        self._validate_zone_type(zone_type)

        zone_dict: Dict[str, BaseVehicleSingleZoneInfo] = self._get_zone_dict_by_type(zone_type)
        if zone_id in zone_dict:
            raise ValueError(f"Cannot create zone {zone_id} of type {zone_type.value}: already exists in the state")
        zone_info_class: Type[BaseVehicleSingleZoneInfo] = self._get_zone_info_subclass_by_type(zone_type)
        zone_info: zone_info_class = zone_info_class(*args, **kwargs)
        zone_dict[zone_id] = zone_info

    @deprecated("Deprecated, use update_all_zones() instead")
    def update_zone(self, *args, zone_type: ZoneType, zone_id: str, **kwargs) -> None:
        """
        Update data for an existing zone (with the given `zone_type` and `zone_id`).

        # TODO: remove this method?
        """
        self._validate_zone_type(zone_type)

        zone_dict: Dict[str, BaseVehicleSingleZoneInfo] = self._get_zone_dict_by_type(zone_type)
        zone_info: BaseVehicleSingleZoneInfo = zone_dict.get(zone_id)
        if zone_info is None:
            raise ValueError(f"Cannot update zone {zone_id} of type {zone_type.value}: does not exist in the state")
        zone_info.update(*args, **kwargs)

    def update_all_zones(self, *args, **kwargs) -> None:
        """
        Update data for all zones, of all types.
        """
        for zone_info in self._get_zone_iterator(): # type: BaseVehicleSingleZoneInfo
            zone_info.update(*args, **kwargs)

    def remove_zone(self, zone_type: ZoneType, zone_id: str) -> None:
        """
        Remove data for an existing zone (with the given `zone_type` and `zone_id`).
        """
        self._validate_zone_type(zone_type)

        zone_dict: Dict[str, BaseVehicleSingleZoneInfo] = self._get_zone_dict_by_type(zone_type)
        if zone_id not in zone_dict:
            raise ValueError(f"Cannot remove zone {zone_id} of type {zone_type.value}: does not exist in the state")
        zone_dict.pop(zone_id)

class TramZonesInfo(BaseVehicleZonesInfo):

    def __init__(self) -> None:
        self.tracks: Dict[str, VehicleTrackInfo] = dict()
        self.platforms: Dict[str, VehiclePlatformInfo] = dict()

        defined_zone_types: Set[ZoneType] = {
            ZoneType.TRACK, ZoneType.PLATFORM
        }
        super().__init__(defined_zone_types)

    def _get_zone_dict_by_type(self, zone_type: ZoneType) -> Dict[str, BaseVehicleSingleZoneInfo]:
        match zone_type:
            case ZoneType.TRACK:
                return self.tracks
            case ZoneType.PLATFORM:
                return self.platforms
        raise ValueError(f"Invalid zone_type: {zone_type.value}")

    def _get_zone_info_subclass_by_type(self, zone_type: ZoneType) -> Type[BaseVehicleSingleZoneInfo]:
        match zone_type:
            case ZoneType.TRACK:
                return VehicleTrackInfo
            case ZoneType.PLATFORM:
                return VehiclePlatformInfo
        raise ValueError(f"Invalid zone_type: {zone_type.value}")

class CarZonesInfo(BaseVehicleZonesInfo):

    def __init__(self) -> None:
        self.intrusion_zones: Dict[str, VehicleIntrusionZoneInfo] = dict()

        defined_zone_types: Set[ZoneType] = {ZoneType.INTRUSION_ZONE}
        super().__init__(defined_zone_types)

    def _get_zone_dict_by_type(self, zone_type: ZoneType) -> Dict[str, BaseVehicleSingleZoneInfo]:
        match zone_type:
            case ZoneType.INTRUSION_ZONE:
                return self.intrusion_zones
        raise ValueError(f"Invalid zone_type: {zone_type.value}")

    def _get_zone_info_subclass_by_type(self, zone_type: ZoneType) -> Type[BaseVehicleSingleZoneInfo]:
        match zone_type:
            case ZoneType.INTRUSION_ZONE:
                return VehicleIntrusionZoneInfo
        raise ValueError(f"Invalid zone_type: {zone_type.value}")

VehicleZonesInfo: TypeAlias = TramZonesInfo | CarZonesInfo

# --- (3) vehicle state object ---

class BaseVehicleState(ABC):

    def __init__(self, lifetime_start_ts: float,
                 *, zones_info_wrapper_type: Type[VehicleZonesInfo]) -> None:

        # vehicle_id: str

        self.lifetime_start_ts: float = lifetime_start_ts

        self.speeds: CurAndMaxSpeeds = CurAndMaxSpeeds()
        self.stationary_start_ts: float | None = None
        self.zones: VehicleZonesInfo = zones_info_wrapper_type()

    def add_zone(self, *args, zone_type: ZoneType, zone_id: str, **kwargs) -> None:
        self.zones.add_zone(*args, zone_type=zone_type, zone_id=zone_id, **kwargs)

    def remove_zone(self, *args, zone_type: ZoneType, zone_id: str, **kwargs) -> None:
        self.zones.add_zone(*args, zone_type=zone_type, zone_id=zone_id, **kwargs)

    def _update_stationary_start_ts(self,
                                    *, prev_is_stationary: bool | None,
                                    cur_is_stationary: bool | None,
                                    current_ts: float) -> None:
        if prev_is_stationary is not None and cur_is_stationary is None:
            raise RuntimeError("Inconsistent state: speeds.is_stationary was previously set "
                               "to a numerical value but is now None.")

        is_stationary_start: bool = not prev_is_stationary and cur_is_stationary
        is_stationary_end: bool = prev_is_stationary and not cur_is_stationary

        # update the start of the current stationary period
        if is_stationary_start:
            # new stationary period: record the timestamp
            self.stationary_start_ts = current_ts
        if is_stationary_end:
            # the stationary period has ended: reset the timestamp
            self.stationary_start_ts = None

    def update_speeds(self, *, current_ts: float,
                      speed_raw: float | None,
                      speed_smoothed: float | None) -> None:
        prev_is_stationary: bool | None = self.speeds.is_stationary
        self.speeds.update(raw=speed_raw, smoothed=speed_smoothed)
        cur_is_stationary: bool | None = self.speeds.is_stationary
        # update stationary timestamp
        self._update_stationary_start_ts(prev_is_stationary=prev_is_stationary,
                                         cur_is_stationary=cur_is_stationary,
                                         current_ts=current_ts)
        # update speeds in all zones for this vehicle
        self.zones.update_all_zones(current_raw_kmh=speed_raw,
                                    current_smoothed_kmh=speed_smoothed)


class TramState(BaseVehicleState):

    def __init__(self, lifetime_start_ts: float) -> None:
        super().__init__(lifetime_start_ts,
                         zones_info_wrapper_type=TramZonesInfo)

class CarState(BaseVehicleState):

    def __init__(self, lifetime_start_ts: float) -> None:
        super().__init__(lifetime_start_ts,
                         zones_info_wrapper_type=CarZonesInfo)

VehicleState: TypeAlias = TramState | CarState

def _vehicle_state_type_from_vehicle_type(vehicle_type: VehicleType) -> Type[VehicleState]:
    match vehicle_type:
        case VehicleType.TRAM:
            return TramState
        case VehicleType.CAR:
            return CarState
    raise ValueError(f"Unsupported vehicle type: {vehicle_type.value}")

# --- zone states (for all zones) ---

class OccupancyEventEndedException(Exception):
    pass

class OccupancyEventState:

    """
    An object representing the mutable state of an ongoing zone occupancy event,
    i. e. an event consisting in a particular zone being occupied by at least one vehicle.
    """

    def __init__(self, *, start_ts: float, vehicle_id: str) -> None:
        # NOTE: Meant to be called whenever an event "vehicle entered zone" is created.
        # As such, always initialised with one vehicle.

        self.start_ts: float = start_ts
        self.vehicle_ids: Set[str] = {vehicle_id}
        self.num_vehicles: int = len(self.vehicle_ids)
        self.max_vehicles: int = self.num_vehicles

        # Whether this event has ended.
        # This flag is set to `True` when `num_vehicles` reaches 0.
        # After the flag has been set to `True`, methods to change this event's state
        # should raise an `OccupancyEventEndedException`.
        self.has_ended: bool = False

    def _update_max_vehicles(self) -> None:
        self.max_vehicles = max(self.num_vehicles,
                                self.max_vehicles)

    def add_vehicle(self, vehicle_id: str) -> None:
        if self.has_ended:
            raise OccupancyEventEndedException()
        if vehicle_id in self.vehicle_ids:
            raise ValueError(f"Can't add vehicle {vehicle_id} to vehicle IDs: already present")
        self.vehicle_ids.add(vehicle_id)
        self.num_vehicles = len(self.vehicle_ids)
        self._update_max_vehicles()

    def remove_vehicle(self, vehicle_id: str) -> None:
        if self.has_ended:
            raise OccupancyEventEndedException()
        if vehicle_id not in self.vehicle_ids:
            raise ValueError(f"Can't remove vehicle {vehicle_id} from vehicle IDs: not present")
        self.vehicle_ids.remove(vehicle_id)
        self.num_vehicles = len(self.vehicle_ids)
        self._update_max_vehicles()
        if self.num_vehicles == 0:
            self.has_ended = True

class CompletedOccupancyEvent(BaseModel):

    """
    An object representing the state of a completed zone occupancy event,
    """

    start_ts: NonNegativeFloat
    end_ts: NonNegativeFloat
    max_vehicles: PositiveInt

    @model_validator(mode="after")
    def _check_timestamps(self) -> Self:
        if not self.start_ts <= self.end_ts:
            raise ValueError("start_ts must be less than or equal to end_ts")
        return self

    @classmethod
    def from_occupancy_event_state(cls,
                                   *, state: OccupancyEventState,
                                   end_ts: float) -> Self:
        return cls(start_ts=state.start_ts,
                   end_ts=end_ts,
                   max_vehicles=state.max_vehicles)

class BaseZoneState(ABC):

    """
    An object representing a zone, maintaining data about:
    (1) the current occupancy event, if any;
    (2) the previous occupancy event, if any.
    """

    def __init__(self) -> None:
        self.current_occupancy: OccupancyEventState | None = None
        self.previous_occupancy: CompletedOccupancyEvent | None = None

    # vehicle entered zone
    def add_vehicle(self, *, event_ts: float, vehicle_id: str) -> None:
        # if there is no current occupancy event: create one
        if self.current_occupancy is None:
            self.current_occupancy = OccupancyEventState(
                start_ts=event_ts, vehicle_id=vehicle_id
            )
        # if there is one, update it
        else:
            self.current_occupancy.add_vehicle(vehicle_id)

    # vehicle exited zone
    def remove_vehicle(self, *, event_ts: float, vehicle_id: str) -> None:
        # if there is no current occupancy event: raise an exception
        if self.current_occupancy is None:
            msg: str = f"Can't remove vehicle ID {vehicle_id} from the zone: the zone is currently registered is unoccupied"
            raise RuntimeError(msg)
        # remove the vehicle ID from the occupancy event
        self.current_occupancy.remove_vehicle(vehicle_id)
        # if the occupancy event has ended:
        if self.current_occupancy.has_ended:
            # update the previous occupancy event
            self.previous_occupancy = CompletedOccupancyEvent.from_occupancy_event_state(
                state=self.current_occupancy, end_ts=event_ts
            )
            # set the current occupancy event to None
            self.current_occupancy = None

class TrackState(BaseZoneState):
    # zone_type: Literal[ZoneType.TRACK] = ZoneType.TRACK
    pass

class PlatformState(BaseZoneState):
    # zone_type: Literal[ZoneType.PLATFORM] = ZoneType.PLATFORM
    pass

class IntrusionZoneState(BaseZoneState):
    # zone_type: Literal[ZoneType.INTRUSION_ZONE] = ZoneType.INTRUSION_ZONE
    pass

ZoneState: TypeAlias = TrackState | PlatformState | IntrusionZoneState

def _zone_state_type_from_zone_type(zone_type: ZoneType) -> Type[ZoneState]:
    match zone_type:
        case ZoneType.TRACK:
            return TrackState
        case ZoneType.PLATFORM:
            return PlatformState
        case ZoneType.INTRUSION_ZONE:
            return IntrusionZoneState
        case _:
            raise ValueError(f"Unsupported zone type: {zone_type}")

# --- live state updater ---

class LiveStateUpdateException(Exception):
    pass

# --- (1) configs ---

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

ZoneConfig: TypeAlias = Annotated[
    TrackConfig | PlatformConfig | IntrusionZoneConfig,
    Field(discriminator="zone_type")
]

class ZonesConfig(BaseModel):
    tracks: List[TrackConfig]
    platforms: List[PlatformConfig]
    intrusion_zones: List[IntrusionZoneConfig]

# --- (2) internal mappings for zones and vehicles ---

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseZonesMappings:
    """
    Mappings agnostic wrt zone types.
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
    Mappings whose semantics are specific to the currently specified zone types.
    """

    # platform id -> track id
    ids_platform_to_track: Dict[str, str]
    # track id -> { platform ids }
    ids_track_to_platforms: Dict[str, Set[str]]
    # zone id -> ZoneState
    states: Dict[str, ZoneState]

@dataclass(slots=True, kw_only=True)
class VehiclesMappings:
    # VehicleType -> { vehicle ids }
    type_to_ids: Dict[VehicleType, Set[str]]
    # vehicle id -> VehicleType
    id_to_type: Dict[str, VehicleType]
    # vehicle id -> VehicleState
    states: Dict[str, VehicleState]

# --- (3) master object ---

class LiveStateForCamera:

    def __init__(self, *, camera_id: str, zones_config: ZonesConfig) -> None:
        self._camera_id: str = camera_id

        # the current frame ID
        self._cur_frame_id: str | None = None
        self._cur_frame_ts: float | None = None
        self._prev_frame_ts: float | None = None

        # zone states: maintained for all zones defined in `zones_config`
        self.zones: ZonesMappings = self._init_zones(zones_config)
        # vehicle states: maintained for currently existing vehicles
        self.vehicles: VehiclesMappings = self._init_vehicles_mappings()

    # --- state validation methods ---

    def _check_frame_initialised(self) -> None:
        if self._cur_frame_id is None or self._cur_frame_ts is None:
            raise RuntimeError("frame_id and/or current_ts are None: set to current values first "
                               "before updating the instance's state")

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
            raise ValueError(f"Zone {zone_id} not registered")

    # --- event processing methods ---

    def _start_vehicle_lifetime(self,
                                *, vehicle_type: VehicleType, vehicle_id: str) -> None:
        self._check_frame_initialised()
        self._check_vehicle_not_initialised(vehicle_id)
        vehicle_state_type: Type[VehicleState] = _vehicle_state_type_from_vehicle_type(vehicle_type)

        # create a new vehicle state object for the given vehicle id
        self.vehicles.states[vehicle_id] = vehicle_state_type(self._cur_frame_ts)
        # register the mapping from the new vehicle id to the given vehicle type
        self.vehicles.id_to_type[vehicle_id] = vehicle_type
        # register the new vehicle id under the given vehicle type
        self.vehicles.type_to_ids[vehicle_type].add(vehicle_id)

    def _end_vehicle_lifetime(self, vehicle_id: str) -> None:
        self._check_frame_initialised()
        self._check_vehicle_initialised(vehicle_id)

        # unregister the vehicle state
        self.vehicles.states.pop(vehicle_id)
        # unregister vehicle type mappings
        vehicle_type: VehicleType = self.vehicles.id_to_type.pop(vehicle_id)
        self.vehicles.type_to_ids[vehicle_type].remove(vehicle_id)

    def _register_zone_entrance(self, *, vehicle_id: str, zone_id: str) -> None:
        self._check_frame_initialised()
        self._check_vehicle_initialised(vehicle_id)
        self._check_zone_registered(zone_id)

        zone_type: ZoneType = self._get_zone_type(zone_id)
        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)
        zone_state: ZoneState = self._get_zone_state(zone_id)

        # update vehicle state with zone
        vehicle_state.add_zone(zone_type=zone_type, zone_id=zone_id)
        # update zone state with vehicle
        zone_state.add_vehicle(event_ts=self._cur_frame_ts, vehicle_id=vehicle_id)

    def _register_zone_exit(self, *, vehicle_id: str, zone_id: str) -> None:
        # vehicle may or may not be initialised as per the result of `_check_vehicle_initialised()`:
        # this method should be called, in particular, after removing the vehicle from the vehicle mappings
        self._check_frame_initialised()
        self._check_zone_registered(zone_id)

        zone_type: ZoneType = self._get_zone_type(zone_id)
        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)
        zone_state: ZoneState = self._get_zone_state(zone_id)

        # update vehicle state with zone
        vehicle_state.remove_zone(zone_type=zone_type, zone_id=zone_id)
        # update zone state with vehicle
        zone_state.remove_vehicle(event_ts=self._cur_frame_ts, vehicle_id=vehicle_id)

    def _update_vehicle_speeds(self, *,
                               vehicle_id: str,
                               speed_raw: float | None, speed_smoothed: float | None):
        self._check_frame_initialised()
        self._check_vehicle_initialised(vehicle_id)

        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)

        vehicle_state.update_speeds(current_ts=self._cur_frame_ts,
                                    speed_raw=speed_raw,
                                    speed_smoothed=speed_smoothed)

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

        for zone in [*config.tracks,
                     *config.platforms,
                     *config.intrusion_zones]: # type: ZoneConfig
            zone_id: str = zone.zone_id
            zone_type: ZoneType = zone.zone_type
            type_to_ids[zone_type].add(zone_id)
            id_to_type[zone_id] = zone_type
            id_to_num_id[zone_id] = zone.zone_numerical_id
            id_to_description[zone_id] = zone.description
            zone_state_type: Type[ZoneState] = _zone_state_type_from_zone_type(zone_type)
            states[zone_id] = zone_state_type()
            if zone_type == ZoneType.PLATFORM:
                track_id: str = zone.track_zone_id
                ids_platform_to_track[zone_id] = track_id
                ids_track_to_platforms[track_id].add(zone_id)

        mappings: ZonesMappings = ZonesMappings(
            type_to_ids, id_to_type, id_to_num_id, id_to_description,
            ids_platform_to_track, ids_track_to_platforms, states
        )
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
        mappings: VehiclesMappings = VehiclesMappings(
            vehicle_type_to_ids, vehicle_id_to_type, states
        )
        return mappings

    # --- misc helpers ---

    def _get_vehicle_state(self, vehicle_id: str) -> VehicleState:
        return self.vehicles.states[vehicle_id]

    def _get_zone_state(self, zone_id: str) -> ZoneState:
        return self.zones.states[zone_id]

    def _get_zone_type(self, zone_id: str) -> ZoneType:
        return self.zones.id_to_type[zone_id]

    # --- state update methods ---

    def _update_from_lifetime_events(self, events: List[LifetimeEvent]) -> None:
        for e in events: # type: LifetimeEvent
            if e.boundary_type == EventBoundaryType.START:
                self._start_vehicle_lifetime(vehicle_type=e.vehicle_type,
                                             vehicle_id=e.vehicle_id)
            else:
                self._end_vehicle_lifetime(e.vehicle_id)

    def _update_from_zone_events(self, events: List[ZoneOccupancyEvent]) -> None:
        for e in events: # type: ZoneOccupancyEvent
            if e.boundary_type == EventBoundaryType.START:
                self._register_zone_entrance(vehicle_id=e.vehicle_id, zone_id=e.zone_id)
            else:
                self._register_zone_exit(vehicle_id=e.vehicle_id, zone_id=e.zone_id)

    def _update_from_speed_events(self, events: List[SpeedUpdateEvent]) -> None:
        for e in events:  # type: SpeedUpdateEvent
            self._update_vehicle_speeds(vehicle_id=e.vehicle_id,
                                        speed_raw=e.speeds.raw,
                                        speed_smoothed=e.speeds.smoothed)

    # --- state export methods ---

    def _export_state(self) -> LiveAnalyticsState:
        raise NotImplementedError()

    # --- master method ---

    def _check_frame_ts(self, events_container: EventsContainer) -> None:
        prev_ts: float | None = self._prev_frame_ts
        if prev_ts is not None:
            cur_ts: float = events_container.frame_ts
            if not cur_ts > prev_ts:
                raise ValueError("The current frame timestamp must be greater than the previous one "
                                 f"(previous {prev_ts}, received current: {cur_ts})")

    def update_and_export_state(self, events_container: EventsContainer) -> LiveAnalyticsState:
        self._check_frame_ts(events_container)

        self._cur_frame_id = events_container.frame_id
        self._prev_frame_ts = self._cur_frame_ts
        self._cur_frame_ts = events_container.frame_ts

        # NOTE: order DOES matter
        # (1) lifetime
        self._update_from_lifetime_events(events_container.events.lifetime)
        # (2) zone enters / exits
        self._update_from_zone_events(events_container.events.zone_occupancy)
        # (3) speeds
        self._update_from_speed_events(events_container.events.speeds)

        exported_state: LiveAnalyticsState = self._export_state()
        return exported_state