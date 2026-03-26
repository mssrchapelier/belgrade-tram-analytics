from datetime import datetime
from itertools import chain
from typing import (
    List, Tuple
)

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.models.components.tracking import TrackState
from tram_analytics.v1.models.components.vehicle_info import (
    VehicleInfo, TramInfo, CarInfo
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.coord_conversion.homography import CoordConverter
from tram_analytics.v1.pipeline.components.vehicle_info.components.coord_conversion.homography_config import HomographyConfig
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.config import SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.components.zones.zones_config import (
    ZonesConfig
)
from tram_analytics.v1.pipeline.components.vehicle_info.updater.car import CarZoneAndSpeedAssigner
from tram_analytics.v1.pipeline.components.vehicle_info.updater.tram import TramZoneAndSpeedAssigner


class ZoneAndSpeedAssigner:

    def __init__(self,
                 *, zones_config: ZonesConfig,
                 homography_config: HomographyConfig | None,
                 speed_config: SpeedCalculatorConfig):
        self._coord_converter: CoordConverter | None = (
            CoordConverter(homography_config) if homography_config is not None
            else None
        )

        self._car_processor: CarZoneAndSpeedAssigner = CarZoneAndSpeedAssigner(
            zones_config=zones_config.intrusion_zones,
            coord_converter=self._coord_converter,
            speed_config=speed_config
        )
        self._tram_processor: TramZoneAndSpeedAssigner = TramZoneAndSpeedAssigner(
            rail_track_config=zones_config.tracks,
            platform_config=zones_config.platforms,
            coord_converter=self._coord_converter,
            speed_config=speed_config
        )

    def _select_assigner(self, vehicle_type: VehicleType) -> CarZoneAndSpeedAssigner | TramZoneAndSpeedAssigner:
        match vehicle_type:
            case VehicleType.CAR:
                return self._car_processor
            case VehicleType.TRAM:
                return self._tram_processor
            case _:
                raise ValueError(f"No zone assigner defined for vehicle type: {vehicle_type}")

    def process_for_frame(
            self, *, states: List[TrackState], frame_ts: datetime
    ) -> List[VehicleInfo]:
        # TODO: run in two threads for the two assigners?
        #
        # Note re potential offloading to two threads:
        # 1) This may not introduce any noticeable speedup since this is all CPU-bound work,
        #   with only small-size NumPy arrays created and worked upon,
        #   so not much is going to be saved by the C code releasing the GIL).
        # 2) Running these in two subprocesses can introduce a greater overhead and limited gains.
        #
        # Leaving as is for now.

        # NOTE: Implemented ensuring that the order of the output objects corresponds to the order of the input states.
        # TODO: Refactor (the below is so unwieldy only due to the reordering)

        # split states by vehicle type and store the original indices
        car_states: List[TrackState] = []
        car_indices: List[int] = []
        tram_states: List[TrackState] = []
        tram_indices: List[int] = []
        for original_idx, state in enumerate(states): # type: int, TrackState
            vehicle_type: VehicleType = state.vehicle_type
            if vehicle_type not in {VehicleType.CAR, VehicleType.TRAM}:
                raise ValueError(f"Unsupported vehicle type: {vehicle_type}")
            append_state_to: List[TrackState] = (
                car_states if vehicle_type is VehicleType.CAR else tram_states
            )
            append_idx_to: List[int] = (
                car_indices if vehicle_type is VehicleType.CAR else tram_indices
            )
            append_state_to.append(state)
            append_idx_to.append(original_idx)
        car_infos: List[VehicleInfo] = []
        tram_infos: List[VehicleInfo] = []
        for vehicle_type, states_for_type in zip([VehicleType.CAR, VehicleType.TRAM],
                                                 [car_states, tram_states]): # type: VehicleType, List[TrackState]
            assigner: CarZoneAndSpeedAssigner | TramZoneAndSpeedAssigner = self._select_assigner(vehicle_type)
            infos_for_type: List[CarInfo] | List[TramInfo] = assigner.process_for_frame(
                states=states_for_type, frame_ts=frame_ts
            )
            extend_which: List[VehicleInfo] = (
                car_infos if vehicle_type is VehicleType.CAR else tram_infos
            )
            extend_which.extend(infos_for_type)
        with_orig_idx: List[Tuple[int, VehicleInfo]] = [
            (idx, info_obj)
            for idx, info_obj in zip(chain(car_indices, tram_indices),
                                     chain(car_infos, tram_infos))
        ]
        # order by the original index
        with_orig_idx.sort(key=lambda item: item[0])
        infos_sorted: List[VehicleInfo] = [info_obj for idx, info_obj in with_orig_idx]
        return infos_sorted
