from typing import List

from tram_analytics.v1.models.components.frame_ingestion import FrameMetadata
from tram_analytics.v1.models.components.vehicle_info import VehicleInfo
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import (
    EventsInputData, VehicleInput, SpeedsInput
)


def convert_vehicle_info(
    frame_metadata: FrameMetadata, vehicle_infos: List[VehicleInfo]
) -> EventsInputData:
    """
    For a given frame, extract data from the output of the main pipeline
    into an object used as input to the events computer.
    """
    dest_vehicles: List[VehicleInput] = [
        VehicleInput(vehicle_type=src_vehicle.vehicle_type,
                     vehicle_id=src_vehicle.vehicle_id,
                     is_matched=src_vehicle.is_matched,
                     speeds=SpeedsInput(raw_ms=src_vehicle.speeds.raw,
                                        smoothed_ms=src_vehicle.speeds.smoothed),
                     zone_ids=src_vehicle.zone_ids)
        for src_vehicle in vehicle_infos
    ]
    dest_obj: EventsInputData = EventsInputData(
        camera_id=frame_metadata.camera_id,
        frame_id=frame_metadata.frame_id,
        frame_ts=frame_metadata.timestamp,
        vehicles=dest_vehicles
    )
    return dest_obj
