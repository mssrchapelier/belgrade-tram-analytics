from typing import List

from archive.v1.src.models.models import MainPipelineArtefacts
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, \
    VehicleInput, SpeedsInput


def convert_vehicle_info(src_obj: MainPipelineArtefacts) -> EventsInputData:
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
        for src_vehicle in src_obj.vehicles_info
    ]
    dest_obj: EventsInputData = EventsInputData(
        camera_id=src_obj.frame_metadata.camera_id,
        frame_id=src_obj.frame_metadata.frame_id,
        frame_ts=src_obj.frame_metadata.timestamp,
        vehicles=dest_vehicles
    )
    return dest_obj
