from typing import Dict

from tram_analytics.v1.models.common_types import VehicleType

# Maps YOLO-detected classes to vehicle type.
# TODO: refactor
VEHICLE_TYPE_BY_CLASS_ID: Dict[int, VehicleType] = {
    0: VehicleType.TRAM,
    2: VehicleType.CAR
}

# VEHICLE_TYPE_BY_CLASS_ID: Dict[int, VehicleType] = {
#     0: VehicleType.TRAM,
#     1: VehicleType.CAR
# }

def get_vehicle_type(class_id: int) -> VehicleType:
    if class_id not in VEHICLE_TYPE_BY_CLASS_ID:
        raise ValueError(f"Unknown class ID: {class_id}")
    return VEHICLE_TYPE_BY_CLASS_ID[class_id]
