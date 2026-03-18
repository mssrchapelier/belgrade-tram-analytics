from typing import NamedTuple

from tram_analytics.v1.models.common_types import VehicleType


class VehicleZoneMapping(NamedTuple):
    vehicle_id: str
    zone_id: str

class VehicleIdTypeMapping(NamedTuple):
    vehicle_id: str
    vehicle_type: VehicleType