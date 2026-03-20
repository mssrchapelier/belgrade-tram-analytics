from typing import Dict

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.pipeline.components.tracking.tracking import SingleClassSortParams

TRACKER_PARAMS: Dict[VehicleType, SingleClassSortParams] = {
    VehicleType.TRAM: SingleClassSortParams(max_age=3, min_hits=2),
    VehicleType.CAR: SingleClassSortParams(max_age=3, min_hits=2)
}
