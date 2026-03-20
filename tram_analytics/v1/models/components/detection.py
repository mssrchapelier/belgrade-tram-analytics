from pydantic import BaseModel

from common.utils.pydantic.types_pydantic import OpenUnitIntervalValue
from tram_analytics.v1.models.common_types import BoundingBox, VehicleType

class RawDetection(BaseModel):
    # Preserving the raw class ID assigned by the model, for reference.
    # Should not be used downstream.
    class_id: int
    vehicle_type: VehicleType
    confidence: OpenUnitIntervalValue
    # x1, x2, y1, y2: absolute values (pixels)
    bbox: BoundingBox


class Detection(BaseModel):
    detection_id: str
    frame_id: str
    raw_detection: RawDetection
