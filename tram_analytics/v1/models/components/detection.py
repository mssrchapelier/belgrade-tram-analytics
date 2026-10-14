from pydantic import BaseModel, Field

from common.utils.pydantic.types_pydantic import OpenUnitIntervalValue
from tram_analytics.v1.models.common_types import BoundingBox, VehicleType

class RawDetection(BaseModel):
    """
    An object representing a single detection result produced by the object detection model.
    
    Note that the pipeline includes a tracking module downstream,
    which may assign a different interpolated position for the object (the tracker box), 
    and normally, the tracker box rather than the raw detection box 
    will be used for any further analytics downstream.
    """

    # Preserving the raw class ID assigned by the model, for reference.
    # Should not be used downstream.
    class_id: int = Field(
        description=(
            "The raw numeric class label assigned by the object detection model. "
            "Do not rely on this for determining the vehicle class -- "
            "use the value of `vehicle_type` instead."
        )
    )
    vehicle_type: VehicleType = Field(
        description="The vehicle class associated with this detection."
    )
    confidence: OpenUnitIntervalValue = Field(
        description="The confidence score assigned by the model to this detection."
    )
    # x1, x2, y1, y2: absolute values (pixels)
    bbox: BoundingBox = Field(
        description=(
            "The coordinates of the bounding box for the detection."
        )
    )

class Detection(BaseModel):
    """
    An object wrapping the raw detection result, 
    associating an assigned unique ID and the frame's ID with the detection.
    """
    
    detection_id: str = Field(
        description="The UUID assigned to this detection result."
    )
    frame_id: str = Field(
        description="The ID of the frame with which the detection result is associated."
    )
    raw_detection: RawDetection = Field(
        description="The object describing the raw detection as produced by the model."
    )
