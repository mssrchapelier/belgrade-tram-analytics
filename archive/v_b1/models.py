from typing import List, Self
from datetime import datetime

from pydantic import BaseModel, Field
import numpy as np
from numpy.typing import NDArray

class DetectionClass(BaseModel):
    class_id: int
    label: str

class Frame(BaseModel):
    frame_id: str
    camera_id: str
    image: bytes
    timestamp: datetime

class Detection(BaseModel):
    detection_id: str
    frame_id: str
    class_id: int
    track_id: int
    confidence: float = Field(ge=0.0, le=1.0)
    # x1, x2, y1, y2: absolute (pixels)
    x1: int = Field(ge=1)
    x2: int = Field(ge=1)
    y1: int = Field(ge=1)
    y2: int = Field(ge=1)
