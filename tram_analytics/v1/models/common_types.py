from enum import StrEnum, auto
from typing import Self, List

from pydantic import BaseModel, model_validator

from common.utils.custom_types import PlanarPosition


class ZoneType(StrEnum):
    TRACK = auto()
    PLATFORM = auto()
    INTRUSION_ZONE = auto()

class VehicleType(StrEnum):
    TRAM = auto()
    CAR = auto()

class CoordType(StrEnum):
    """
    The type of coordinates to which the given coordinates belong:
    - `IMAGE`: pixel coordinates on the source image
    - `WORLD`: coordinates in the real world (as defined by a homography conversion or otherwise)
    """
    IMAGE = auto()
    WORLD = auto()

class MotionStatus(StrEnum):
    """
    The vehicle's status wrt whether it is considered to be moving or not,
    determined by applying a threshold on one of the calculated speed values
    or in some other way.
    """
    UNDEFINED = auto()
    STATIONARY = auto()
    MOVING = auto()

class SpeedDisplayUnit(StrEnum):
    """
    To be used in configs to set the speed unit for display
    (internally, metres per second are always used).
    """
    METRES_PER_SECOND = auto()
    KILOMETRES_PER_HOUR = auto()

def get_speed_unit_str(unit: SpeedDisplayUnit) -> str:
    match unit:
        case SpeedDisplayUnit.KILOMETRES_PER_HOUR:
            return "km/h"
        case SpeedDisplayUnit.METRES_PER_SECOND:
            return "m/s"
        case _:
            raise ValueError(f"Unknown speed unit: {unit}")

def convert_speed(speed_ms: float | None, unit: SpeedDisplayUnit) -> float | None:
    if speed_ms is None:
        return None
    match unit:
        case SpeedDisplayUnit.METRES_PER_SECOND:
            return speed_ms
        case SpeedDisplayUnit.KILOMETRES_PER_HOUR:
            return speed_ms * 3.6
        case _:
            raise ValueError(f"Unknown speed display unit: {unit}")


class BoundingBox(BaseModel):
    x1: float
    x2: float
    y1: float
    y2: float

    @model_validator(mode="after")
    def _validate_coords(self) -> Self:
        if self.x1 >= self.x2:
            raise ValueError("x1 must be strictly less than x2")
        if self.y1 >= self.y2:
            raise ValueError("y1 must be strictly less than y2")
        return self

    def to_xyxy_list(self) -> List[float]:
        return [self.x1, self.y1, self.x2, self.y2]

    @property
    def width(self) -> float:
        return self.x2 - self.x1

    @property
    def height(self) -> float:
        return self.y2 - self.y1

    @property
    def centroid(self) -> PlanarPosition:
        center_x: float = self.x1 + self.width / 2
        center_y: float = self.y1 + self.height / 2
        return center_x, center_y


class SpeedType(StrEnum):
    RAW = auto()
    SMOOTHED = auto()
