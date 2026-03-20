from typing import Dict

import cv2
from pydantic import BaseModel

from common.utils.custom_types import ColorTuple
from tram_analytics.v1.models.common_types import VehicleType

PROXY_POINT_COLOR: ColorTuple = (255, 255, 0)
PROXY_POINT_SIZE: int = 15
PROXY_POINT_THICKNESS: int = 1
LINE_TYPE: int = cv2.LINE_8


class BboxColours(BaseModel):
    border_color: ColorTuple

    # text for: class_id
    classid_bg_color: ColorTuple
    classid_text_color: ColorTuple


CLASS_COLOURS: Dict[VehicleType, BboxColours] = {
    VehicleType.TRAM: BboxColours(
        border_color=(64, 0, 191), # BF0040 (dark pink)
        classid_bg_color=(255, 255, 255), # white
        classid_text_color=(64, 0, 191) # BF0040 (dark pink)
    ),
    VehicleType.CAR: BboxColours(
        border_color=(255, 152, 17), # 0FA1FF (blue nebula)
        classid_bg_color=(255, 255, 255), # white
        classid_text_color=(255, 152, 17) # 0FA1FF (blue nebula)
    )
}
CORRIDOR_DASH_LENGTH: int = 10
CORRIDOR_GAP_LENGTH: int = 15
CORRIDOR_COLOUR: ColorTuple = (121, 253, 255)
CORRIDOR_THICKNESS: int = 1
