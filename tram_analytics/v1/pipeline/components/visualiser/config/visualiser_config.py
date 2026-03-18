__version__ = "0.2.0"

from typing import Tuple, TypeAlias, List

import cv2
from pydantic import BaseModel, RootModel, Field, NonNegativeInt, PositiveInt, PositiveFloat

from common.utils.custom_types import PixelPosition, ColorTuple
from common.utils.img.cv2.pretty_put_text import Corner
from common.utils.pydantic.types_pydantic import OddPositiveInt
from tram_analytics.v1.models.common_types import SpeedDisplayUnit


class ColorlessTextConfig(BaseModel):
    font_face: int = cv2.FONT_HERSHEY_PLAIN
    font_scale: PositiveFloat
    thickness: PositiveInt

class ColorlessTextboxConfig(ColorlessTextConfig):
    # anchor, which_corner unspecified
    offset: Tuple[NonNegativeInt, NonNegativeInt]
    padding: Tuple[NonNegativeInt, NonNegativeInt]


class FrameOverlayTextboxConfig(ColorlessTextboxConfig):
    anchor: PixelPosition
    which_corner: Corner
    bg_color: ColorTuple
    font_color: ColorTuple

class FrameOverlayConfig(BaseModel):
    frame_id_display_length: int = Field(gt=0)
    timestamp_format: str = "%d-%m-%Y %H:%M:%S .%f"
    textbox: FrameOverlayTextboxConfig

class TextConfig(BaseModel):
    display_length: int = Field(gt=0)
    textbox: ColorlessTextboxConfig

# TODO: change all imports of these to TextConfig
ClassIDConfig: TypeAlias = TextConfig
TrackIDConfig: TypeAlias = TextConfig
SpeedRenderConfig: TypeAlias = TextConfig

class SpeedConfig(BaseModel):
    unit: SpeedDisplayUnit
    render: SpeedRenderConfig

class TrackStateAnnotationsConfig(BaseModel):
    class_id: ClassIDConfig
    track_id: TrackIDConfig

class DashedLineConfig(BaseModel):
    dash_length: PositiveInt
    gap_length: PositiveInt

class TrackStateLineAppearanceConfig(BaseModel):
    thickness: PositiveInt
    unconfirmed_matched: DashedLineConfig
    unconfirmed_unmatched: DashedLineConfig
    confirmed_unmatched: DashedLineConfig

class TrackStateConfig(BaseModel):
    bbox_text: TrackStateAnnotationsConfig
    bbox_border: TrackStateLineAppearanceConfig

class TrackConfig(BaseModel):
    marker_size: OddPositiveInt
    line_thickness: int

class SingleROIVisualisationConfig(DashedLineConfig):
    detector_id: str
    color: ColorTuple
    thickness: PositiveInt

class ROIVisualisationConfig(RootModel[List[SingleROIVisualisationConfig]]):
    pass

class VisualiserConfig(BaseModel):
    out_height: PositiveInt | None = None
    # whether to convert the canvas to greyscale
    to_greyscale: bool

    frame_overlay: FrameOverlayConfig
    track: TrackConfig
    track_state: TrackStateConfig
    speed: SpeedConfig

    roi: ROIVisualisationConfig | None = None