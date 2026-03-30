from enum import StrEnum
from typing import List, Literal, Annotated, TypeAlias, Dict, Any

import cv2
from pydantic import BaseModel, RootModel, NonNegativeFloat, PositiveInt, Field

from common.utils.custom_types import PlanarPosition
from common.utils.pydantic.types_pydantic import OpenUnitIntervalValue


class PointConfigImageCoords(BaseModel):
    x: NonNegativeFloat
    y: NonNegativeFloat

class PointConfigUTMZone(BaseModel):
    number: int
    letter: str

class PointConfigWorldCoords(BaseModel):
    northing: float
    easting: float
    zone: PointConfigUTMZone

class PointConfigItem(BaseModel):
    image: PointConfigImageCoords
    world: PointConfigWorldCoords

class DefiningPointsConfig(RootModel[List[PointConfigItem]]):
    pass

def image_point_from_config(pt_config: PointConfigImageCoords) -> PlanarPosition:
    return pt_config.x, pt_config.y

def world_point_from_config(pt_config: PointConfigWorldCoords) -> PlanarPosition:
    return pt_config.easting, pt_config.northing


class HomographyEstimationMethod(StrEnum):
    DEFAULT = "default"
    RANSAC = "ransac"
    LMEDS = "lmeds"
    RHO = "rho"

class BaseRansacRhoParamsConfig(BaseModel):
    # OpenCV `ransacReprojThreshold` argument value for RANSAC and RHO.
    # See more: https://docs.opencv.org/4.x/d9/d0c/group__calib3d.html#ga4abc2ece9fab9398f2e560d53c8c9780
    reproj_threshold: float = 3.0

class RansacParamsConfig(BaseRansacRhoParamsConfig):
    max_iters: PositiveInt = 2000

class RhoParamsConfig(BaseRansacRhoParamsConfig):
    # TODO: add max reprojection error for RANSAC and RHO (see https://docs.opencv.org/4.x/d9/d0c/group__calib3d.html#ga4abc2ece9fab9398f2e560d53c8c9780)
    pass

class BaseHomographyMethodConfig(BaseModel):
    confidence: OpenUnitIntervalValue = 0.995

class DefaultHomographyMethodConfig(BaseHomographyMethodConfig):
    method_name: Literal[HomographyEstimationMethod.DEFAULT] = HomographyEstimationMethod.DEFAULT

class RansacHomographyMethodConfig(BaseHomographyMethodConfig):
    method_name: Literal[HomographyEstimationMethod.RANSAC] = HomographyEstimationMethod.RANSAC
    params: RansacParamsConfig = RansacParamsConfig()

class LmedsHomographyMethodConfig(BaseHomographyMethodConfig):
    method_name: Literal[HomographyEstimationMethod.LMEDS] = HomographyEstimationMethod.LMEDS

class RhoHomographyMethodConfig(BaseHomographyMethodConfig):
    method_name: Literal[HomographyEstimationMethod.RHO] = HomographyEstimationMethod.RHO
    params: RhoParamsConfig = RhoParamsConfig()

HomographyMethodConfig: TypeAlias = Annotated[
    DefaultHomographyMethodConfig | RansacHomographyMethodConfig | LmedsHomographyMethodConfig | RhoHomographyMethodConfig,
    Field(discriminator="method_name")
]

class HomographyConfig(BaseModel):
    method: HomographyMethodConfig
    defining_points: DefiningPointsConfig

def _get_cv2_homography_method(method: HomographyEstimationMethod) -> int:
    match method:
        case HomographyEstimationMethod.DEFAULT:
            return 0
        case HomographyEstimationMethod.RANSAC:
            return cv2.RANSAC
        case HomographyEstimationMethod.LMEDS:
            return cv2.LMEDS
        case HomographyEstimationMethod.RHO:
            return cv2.RHO
        case _:
            raise ValueError(f"Unsupported method: {method}")

def get_cv2_kwargs_for_homography(method_config: HomographyMethodConfig):
    func_kwargs: Dict[str, Any] = dict()
    func_kwargs["confidence"] = method_config.confidence
    match method_config.method_name:
        case HomographyEstimationMethod.DEFAULT:
            func_kwargs["method"] = 0
        case HomographyEstimationMethod.RANSAC:
            func_kwargs["method"] = cv2.RANSAC
            func_kwargs["maxIters"] = method_config.params.max_iters
            func_kwargs["ransacReprojThreshold"] = method_config.params.reproj_threshold
        case HomographyEstimationMethod.LMEDS:
            func_kwargs["method"] = cv2.LMEDS
        case HomographyEstimationMethod.RHO:
            func_kwargs["method"] = cv2.RHO
            func_kwargs["ransacReprojThreshold"] = method_config.params.reproj_threshold
        case _:
            raise ValueError(f"Unsupported homography estimation method: {method_config.method_name}")
    return func_kwargs