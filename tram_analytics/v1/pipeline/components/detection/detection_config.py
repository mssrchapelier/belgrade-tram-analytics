from enum import StrEnum, auto
from typing import List, Tuple, Dict, Any, Annotated, TypeAlias, Literal

from pydantic import BaseModel, Field, field_validator, HttpUrl
from shapely import Polygon

from tram_analytics.v1.models.common_types import VehicleType

class ROIFilteringPolicy(StrEnum):
    CENTROID = auto()
    AREA_FRACTION = auto()

class BaseROIConfig(BaseModel):
    policy: ROIFilteringPolicy
    # roi_coords: [ [x_1, y_1], ..., [x_n, y_n] ] for each of the n vertices defining the ROI
    coords: List[Tuple[float, float]]

    @field_validator("coords")
    @classmethod
    def _validate_coords(cls, coords: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        polygon: Polygon = Polygon(coords)
        if polygon.area == 0.0:
            raise ValueError("Invalid coords: ROI must be a valid polygon with non-zero area")
        return coords

class ROIConfigCentroid(BaseROIConfig):
    policy: Literal[ROIFilteringPolicy.CENTROID] = ROIFilteringPolicy.CENTROID

class ROIConfigMinAreaFraction(BaseROIConfig):
    policy: Literal[ROIFilteringPolicy.AREA_FRACTION] = ROIFilteringPolicy.AREA_FRACTION
    min_area_fraction: Annotated[float, Field(ge=0.0, le=1.0)]

ROIConfig: TypeAlias = Annotated[
    ROIConfigCentroid | ROIConfigMinAreaFraction,
    Field(discriminator="policy")
]

class DetectorType(StrEnum):
    YOLO = auto()
    REMOTE_STUB = auto()

class BaseDetectorWorkerConfig(BaseModel):
    detector_type: DetectorType
    # roi: ROIConfig | None = None  # None for no ROI filtering
    # NOTE: A ROI must be present in the config.
    # TODO: implement nullable ROI configs (detection anywhere in the image in that case)
    roi: ROIConfig
    detector_id: str

    # detected class ID -> vehicle type
    classes: Dict[int, VehicleType]

class DetectorConfigYOLO(BaseDetectorWorkerConfig):
    detector_type: Literal[DetectorType.YOLO] = DetectorType.YOLO

    weights_path: str

    # TODO: Implement proper Pydantic configs with arguments for model initialisation and prediction runs
    # model_init_kwargs
    init_kwargs: Dict[str, Any] = dict()
    # model_run_kwargs
    # TODO: send keys of self.classes as a list as the `classes` argument for the call
    run_kwargs: Dict[str, Any] = dict()

class DetectorConfigRemoteStub(BaseDetectorWorkerConfig):
    detector_type: Literal[DetectorType.REMOTE_STUB] = DetectorType.REMOTE_STUB
    inference_endpoint: HttpUrl

DetectorConfig: TypeAlias = Annotated[
    DetectorConfigYOLO | DetectorConfigRemoteStub,
    Field(discriminator="detector_type")
]

class DetectionServiceDeploymentOption(StrEnum):
    SINGLE_PROCESS = auto()
    SEPARATE_WORKER_PROCESSES = auto()

class BaseDetectionServiceDeploymentConfig(BaseModel):
    pass

class SingleProcessDetectionServiceDeploymentConfig(BaseDetectionServiceDeploymentConfig):
    option: Literal[DetectionServiceDeploymentOption.SINGLE_PROCESS] = (
        DetectionServiceDeploymentOption.SINGLE_PROCESS
    )

class SeparateWorkerProcessesDetectionServiceDeploymentConfig(BaseDetectionServiceDeploymentConfig):
    option: Literal[DetectionServiceDeploymentOption.SEPARATE_WORKER_PROCESSES] = (
        DetectionServiceDeploymentOption.SEPARATE_WORKER_PROCESSES
    )

DetectionServiceDeploymentConfig: TypeAlias = Annotated[
    SingleProcessDetectionServiceDeploymentConfig | SeparateWorkerProcessesDetectionServiceDeploymentConfig,
    Field(discriminator="option")
]

class DetectionServiceConfig(BaseModel):
    deployment: DetectionServiceDeploymentConfig
    detectors: List[DetectorConfig]

#
# DetectorDeploymentConfig: TypeAlias = Annotated[
#     InProcessDetectorDeploymentConfig | SeparateProcessDetectorDeploymentConfig,
#     Field(discriminator="option")
# ]
#
# class DetectorWorkerConfig(BaseModel):
#     deployment: DetectorDeploymentConfig
#     # detector: DetectorConfig
#
# class BaseDetectionServiceConfig[
#     DeploymentOptionT: DetectorDeploymentOption
# ](BaseModel):
#     deployment: DetectorDeploymentConfig
#     detectors: List[DetectorWorkerConfig]

