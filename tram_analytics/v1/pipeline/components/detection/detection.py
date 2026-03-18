import logging
from abc import ABC, abstractmethod
from logging import Logger
from time import perf_counter
from typing import List, Self, override, TypeAlias

from numpy import uint8
from numpy.typing import NDArray
from pydantic_yaml import parse_yaml_file_as

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from common.utils.random.id_gen import get_uuid
from tram_analytics.v1.models.components.detection import RawDetection, Detection
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.pipeline.components.detection.detection_config import (
    DetectionServiceConfig
)
from tram_analytics.v1.pipeline.components.detection.detection_config import DetectorConfig, \
    DetectionServiceDeploymentOption
from tram_analytics.v1.pipeline.components.detection.detector_worker import (
    BaseDetectorWorker, InProcessDetectorWorker, SeparateProcessDetectorWorker
)


class BaseDetectionService[DetectorWorkerT: BaseDetectorWorker](ABC):

    def __init__(self, config: DetectionServiceConfig):
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._workers: List[DetectorWorkerT] = [
            self._create_detector_worker(det_config)
            for det_config in config.detectors
        ]

    @abstractmethod
    def _create_detector_worker(self, detector_config: DetectorConfig) -> DetectorWorkerT:
        pass

    @classmethod
    def from_config(cls, config: DetectionServiceConfig) -> Self:
        return cls(config)

    @classmethod
    def from_yaml(cls, config_path: str) -> Self:
        config: DetectionServiceConfig = parse_yaml_file_as(
            DetectionServiceConfig, config_path
        )
        return cls(config)

    def __enter__(self) -> Self:
        for worker in self._workers:  # type: DetectorWorkerT
            worker.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        for worker in self._workers:  # type: DetectorWorkerT
            worker.stop()

    @abstractmethod
    def _get_all_raw_detections(self, img: NDArray[uint8]) -> List[RawDetection]:
        pass

    @staticmethod
    def _process_raw_detections(
            raw_dets: List[RawDetection], *, frame_id: str
    ) -> List[Detection]:
        dets: List[Detection] = []
        for raw_det in raw_dets:  # type: RawDetection
            det_id: str = get_uuid()
            det: Detection = Detection(frame_id=frame_id,
                                       detection_id=det_id,
                                       raw_detection=raw_det)
            dets.append(det)
        return dets

    def detect(self, frame: Frame) -> List[Detection]:
        start_ts: float = perf_counter()
        raw_dets: List[RawDetection] = self._get_all_raw_detections(frame.image)
        received_raw_ts: float = perf_counter()
        dets: List[Detection] = self._process_raw_detections(
            raw_dets, frame_id=frame.frame_id
        )
        end_ts: float = perf_counter()
        self._logger.debug(
            f"Frame {frame.frame_id}"
            f" | raw processing: {received_raw_ts - start_ts:.4f} s"
            f" | postprocessing: {end_ts - received_raw_ts:.4f} s"
            f" | total: {end_ts - start_ts:.4f} s"
        )
        return dets

class SingleProcessDetectionService(BaseDetectionService[InProcessDetectorWorker]):

    @override
    def _create_detector_worker(self, detector_config: DetectorConfig) -> InProcessDetectorWorker:
        return InProcessDetectorWorker(detector_config)

    @override
    def _get_all_raw_detections(self, image: NDArray[uint8]) -> List[RawDetection]:
        raw_dets: List[RawDetection] = []
        for worker in self._workers: # type: InProcessDetectorWorker
            detector_dets: List[RawDetection] = worker.detect(image)
            raw_dets.extend(detector_dets)
        return raw_dets

class SeparateWorkerProcessesDetectionService(BaseDetectionService[SeparateProcessDetectorWorker]):

    @override
    def _create_detector_worker(self, detector_config: DetectorConfig) -> SeparateProcessDetectorWorker:
        return SeparateProcessDetectorWorker(detector_config)

    def _send_to_workers(self, img: NDArray[uint8]) -> None:
        for worker in self._workers:  # type: SeparateProcessDetectorWorker
            # send the frame for inference
            worker.send(img)

    def _recv_from_workers(self) -> List[RawDetection]:
        # NOTE: collapsing all outputs into a single list;
        # rewrite if per-worker logic is needed
        dets: List[RawDetection] = []
        for worker in self._workers:  # type: SeparateProcessDetectorWorker
            worker_dets: List[RawDetection] = worker.recv()
            dets.extend(worker_dets)
        return dets

    @override
    def _get_all_raw_detections(self, img: NDArray[uint8]) -> List[RawDetection]:
        # send to all workers for inference
        self._send_to_workers(img)
        # wait for all results to become available
        raw_dets: List[RawDetection] = self._recv_from_workers()
        return raw_dets

DetectionService: TypeAlias = SingleProcessDetectionService | SeparateWorkerProcessesDetectionService

def build_detection_service(config: DetectionServiceConfig) -> DetectionService:
    deployment_option: DetectionServiceDeploymentOption = config.deployment.option
    match deployment_option:
        case DetectionServiceDeploymentOption.SINGLE_PROCESS:
            return SingleProcessDetectionService(config)
        case DetectionServiceDeploymentOption.SEPARATE_WORKER_PROCESSES:
            return SeparateWorkerProcessesDetectionService(config)
        case _:
            raise ValueError(f"Unknown deployment option: {deployment_option}")

