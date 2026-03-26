from abc import ABC, abstractmethod
from typing import List, override

from numpy import uint8
from numpy.typing import NDArray

from common.utils.concurrency.mp_utils import stop_shutdownable
from tram_analytics.v1.models.components.detection import RawDetection
from tram_analytics.v1.pipeline.components.detection.detection_config import DetectorConfig
from tram_analytics.v1.pipeline.components.detection.components.detector import BaseDetector, build_detector
from tram_analytics.v1.pipeline.components.detection.components.detector_process import DetectorProcess

DETECTOR_PROCESS_TIMEOUT_PER_JOIN: float = 5.0

class BaseDetectorWorker(ABC):

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

class InProcessDetectorWorker(BaseDetectorWorker):

    def __init__(self, detector_config: DetectorConfig) -> None:
        self._detector: BaseDetector = build_detector(detector_config)

    @override
    def start(self) -> None:
        self._detector.start()

    @override
    def stop(self) -> None:
        self._detector.stop()

    def detect(self, image_bgr: NDArray[uint8]) -> List[RawDetection]:
        return self._detector.detect(image_bgr)

class SeparateProcessDetectorWorker(BaseDetectorWorker):

    def __init__(self, detector_config: DetectorConfig) -> None:
        self._detector_process: DetectorProcess = DetectorProcess(detector_config)

    @override
    def start(self) -> None:
        self._detector_process.start()

    @override
    def stop(self) -> None:
        # put a sentinel
        self._detector_process.in_queue.put(None)
        # then stop (either the sentinel will trigger the stop anyway, or the exit event that is set by the below)
        stop_shutdownable(self._detector_process,
                          timeout_per_join=DETECTOR_PROCESS_TIMEOUT_PER_JOIN)

    def send(self, image_bgr: NDArray[uint8]) -> None:
        if not self._detector_process.is_alive():
            raise RuntimeError("Called send but the worker process is not alive")
        # put the inputs into the input queue
        self._detector_process.in_queue.put(image_bgr)

    def recv(self) -> List[RawDetection]:
        if not self._detector_process.is_alive():
            raise RuntimeError("Called recv but the worker process is not alive")
        # wait until the outputs are produced into the output queue
        outputs: List[RawDetection] | None = self._detector_process.out_queue.get()
        if outputs is None:
            # receiving a sentinel here means the process has exited whilst processing the inputs
            # (rather than performing a requested stop);
            # this should be treated as an exception
            raise RuntimeError("Exception in detector process (got a sentinel from the detector process "
                               "whilst waiting for processed outputs)")
        return outputs
