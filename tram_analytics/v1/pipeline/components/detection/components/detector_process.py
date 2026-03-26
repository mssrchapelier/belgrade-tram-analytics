import logging
from logging import Logger
from multiprocessing import Queue as QueueType, Queue
from typing import List, override

from numpy import uint8
from numpy._typing import NDArray

from common.settings.constants import LOGGING_SERVER_HOST, LOGGING_SERVER_PORT
from common.utils.concurrency.mp_utils import ShutdownableProcess
from common.utils.logging_utils.logging_utils import (
    get_logger_name_for_object, configure_global_logging_to_tcp_socket
)
from tram_analytics.v1.models.components.detection import RawDetection
from tram_analytics.v1.pipeline.components.detection.detection_config import DetectorConfig


class DetectorProcess(ShutdownableProcess):

    def __init__(self, detector_config: DetectorConfig) -> None:
        self._detector_config: DetectorConfig = detector_config
        super().__init__()

        # inherit the logging level from the parent process
        self._logging_level: int = logging.getLogger().level

        # input queue
        self.in_queue: QueueType[NDArray[uint8] | None] = Queue(maxsize=1)
        # output queue
        self.out_queue: QueueType[List[RawDetection] | None] = Queue(maxsize=1)

    def _run_worker(self) -> None:
        from tram_analytics.v1.pipeline.components.detection.components.detector import BaseDetector, build_detector

        logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        # in a multiprocessing context, must initialise the model here rather than in constructor
        detector: BaseDetector = build_detector(self._detector_config)

        detector.start()
        try:
            while not self.is_exit_signal():
                # wait for the next inputs
                next_inputs: NDArray[uint8] | None = self.in_queue.get()
                if next_inputs is None:
                    # got a sentinel --> stop processing
                    break
                logger.debug("Detector process starting inference")
                outputs: List[RawDetection] = detector.detect(next_inputs)
                logger.debug("Detector process ended inference")
                # push the outputs
                self.out_queue.put(outputs)
        finally:
            detector.stop()
            # put a sentinel in the output queue to signal the end of processing
            self.out_queue.put(None)

    @override
    def run(self) -> None:
        configure_global_logging_to_tcp_socket(
            host=LOGGING_SERVER_HOST, port=LOGGING_SERVER_PORT,
            level=self._logging_level
        )
        self._run_worker()
