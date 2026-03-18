from typing import Dict, List

from pydantic import BaseModel, Field

class DetectionRaw(BaseModel):
    class_id: str
    confidence_score: float | None = Field(gt=0.0, le=1.0)
    # coordinates (absolute, pixels)
    x1: int
    x2: int
    y1: int
    y2: int

class SingleDetector:

    def __init__(self):
        # ... initialise the detection model, etc. ...
        pass

class DetectionService:

    def __init__(self):
        # ... initialise detection models from configs, etc. ...
        # configs contain detector IDs

        # { detector_id: SingleDetector }
        self._detectors: Dict[str, SingleDetector] = dict()

    async def get_detectors(self) -> List[str]:
        """
        Return IDs of available detectors.
        """
        return sorted(list(self._detectors.keys()))

    async def detect_objects(self, image: bytes, detector_id: str) -> List[DetectionRaw]:
        raise NotImplementedError()