from typing import List, Dict

from pydantic import BaseModel

class FeatureExtractionResultRaw(BaseModel):
    embedding: List[float]

class FeatureExtractor:

    def __init__(self):
        # ... initialise the detection model, etc. ...
        pass

class FeatureExtractionService:

    def __init__(self):
        # ... initialise detection models from configs, etc. ...
        # configs contain detector IDs

        # { detector_id: FeatureExtractor }
        self._extractors: Dict[str, FeatureExtractor] = dict()

    async def get_extractors(self) -> List[str]:
        """
        Return IDs of available extractors.
        """
        return sorted(list(self._extractors.keys()))

    async def extract_features(self, image: bytes, extractor_id: str) -> FeatureExtractionResultRaw:
        raise NotImplementedError()