from typing import List

from pydantic import BaseModel

from archive.v_a1.detector import DetectionRaw
from archive.v_a1.frame_producer import FrameItemRaw

class FrameItem(FrameItemRaw):
    frame_id: str

class Detection(DetectionRaw):
    detection_id: str
    frame_id: str

class ImagePatch(BaseModel):
    patch_id: str
    detection_id: str
    image: bytes

class PatchEmbedding(BaseModel):
    patch_embedding_id: str
    patch_id: str
    embedding: List[float]

class DetectionTrackMapping(BaseModel):
    detection_id: str
    track_id: str