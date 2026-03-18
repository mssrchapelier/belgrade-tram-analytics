from typing import List

from pydantic import BaseModel

class Bbox(BaseModel):
    # coordinates (absolute, pixels)
    x1: int
    x2: int
    y1: int
    y2: int

class ImagePatchRaw(BaseModel):
    image: bytes

class PatchExtractionService:

    async def extract_patches(self, image: bytes, bboxes: List[Bbox]) -> List[ImagePatchRaw]:
        """
        Crops patches defined by bounding boxes from the original image and returns them
        (in the same order).
        """
        raise NotImplementedError()