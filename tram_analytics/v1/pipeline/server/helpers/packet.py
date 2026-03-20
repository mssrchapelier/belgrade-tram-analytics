from dataclasses import dataclass
from typing import List

import cv2
import numpy as np
from numpy import uint8
from numpy.typing import NDArray

from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts

OUT_IMG_FORMAT: str = ".jpg"
CV2_FLAGS: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 90]

@dataclass(slots=True, kw_only=True)
class PipelinePacket:
    annotated_image: bytes
    artefacts: PipelineArtefacts


def _build_pipeline_packet(
        annot_img_numpy: NDArray[uint8], artefacts: PipelineArtefacts
) -> PipelinePacket:
    encoded_img: bytes = _encode_numpy_image(annot_img_numpy)
    return PipelinePacket(annotated_image=encoded_img,
                          artefacts=artefacts)

def _encode_numpy_image(img_numpy: NDArray[uint8]) -> bytes:
    contiguous: NDArray[uint8] = np.ascontiguousarray(img_numpy)
    success, encoded = cv2.imencode(
        ext=OUT_IMG_FORMAT, img=contiguous, params=CV2_FLAGS
    )  # type: bool, NDArray[uint8]
    encoded_bytes: bytes = encoded.tobytes()
    return encoded_bytes
