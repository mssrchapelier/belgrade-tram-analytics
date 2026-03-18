from dataclasses import dataclass

from numpy import uint8
from numpy.typing import NDArray

from archive.v1.src.v_0_1_0.pipeline.pipeline_server import _encode_numpy_image
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts


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
