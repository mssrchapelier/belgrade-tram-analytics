__version__ = "0.2.0"

from typing import Iterator, Tuple, Literal, TypeAlias, Annotated

from numpy.typing import NDArray
from pydantic import Field

from archive.v1.src.v_0_1_0.pipeline.pipeline import (
    BasePipeline_Old, PipelineArtefacts_Old, BasePipelineConfig, PipelineOutputType,
    ImageStreamingPipelineConfig, VideoWriterPipelineConfig
)

class ArtefactsStreamingPipelineConfig(BasePipelineConfig):
    out_to: Literal[PipelineOutputType.ARTEFACTS_STREAM] = PipelineOutputType.ARTEFACTS_STREAM

PipelineConfig: TypeAlias = Annotated[
    ImageStreamingPipelineConfig | VideoWriterPipelineConfig | ArtefactsStreamingPipelineConfig,
    Field(discriminator="out_to")
]

class ArtefactsStreamingPipeline(BasePipeline_Old):

    def _init_runtime(self) -> None:
        super()._init_runtime()

    def _reset_runtime(self) -> None:
        super()._reset_runtime()

    def __iter__(self) -> Iterator[Tuple[NDArray, PipelineArtefacts_Old]]:
        with self:
            for annotated_image, artefacts in self._get_frame_generator():  # type: NDArray, PipelineArtefacts_Old
                yield annotated_image, artefacts
        print(f"Pipeline completed")