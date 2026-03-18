from typing import Iterator, Tuple, override

from numpy import uint8
from numpy.typing import NDArray

from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.pipeline.pipeline.base.pipeline import BasePipeline


class ArtefactsStreamingPipeline(BasePipeline):

    @override
    def _init_runtime(self) -> None:
        super()._init_runtime()

    @override
    def _reset_runtime(self) -> None:
        super()._reset_runtime()

    def __iter__(self) -> Iterator[Tuple[NDArray[uint8], PipelineArtefacts]]:
        with self:
            for annotated_image, artefacts in self._get_frame_generator():  # type: NDArray[uint8], PipelineArtefacts
                yield annotated_image, artefacts
        print(f"Main pipeline completed")
