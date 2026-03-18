from typing import Iterator

from numpy import uint8
from numpy._typing import NDArray

from tram_analytics.v1.pipeline.pipeline.base.pipeline import BasePipeline


class ImageStreamingPipeline(BasePipeline):

    def _init_runtime(self) -> None:
        super()._init_runtime()

    def _reset_runtime(self) -> None:
        super()._reset_runtime()

    def __iter__(self) -> Iterator[NDArray[uint8]]:
        with self:
            for out_img in self._get_frame_generator_imgonly():  # type: NDArray[uint8]
                yield out_img
        print(f"Pipeline completed")
