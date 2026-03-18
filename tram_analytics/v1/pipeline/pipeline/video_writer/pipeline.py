from pathlib import Path

from numpy import uint8
from numpy.typing import NDArray

from common.settings.constants import ASSETS_DIR
from common.utils.fileops_utils import resolve_rel_path
from tram_analytics.v1.pipeline.pipeline.base.pipeline import BasePipeline
from tram_analytics.v1.pipeline.pipeline.video_writer.config import VideoWriterPipelineConfig
from tram_analytics.v1.pipeline.pipeline.video_writer.video_writer import FileVideoWriter


class VideoWriterPipeline(BasePipeline):

    def __init__(self, config: VideoWriterPipelineConfig):
        super().__init__(config)
        self._out_video_path: Path = (
            resolve_rel_path(Path(config.out_video_path),
                             ASSETS_DIR)
        )

    def _init_runtime(self) -> None:
        super()._init_runtime()
        self._video_writer: FileVideoWriter = FileVideoWriter(str(self._out_video_path),
                                                              fps=self._config.out_fps)
        Path(self._out_video_path).parent.mkdir(parents=True, exist_ok=True)

    def _reset_runtime(self) -> None:
        self._video_writer.close_runtime()
        super()._reset_runtime()

    def run(self) -> None:
        with self:
            for out_img in self._get_frame_generator_imgonly(): # type: NDArray[uint8]
                self._video_writer.write_frame(out_img)
        print(f"Pipeline completed. Video written to: {self._out_video_path}")
