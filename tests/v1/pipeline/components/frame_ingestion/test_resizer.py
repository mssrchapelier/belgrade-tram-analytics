from typing import NamedTuple, Iterator, Tuple, List, Annotated
from unittest import TestCase
from contextlib import contextmanager
from fractions import Fraction
from pathlib import Path

from pydantic import BaseModel, BeforeValidator
import av
from av import VideoStream
from av.container import InputContainer
from av.video.frame import VideoFrame

from tests import DATA_ROOT
from tram_analytics.v1.pipeline.components.frame_ingestion.resizer import FrameResizer
from common.utils.pydantic.csv_helpers import empty_str_to_none, load_models_from_csv

FRAME_INGESTION_DATA_DIR: Path = DATA_ROOT / "v1/pipeline/components/frame_ingestion"

# --- models for test cases ---

class TargetWidthCase(BaseModel):
    description: str
    orig_w: int
    orig_h: int
    target_h: Annotated[int | None, BeforeValidator(empty_str_to_none)]
    expected_target_w: Annotated[int | None, BeforeValidator(empty_str_to_none)]

class ResizeCase(BaseModel):
    description: str
    target_h: Annotated[int | None, BeforeValidator(empty_str_to_none)]

class VideoResource(NamedTuple):
    filepath: Path
    video_track_idx: int

@contextmanager
def _get_video_fixtures(resource: VideoResource) -> Iterator[Tuple[InputContainer, VideoStream, VideoFrame]]:
    with av.open(resource.filepath) as container: # type: InputContainer
        stream: VideoStream = container.streams.video[resource.video_track_idx]
        frame_iter: Iterator[VideoFrame] = container.decode(stream)
        frame: VideoFrame = next(frame_iter)
        yield container, stream, frame

class TestFrameResizer(TestCase):

    TARGET_WIDTH_DATA_PATH: Path = FRAME_INGESTION_DATA_DIR / "get_target_width_cases.csv"
    # 568x320
    VIDEO_RESOURCE: VideoResource = VideoResource(
        filepath=FRAME_INGESTION_DATA_DIR / "video_for_resizer.mp4",
        video_track_idx=0
    )
    RESIZE_DATA_PATH: Path = FRAME_INGESTION_DATA_DIR / "test_resize.csv"

    def test_get_target_w(self) -> None:
        source_data: List[TargetWidthCase] = load_models_from_csv(self.TARGET_WIDTH_DATA_PATH,
                                                                  model_type=TargetWidthCase)

        for item in source_data: # type: TargetWidthCase
            with self.subTest(item):
                expected: int | None = item.expected_target_w
                actual: int | None = FrameResizer._get_target_w(
                    orig_w=item.orig_w, orig_h=item.orig_h, target_h=item.target_h
                )
                self.assertEqual(expected, actual)

    @staticmethod
    def _get_expected_dims(original_frame: VideoFrame, target_h: int | None) -> Tuple[int, int]:
        original_w: int = original_frame.width
        original_h: int = original_frame.height
        if target_h is None:
            return original_w, original_h
        scaling_factor: Fraction = Fraction(target_h, original_h)
        expected_resized_w: int = round(original_w * scaling_factor)
        expected_resized_h: int = round(original_h * scaling_factor)
        return expected_resized_w, expected_resized_h

    def test_resize_before_init_fails(self) -> None:
        # set non-null target height
        resizer: FrameResizer = FrameResizer(target_h=50)
        with _get_video_fixtures(self.VIDEO_RESOURCE) as (
                container, stream, frame
        ):  # type: InputContainer, VideoStream, VideoFrame
            # calling resize without init first: should raise an error
            self.assertRaises(RuntimeError,
                              resizer.resize,
                              frame)

    def test_resize_after_init(self) -> None:
        # for a source frame of 568 x 320, the expected target width is 178
        source_data: List[ResizeCase] = load_models_from_csv(self.RESIZE_DATA_PATH,
                                                             model_type=ResizeCase)

        for item in source_data: # type: ResizeCase
            with self.subTest(item):
                resizer: FrameResizer = FrameResizer(target_h=item.target_h)
                with _get_video_fixtures(self.VIDEO_RESOURCE) as (
                        container, stream, original_frame
                ): # type: InputContainer, VideoStream, VideoFrame
                    resizer.init(stream)

                    resized_frame: VideoFrame = resizer.resize(original_frame)

                    expected_resized_w, expected_resized_h = (
                        self._get_expected_dims(original_frame, item.target_h)
                    ) # type: int, int

                    actual_resized_w: int = resized_frame.width
                    actual_resized_h: int = resized_frame.height

                    self.assertEqual(expected_resized_w, actual_resized_w)
                    self.assertEqual(expected_resized_h, actual_resized_h)
