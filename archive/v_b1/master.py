from typing import Dict, List, Any

from src.v1_1.models import Frame, Detection, TrackAssignment
from src.v1_1.frame_ingestion import FileFrameStreamer
from src.v1_1.detection import DetectionService, InProcessYOLODetector
from src.v1_1.drawing import Visualizer
from common.utils.img.img_bytes_conversion import write_img_bytes_to_path

def run():
    from pathlib import Path
    from time import perf_counter

    video_path: str = "REDACTED/dataset/short_videos/A_B.mp4"
    camera_id: str = "cam1"
    detector_id: str = "detwkr1"
    class_ids: List[int] = [0]
    weights_path: str = "REDACTED/run_results/run-2025_11_22-23_13/weights/best.pt"
    model_init_kwargs: Dict[str, Any] | None = None
    model_run_kwargs: Dict[str, Any] | None = None
    out_img_dir: str = "REDACTED/v1_master/A_B"

    out_dir_path: Path = Path(out_img_dir)
    out_dir_path.mkdir(parents=True)

    frame_producer: FileFrameStreamer = FileFrameStreamer(video_resource_id=video_path,
                                                          camera_id=camera_id)
    detection_worker: InProcessYOLODetector = InProcessYOLODetector(
        detector_id=detector_id, weights_path=weights_path,
        model_init_kwargs=model_init_kwargs, model_run_kwargs=model_run_kwargs
    )
    detection_service: DetectionService = DetectionService([detection_worker])
    # tracker: SortWrapper = SortWrapper({0: SingleClassSortParams()})
    visualizer: Visualizer = Visualizer()

    with frame_producer, detection_service: # type: FileFrameStreamer, DetectionService
        for idx, frame in enumerate(frame_producer): # type: int, Frame

            ts_start_det: float = perf_counter()
            dets: List[Detection] = detection_service.detect_old(frame)
            time_det: float = perf_counter() - ts_start_det

            # ts_start_track: float = perf_counter()
            # tracks: List[TrackAssignment] = tracker.update(dets)
            # time_track: float = perf_counter() - ts_start_track

            ts_start_imgproc: float = perf_counter()
            dest_img: bytes = visualizer.process_frame_old(frame, dets)
            time_imgproc: float = perf_counter() - ts_start_imgproc

            dest_path: str = str(out_dir_path.joinpath(f"{idx:04d}.png"))
            ts_start_imgwrite: float = perf_counter()
            write_img_bytes_to_path(dest_img, dest_path, img_format="PNG")
            ts_end_imgwrite: float = perf_counter()
            time_imgwrite: float = ts_end_imgwrite - ts_start_imgwrite

            time_total: float = ts_end_imgwrite - ts_start_det

            print(f"frame: {idx:04d} | det: {time_det:.3f} s"
                  # + f"| track: {time_track:.3f}"
                  + f" | imgproc: {time_imgproc:.3f} s | imgwrite: {time_imgwrite:.3f} s"
                  + f" | total: {time_total:.3f} s")

if __name__ == "__main__":
    run()