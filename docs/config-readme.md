# Runtime configuration

*Documentation in progress.*

This document describes runtime configuration options. These are mostly concentrated in:
- the environment variables (defined in [`docker.env`](../docker.env) and [`docker-compose.yml`](../docker-compose.yml) / [`docker/compose-gpu.yml`](../docker/compose-gpu.yml) for the Docker deployment);
- the configuration files (examples are available under [`examples/config`](../examples/config)).

There are some hardcoded settings as well, although most of them are planned to be moved to one of these two places.

**IMPORTANT NOTE**: All paths specified in YAML configuration files **must be relative to [`ASSETS_DIR`](../docker.env#L44)**.

## Pipeline

*Example file: [`artefacts_and_images.yaml`](../examples/config/pipeline/master/artefacts_and_images.yaml).*
*Base Pydantic model: [`BasePipelineConfig`](../tram_analytics/v1/pipeline/pipeline/base/config.py#L13).*

The processing pipeline's configuration consists mainly of several files responsible for the different steps.

Global settings:
- `out_to`: Defines the manner of operation for the pipeline. Three options are implemented:
    - `artefacts_stream`: Will expose an iterator returning, for each output frame, the **annotated image** (as a BGR NumPy array of dtype `uint8`) and the **master DTO** ([`PipelineArtefacts`](../tram_analytics/v1/models/pipeline_artefacts.py#L14)) describing the scene state at this frame. This is the main mode for the pipeline and the option to be used with the dashboard.
    - `img_stream`: Will expose an iterator returning only the annotated image.
    - `file`: Will write all annotated output frames to a video file.
- `progress_bar` (default `false`): Whether to display a [`tqdm`](https://github.com/tqdm/tqdm) progress bar in the console (useful for reading from a video file rather than from a network stream).
  
Additional settings for a video writer pipeline (`out_to` set to `file`):
- `out_video_path`: The path to which to write the resulting video file.
- `out_fps`: The target frame rate for the output video.

Config file paths for individual modules (`config_paths`):
- `frame_ingestion`: for the frame ingestion step;
- `detection`: for the object (vehicle) detection step;
- `homography`: to define point correspondences for building a homography matrix (pixel coordinates to world coordinates);
- `zones`: to define zones of interest (tram tracks, platforms, intrusion zones for cars);
- `speed`: for speed estimation;
- `scene_events`: for the scene state updater;
- `visualiser`: for image annotation;
- `track_colours`: to define colour palettes for vehicle trajectory visualisation.

### Frame ingestion

*Example file: [frame_ingestion.yaml](../examples/config/pipeline/components/frame_ingestion/frame_ingestion.yaml).*
*Pydantic model: [EnhancedFrameStreamerConfig](../tram_analytics/v1/pipeline/components/frame_ingestion/frame_streamer/from_file/config.py#L9).*

* `video_resource_id`: a path to the source video file or a URL to the source video stream (RTSP has been tested to work).
* `video_track_idx`: the index of the video track in the container (default: `0`).
* `camera_id`: the ID to be used internally and in the output for this camera.
* `target_height`: If set to a non-negative integer value, the video frame will be resized to have this height prior to being passed downstream. Default: `null` (no resizing).
* `video_start_manual_ts`: A manually set timestamp that will be used as the video's start time (namely, as the first frame's timestamp). Must be formatted as an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) string (example: `2025-11-26T08:17:17.200000+00:00`). Default: `null` (sets the video's start time to the current timestamp at init time for network streams, and to the file's [birth time (Linux)](https://manpages.debian.org/trixie/manpages-dev/statx.2.en.html#stx_btime) for videos read from files).

Additional parameters for reading from a video file (will not work properly with ingestion from a network stream):
* `loop_video`: whether to loop the video indefinitely (default: `false`).
* `frame_range`: a range (inclusive) of frames to be processed. The index of the first frame is 0. Default: `null` (to process all frames).

### Detection

*Example file: [detection.yaml](../examples/config/pipeline/components/detection/detection.yaml).*
*Pydantic model: [DetectionServiceConfig](../tram_analytics/v1/pipeline/components/detection/detection_config.py#L95).*

* `deployment.option`: Defines how to run the detector models defined in this configuration file. Two options are available:
    * `single_process`: Run the models in a single process and perform inference sequentially.
    * `separate_worker_processes` *(recommended)*: Run the models in child processes (one per model) and perform inference in parallel. (NOTE: For a CPU-only dev environment, see global runtime settings defined in [`configure_cpu_inference_runtime()`](/home/mssrchapelier/prep/vehicles/belgrade_trams/common/settings/cpu_settings.py#L1) for optimisation.)
* Detectors (under `detectors`):
    * `detector_id`: a string ID for the detector.
    * `detector_type`: the implementation of the detector. Currently, only `yolo` is fully implemented.
    * `classes`: mappings between numerical class IDs in the output and one of the two: `car` or `tram`. Example:
        ```
        0: tram
        1: car
        ```
    * `roi`: the region of interest for the detector (only detections inside this region, as defined below, will be included in the detector's output).
        * `coords`: A list of pixel coordinates in the format `[ [x_1, y_1], ..., [x_n, y_n] ]` for each of the `n` vertices defining the ROI polygon.
        * `policy`: The criterion for including a bounding box in the output with respect to the ROI:
            * `centroid`: If the bounding box's centroid is inside the ROI.
            * `area_fraction`: If the percentage of the bounding box's area that is inside the ROI is greater than or equal to `min_area_fraction`.
        * `min_area_fraction` (only with `policy` set to `area_fraction`): see above.
    
    For YOLO detectors:
    * `weights_path`: the path to the weights file for the model.
    * `init_kwargs`: Any additional keyword arguments with which the model is initialised (see [constructor docs for `ultralytics.YOLO`](https://docs.ultralytics.com/reference/models/yolo/model/#ultralytics.models.yolo.model.YOLO)).
    * `run_kwargs`: Keyword arguments for prediction runs with this model (see [Ultralytics docs](https://docs.ultralytics.com/modes/predict/#inference-arguments)). Useful ones may include `imgsz` to set the size for inference, `device` for GPU inference. If the model is trained to detect objects that are not mapped to cars or trams, list only the relevant classes in `classes` (it is planned to make this unnecessary).
    