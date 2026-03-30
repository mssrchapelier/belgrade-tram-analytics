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
    
### Vehicle info

#### Homography

*Example file: [`homography.yaml`](../examples/config/pipeline/components/vehicle_info/homography.yaml).*

*Pydantic model: [`HomographyConfig`](../tram_analytics/v1/pipeline/components/vehicle_info/components/coord_conversion/homography_config.py#L77).*

This config defines how a [homography matrix](https://en.wikipedia.org/wiki/Homography_(computer_vision)) for converting image coordinates to world coordinates is built for a given scene. The configuration model is designed for use with [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) grid coordinates.

* `method`: The method used to compute a homography matrix (see details in the OpenCV documentation for [`findHomography()`](https://docs.opencv.org/4.x/d9/d0c/group__calib3d.html#ga4abc2ece9fab9398f2e560d53c8c9780)).
    * `method_name`: One of: `default`, `ransac`, `rho`, `lmeds`.
    * `params` (for `ransac` only): `max_iters`(default: 2000) -- the maximum number of RANSAC iterations.
* `defining_points`: a list of defining points for calculation of the homography matrix.
  Each entry has the following format:
    * `image`: the pixel coordinates for the mapped point. Example:
        ```
        x: 203
        y: 118
        ```
    * `world`: the UTM coordinates for the mapped point: `northing`, `easting`, and `zone` (with `number` and `letter`). Example:
        ```
        northing: 4961260.0
        easting: 457608.0
        zone:
          number: 34
          letter: "T"
        ```
      
#### Zones

*Example file: [`zones_config.yaml`](../examples/config/pipeline/components/vehicle_info/zones_config.yaml).*

*Pydantic model: [`ZonesConfig`](../tram_analytics/v1/pipeline/components/vehicle_info/components/zones/zones_config.py#L126).*

This configuration file defines the zones for the given camera for which analytics are to be calculated by the pipeline.

Three types of zones are defined:
* `tracks`: rail tracks for trams;
* `platforms`: sections of tracks defined as stopping platforms for trams;
* `intrusion_zones`: areas inside which cars are tracked (with the primary intended use being detecting cars being present on separated tracks, possibly in violation of traffic rules).

Inside each of these groups, individual zones are defined under the `zones` key, as a list.

Each zone entry contains the following:
* `zone_id`: A string ID for the zone (unique across cameras).
* `zone_numerical_id`: A camera-internal numerical ID for the zone. Used to render analytics on the dashboard in a more convenient way (e. g. "stop 1", "track 2").
* `description`: A human-readable description for the zone.
* `coords`: The defining points (in *pixel coordinates*) for the zone, defined differently for the different zone types:
    * For tracks:
        * `polygon`: A polygon drawn around the area between the two rails forming a track.
        * `centreline`: A polygonal chain (a series of line segments) drawn along the track's centreline. It can start and end *outside* of the track polygon (it will be trimmed to its part contained inside the track polygon), but not *inside* it.
    * For platforms:
        * `track_zone_id`: The zone ID of the track to which this platform belongs.
        * `platform_endpoints_supporting_lines`: Two lines defined so that, for each of the lines, the point where it intersects the track's centreline is the platform's terminus (start/end). Specified as follows:
        ```
        [
            # intersection of this line with the centreline defines the platform's START
            [
                [supporting_line_for_start_x1, supporting_line_for_start_y1],
                [supporting_line_for_start_x2, supporting_line_for_start_y2]
            ],
            # ... defines the platform's END
            [
                [supporting_line_for_end_x1, supporting_line_for_end_y1],
                [supporting_line_for_end_x2, supporting_line_for_end_y2]
            ]
        ]
        ```
    * For intrusion zones:
        * `polygon`: A polygon drawn around the entire intrusion zone.
    
Additionally, for intrusion zones in general, the following must be specified under `intrusion_zones.assignment_settings`:
* `min_area_frac_inside_zone`: The minimum fraction of the area of a vehicle's bounding box that needs to be inside the zone's polygon for the zone to be assigned to this vehicle.

#### Speeds

*Example file: [`speed_config.yaml`](../examples/config/pipeline/components/vehicle_info/speed_config.yaml).*

*Pydantic model: [`SpeedCalculatorConfig`](../tram_analytics/v1/pipeline/components/vehicle_info/components/speeds/speeds.py#L36).*

The configuration file specifies only the parameters for speed smoothing (under `smoothing`) as follows:
* `method`: The name of the method for speed smoothing under `method_name` and any parameters. Currently, the only implemented method is `mean_velocity` (no additional parameters need to be specified under `method`).
* `window`: The parameters for the sliding window used for speed smoothing, ending with the last raw speed observation.
    * `min_duration`: The minimum duration for the window (in seconds), calculated using the frame timestamps associated with the observations. If a sufficient number of observations does not exist yet to satisfy this constraint, the smoothed speed will be set to null.
    * `max_duration`: The maximum duration for the window (in seconds). Can be set to null, which will smooth over all available history (although this behaviour is intended to be deprecated).
    
    It usually makes the most sense to set both these parameters to the same value.
