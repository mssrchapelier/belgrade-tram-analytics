# Runtime configuration

*Documentation in progress.*

This document describes runtime configuration options. These are mostly concentrated in:
- the environment variables (defined in [`docker.env`](../docker.env) and [`docker-compose.yml`](../docker-compose.yml) / [`docker/compose-gpu.yml`](../docker/compose-gpu.yml) for the Docker deployment);
- the configuration files (examples are available under [`examples/config`](../examples/config)).

There are some hardcoded settings as well, although most of them are planned to be moved to one of these two places.
  
## Pipeline

*Example file: [`artefacts_and_images.yaml`](../examples/config/pipeline/master/artefacts_and_images.yaml).*
*Base Pydantic model: [`BasePipelineConfig`](../tram_analytics/v1/pipeline/pipeline/base/config.py#L13).*

The processing pipeline's configuration consists mainly of several files responsible for the different steps.

Global settings:
- `out_to`: Defines the manner of operation for the pipeline. Three options are implemented:
    - `artefacts_stream`: Will expose an iterator returning, for each output frame, the **annotated image** (as a BGR NumPy array of dtype `uint8`) and the **master DTO** ([`PipelineArtefacts`](../tram_analytics/v1/models/pipeline_artefacts.py#L14)) describing the scene state at this frame. This is the main mode for the pipeline and the option to be used with the dashboard.
    - `img_stream`: Will expose an iterator returning only the annotated image.
    - `file`: Will write all annotated output frames to a video file.
- `out_fps`: The target frame rate for the **output**. Only the required frames will be sampled and processed if this value is lower than the frame rate of the source video stream.
- `progress_bar` (default `false`): Whether to display a [`tqdm`](https://github.com/tqdm/tqdm) progress bar in the console (useful for reading from a video file rather than from a network stream).
- `out_video_path` (only with `out_to` set to `file`): The path to which to write the resulting video file (for a video writer pipeline).

Config file paths for individual modules (`config_paths`):
- `frame_ingestion`: for the frame ingestion step;
- `detection`: for the object (vehicle) detection step;
- `homography`: to define point correspondences for building a homography matrix (pixel coordinates to world coordinates);
- `zones`: to define zones of interest (tram tracks, platforms, intrusion zones for cars);
- `speed`: for speed estimation;
- `scene_events`: for the scene state updater;
- `visualiser`: for image annotation;
- `track_colours`: to define colour palettes for vehicle trajectory visualisation.
