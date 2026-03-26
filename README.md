# belgrade-tram-analytics

## What this is

This repository contains a prototype system for obtaining and displaying **domain-specific traffic analytics** for tram zones (tracks and platforms) and occupying vehicles from a video stream in real time.

<div align="center" width="100%">
    <img src="./docs/res/project_showcase.png" alt="Project showcase: Overview of the rendered dashboard, examples of annotated images, and a diagram for the master data transfer object" width="50%" />
</div>

The system is built around a core **processing pipeline** consisting of multiple stages ([frame ingestion](tram_analytics/v1/pipeline/components/frame_ingestion), [object detection](tram_analytics/v1/pipeline/components/detection), [tracking](tram_analytics/v1/pipeline/components/tracking), [speed, class-specific reference point and zone assignment](tram_analytics/v1/pipeline/components/vehicle_info), derived [domain-specific events](tram_analytics/v1/pipeline/components/scene_state/events), [live scene state](tram_analytics/v1/pipeline/components/scene_state/live_state_updater), [annotated image rendering](tram_analytics/v1/pipeline/components/visualiser)), and also includes an **[API server](tram_analytics/v1/pipeline/server)** and an **[operator dashboard](tram_analytics/v1/dashboard)** for real-time monitoring. For deployment, [Docker images](https://hub.docker.com/r/mssrchapelier/belgrade-tram-analytics) for CPU-only and GPU runtimes and a set of automatically pre-fetched demo assets are provided.

The system is designed for use in urban traffic analytics settings focusing on trams, but is applicable to rail vehicles in general and handles wheeled vehicles as well (albeit with a more general approach).

Domain-specific adaptations implemented in this system include:
- specialised treatment of trams (rail track and platform assignment, track centreline-bound reference points);
- speed estimation using homography matching (mapping pixels to [UTM coordinates](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system)) and a specific algorithm for trams;
- a domain-specific event emitter and a live state updater focusing on time periods of interest (time spent by the vehicle inside a specific zone, current and previous occupancy for a zone, stationary periods per vehicle/per zone, etc.)

**Completed ([`v1`](./tram_analytics/v1))**:
- real-time synchronous pipeline
- YOLO v11 "nano" tram detector finetuned for one 320p camera on a manually curated dataset (a tram stop zone in Belgrade -- Nemanjina ulica, near Trg Slavija)
- FastAPI server for the master DTO + annotated image
- dashboard (Gradio base + Jinja2 HTML templates / CSS stylesheet)
- Docker deployment for CPU-only and GPU targets (pre-built images are available [on Docker Hub](https://hub.docker.com/r/mssrchapelier/belgrade-tram-analytics))

***In development ([`v2`](./tram_analytics/v2))***:
- concurrent operation of the pipeline's steps -- message queue (RabbitMQ)-based
- multi-stream capability, including whilst sharing the same detector model(s)
- Redis for caching real-time data
- SQLite and MinIO for persistence (for a future historical analytics module)

*Planned (v3)*:
- historical analytics exposed through an API endpoint (and to the operator through a section of the dashboard)

## How to use

### Option A: Docker

#### Available images

1. For CPU-only inference environments: tagged with `-cpu`. This is the default image (for ease of running locally).
   - The corresponding Dockerfile is: [`./Dockerfile`](Dockerfile).
   - The Compose file is: [`./docker-compose.yml`](docker-compose.yml).
2. For GPU inference environments: tagged with `-gpu`. This image includes NVIDIA dependencies.
   - The Dockerfile is: [`./docker/Dockerfile-gpu`](docker/Dockerfile-gpu).
   - The Compose file is [`./docker/compose-gpu.yml`](docker/compose-gpu.yml).

#### Demo assets

For the purposes of demonstration, a set of assets consisting of a sample video, model weights for the detection module, and a configuration file bundle is provided.

The user does not need to download them separately and can simply run `docker compose up` (see below), because the Docker containers created here are set up to fetch these files automatically (if they are not already present) from a public R2 bucket prior to starting the application itself. Alternatively, the user can provide their own files or set up ingestion from a network stream (see details below).

If desired, the demo assets can be downloaded manually from the bucket through URLs specified in [`docker/download_demo_assets.sh`](docker/download_demo_assets.sh). Sample configs are also contained in this repository (see [`examples/config`](examples/config)).

#### Steps

1. Clone the repository:
    ```bash
    git clone https://github.com/mssrchapelier/belgrade-tram-analytics.git
    cd belgrade-tram-analytics
    ```

2. Obtain the image:

    - Option A: Pull from Docker Hub:
    
        - CPU-only:
        ```bash
        docker compose pull
        ```
        - GPU-enabled:
        ```bash
        docker compose -f ./docker/compose-gpu.yml pull
        ```
    
    - Option B: Build from source:
    
        - CPU-only:
        ```bash
        docker compose build
        ```
        - GPU-enabled:
        ```bash
        docker compose -f ./docker/compose-gpu.yml build
        ```
    
        *(Optional note for building:)* If rebuilding the image is not expected, the `pip` cache mount may be removed from the Dockerfile by removing `--mount=type=cache,target=/root/.cache/pip` from the options for `RUN ... pip install ...`. This will, however, make any re-builds take as much time as the first one. Alternatively, just run `docker builder prune` when re-building is no longer expected.

3. Create and start the container:
   
    - CPU-only:
    ```bash
    docker compose up -d
    ```
    - GPU-enabled:
    ```bash
    docker compose -f ./docker/compose-gpu.yml up -d
    ```

    (If monitoring the logs is desired, remove `-d` from the command to run in foreground mode.)

4. Wait for a few seconds for the service to start.
   
5. Access:
   - the dashboard at `http://localhost:8091`;
   - if desired, the pipeline API server at `http://localhost:8081/latest` at any moment to get the most recent cached master DTO as JSON.

6. To stop the container:
   
    - CPU-only:
    ```bash
    docker compose down
    ```
    - GPU-enabled:
    ```bash
    docker compose -f ./docker/compose-gpu.yml down
    ```
    (or `Ctrl-C` if running in foreground mode, and wait for a couple of seconds for a graceful shutdown).

#### Modifications

##### Provide custom assets

Custom assets can be provided by configuring the [`tram-analytics-assets` named volume](docker-compose.yml#L25) to bind a specific host directory as follows:
```yaml
driver: local
driver_opts:
    type: none
    o: bind
    device: path/to/local/assets/dir
```

##### Ingest from a network stream

The pipeline can be set up to ingest from a network video stream by setting `video_resource_id` to a URL value in the frame ingestion configuration file ([example](examples/config/pipeline/components/frame_ingestion/streamer_topipe.yaml#L2)). The system has been tested to work with RTSP streams.

For testing purposes, a video file can be streamed locally, but the description of this falls outside the scope of this document; a convenient setup that has been tested and can be suggested is the combination of [FFmpeg](https://ffmpeg.org/) and [MediaMTX](https://mediamtx.org/).

### Option B: Use directly

#### Dependencies

Create a virtual environment and install dev dependencies for either a CPU-only ([`requirements/dev-cpu.txt`](requirements/dev-cpu.txt)) or a GPU ([`requirements/dev-gpu.txt`](requirements/dev-gpu.txt)) environment (change `<target>` accordingly):
```bash
python -m venv .venv \
# (on Linux)
&& source .venv/bin/activate \
&& pip install -r ./requirements/dev-<target>.txt
```

(Note: For the GPU version, the large size of NVIDIA-related dependencies may cause `/tmp` to fill up and for `pip install` to fail. It may be necessary to use e. g. `--cache-dir` with a custom location and clean up manually afterwards.)

#### Make environment variables available at runtime

The application expects variables under the keys specified in [`docker.env`](./docker.env) to be available at runtime as environment variables. The containerised deployment provides this file through Compose; for local use, these variables must first be provided through some other means. The `docker.env` file can be used as a template, but the developer will need to copy and modify it.

An env file can be loaded e. g. by using [`dotenv`](https://github.com/theskumar/python-dotenv) (included in dev dependencies) in the following way:
```python
from dotenv import load_dotenv
load_dotenv("/path/to/.env")
```
Alternatively, the same variables can be loaded in any other convenient way (e. g. `os.environ["ASSETS_DIR"] = "path/to/local/assets/dir"` and so on).

When using from a dev environment, it is best to load these in the calling module **prior to importing anything from `tram_analytics.v1`**, as most imports from [`common.settings.constants`](./common/settings/constants.py) contained therein will not otherwise resolve.

#### Provide assets

The following must be present in `ASSETS_DIR`:
- a set of configuration files;
- model weights for the detector(s);
- if consuming from a video file: the video file.

Sample assets can be downloaded from the aforementioned R2 bucket; see paths to individual files in [`docker/download_demo_assets.sh`](docker/download_demo_assets.sh).

#### Entry points

##### Joint launcher

Pipeline worker + API server; dashboard; logging server:
- module: [`tram_analytics.v1.launcher_joint`](./tram_analytics/v1/launcher_joint.py),
- function: [`launch()`](./tram_analytics/v1/launcher_joint.py#L107).

##### Pipeline (separately)

Pipeline as a worker process + API server:
- module [`tram_analytics.v1.pipeline.server.pipeline_server`](./tram_analytics/v1/pipeline/server/pipeline_server.py),
- function [`run_pipeline_server()`](./tram_analytics/v1/pipeline/server/pipeline_server.py#L75) (expects a config path).

Pipeline (in-process):

module [`tram_analytics.v1.pipeline.pipeline.run_pipeline`](./tram_analytics/v1/pipeline/pipeline/run_pipeline.py);

by type of output (all expect a config file path to be passed):

- DTOs + annotated images: [`run_artefacts_streaming_pipeline()`](./tram_analytics/v1/pipeline/pipeline/run_pipeline.py#L63);
- annotated images only: [`run_image_streaming_pipeline()`](./tram_analytics/v1/pipeline/pipeline/run_pipeline.py#L26);
- write to a video file: [`run_video_writer_pipeline()`](./tram_analytics/v1/pipeline/pipeline/run_pipeline.py#L19).

##### Dashboard

Dashboard (not terribly useful on its own, however, without the pipeline server running):
- module [`tram_analytics.v1.dashboard.dashboard.AsyncLiveUpdateGetter`](./tram_analytics/v1/dashboard/dashboard.py);
- *coroutine* [`async_run_dashboard()`](./tram_analytics/v1/dashboard/dashboard.py#L200) (expects the paths to a dashboard config and a live state renderer config).

(Note: The dashboard handles API calls to the pipeline server asynchronously through a wrapper utilising `aiohttp` used as an async context manager, and it was judged best for the calling code to manage the event loop; for this reason, this is a coroutine rather than a function.)

##### Logging server

TCP logging server (not strictly necessary, but messages logged from some of the child processes will not reach stderr then):
- module [`common.utils.logging_utils.logging_server`](./common/utils/logging_utils/logging_server.py);
- function [`run_logging_server()`](./common/utils/logging_utils/logging_server.py#L60).
