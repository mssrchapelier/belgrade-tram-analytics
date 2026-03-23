# belgrade-tram-analytics

## What this is

This repository contains a prototype system for obtaining and displaying domain-specific traffic **analytics for tram zones** (tracks and platforms) and occupying **vehicles** from a video stream **in real time**.

The system is built around a core **processing pipeline** consisting of multiple stages (frame ingestion, object detection, tracking, speed, class-specific reference point and zone assignment, derived domain-specific events, live scene state, annotated image rendering), and also includes an **API server** and an **operator dashboard** for real-time monitoring.

The system is designed for use in urban traffic analytics settings focusing on trams, but is applicable to rail vehicles in general and handles wheeled vehicles as well (albeit with a more general approach).

Domain-specific adaptations implemented in this system include:
- specialised treatment of trams (rail track and platform assignment, track centreline-bound reference points);
- speed estimation using homography matching and a specific algorithm for trams;
- a domain-specific event emitter and a live state updater focusing on time periods of interest (time spent by the vehicle inside a specific zone, current and previous occupancy for a zone, stationary periods per vehicle/per zone, etc.)

**Completed ([`v1`](./tram_analytics/v1))**:
- real-time synchronous pipeline
- tram detector finetuned for one 320p camera on a manually curated dataset (a tram stop zone in Belgrade -- Nemanjina ulica, near Trg Slavija)
- FastAPI server for the master DTO + annotated image
- dashboard (Gradio base + Jinja2 HTML templates / CSS stylesheet)

***In development ([`v2`](./tram_analytics/v2))***:
- concurrent operation of the pipeline's steps -- message queue (RabbitMQ)-based
- multi-stream capability, including whilst sharing the same detector model(s)
- Redis for real-time data flow
- SQLite and MinIO for persistence

*Planned (v3)*:
- historical analytics exposed through an API endpoint (and to the operator through a section of the dashboard)

## How to use

### Option A: Docker (build from source)

*Implemented, but provide the assets on which to run.*

1. Clone the repository:
    ```bash
    git clone https://github.com/mssrchapelier/belgrade-tram-analytics.git
    cd belgrade-tram-analytics
    ```
2. Modify [`docker-compose.yml`](./docker-compose.yml): specify a custom directory on the host to mount into the container in the `volumes` section for the service `tram_analytics`.
3. Build from the modified `docker-compose.yml`:
    ```bash
    docker compose build
    ```
4. *(Must provide a video and configs which to place into the mounted directory. Planned to be hosted on R2 and provided as a public dev link.)*
5. Start the container:
    ```bash
    docker compose up
    ```
6. Wait for a few seconds for the service to start, then access the dashboard at `http://localhost:8091` (and, if desired, the pipeline API server at `http://localhost:8081/latest` at any moment to get the most recent cached master DTO as JSON).
7. To stop the container:
    ```bash
    docker compose down
    ```

### Option B: Use directly

#### Dependencies

Create a virtual environment and install both [base](./requirements/base.txt) (required) and [dev](./requirements/dev.txt) (recommended) dependencies:
```bash
python -m venv .venv \
# (on Linux)
&& source .venv/bin/activate \
&& pip install -r ./requirements/base.txt -r ./requirements/dev.txt
```
Without dev dependencies installed, some of the functionality in [`scripts`](./scripts) (if you need it) might not work.

(Note 1: There are some large dependencies such as `ultralytics` and `torch` which might cause `/tmp` to fill up and for `pip install` to fail. It might be necessary to use e. g. `--cache-dir` with a custom location and clean up manually afterwards.)

(Note 2: Headless versions of `ultralytics` and `opencv-python` instead of the full ones may be installed by specifying [`base-headless`](./requirements/base-headless.txt) instead of `base`.)

#### Make environment variables available at runtime

The application expects variables under the keys specified in [`docker.env`](./docker.env) to be available at runtime as environment variables. The containerised deployment provides this file through Compose; for local use, these variables must first be provided through some other means. This file can be used as a template, but the developer will need to copy and modify it.

An env file can be loaded e. g. by using `dotenv` (listed in [dev dependencies](./requirements/dev.txt)) in the following way:
```python
from dotenv import load_dotenv
load_dotenv("/path/to/.env")
```
Alternatively, the same variables can be loaded in any other convenient way (e. g. `os.environ["ASSETS_DIR"] = "path/to/local/assets/dir"` and the like).

When using from a dev environment, it is best to load these in the calling module *prior to importing anything from `tram_analytics.v1`*, as most imports from [`common.settings.constants`](./common/settings/constants.py) contained therein will not otherwise resolve.

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

#### Dashboard

Dashboard (not terribly useful on its own, however, without the pipeline server running):
- module [`tram_analytics.v1.dashboard.dashboard.AsyncLiveUpdateGetter`](./tram_analytics/v1/dashboard/dashboard.py);
- *coroutine* [`async_run_dashboard()`](./tram_analytics/v1/dashboard/dashboard.py#L200) (expects the paths to a dashboard config and a live state renderer config).

(Note: The dashboard handles API calls to the pipeline server asynchronously through a wrapper utilising `aiohttp` used as an async context manager, and it was judged best for the calling code to manage the event loop; for this reason, this is a coroutine rather than a function.)

#### Logging server

TCP logging server (not strictly necessary, but messages logged from some of the child processes will not reach stderr then):
- module [`common.utils.logging_utils.logging_server`](./common/utils/logging_utils/logging_server.py);
- function [`run_logging_server()`](./common/utils/logging_utils/logging_server.py#L60).

### Option C: Docker (pull from Docker Hub)

*Under implementation.*

## More details

*More extensive documentation is being written and will be available in: [`docs`](./docs).*
