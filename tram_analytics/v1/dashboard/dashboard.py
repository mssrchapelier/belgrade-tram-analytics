from typing import Tuple, Any, Set
import asyncio
from asyncio import Task, AbstractEventLoop, Event
from concurrent.futures import ThreadPoolExecutor
import logging
from logging import Logger
from pathlib import Path
import signal

import gradio as gr
from PIL.Image import Image as ImageType
from aiohttp import ClientSession, ClientResponse, ClientResponseError
from gradio import Blocks, Row, Column, HTML, Image, Timer, JSON
from pydantic import ValidationError
from pydantic_yaml import parse_yaml_file_as

from common.settings.constants import (
    PIPELINE_SERVER_HOST, PIPELINE_SERVER_PORT,
    DASHBOARD_HOST, DASHBOARD_PORT, MAIN_DASHBOARD_RENDERING_MAX_THREADS
)
from common.utils.concurrency.async_session_provider import AsyncSessionProvider
from common.utils.img.img_bytes_conversion import pil_from_bytes
from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v1.dashboard.config import DashboardConfig
from tram_analytics.v1.dashboard.models.live_state import LiveStateForRender
from tram_analytics.v1.dashboard.render.config import LiveStateRendererConfig
from tram_analytics.v1.dashboard.render.render import LiveStateRenderer
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts

PARENT_DIR: Path = Path(__file__).resolve().parent
# favicon for browsers
FAVICON_PATH: Path = PARENT_DIR / "render/favicon.png"
# styles for the main dashboard
MAIN_DASHBOARD_STYLESHEET_PATH: Path = PARENT_DIR / "render" / "style.css"

KB_INTERRUPT_POLLING_INTERVAL: float = 0.5

def _display_speeds_disclaimer():
    gr.Info(message="NOTE: All speeds are approximations from a single camera.",
            duration=None)

class AsyncLiveUpdateGetter:
    """
    An async client for the pipeline's API server.
    After fetching the live state, fetches the image in a separate task
    while rendering the HTML from the received state.
    """

    def __init__(self,
                 *,
                 pipeline_server_hostname: str,
                 pipeline_server_port: int,
                 renderer: LiveStateRenderer,
                 session_provider: AsyncSessionProvider):
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._pipeline_server_base_url: str = f"http://{pipeline_server_hostname}:{pipeline_server_port}"
        self._latest_endpoint: str = f"{self._pipeline_server_base_url}/latest"
        self._image_endpoint: str = f"{self._pipeline_server_base_url}/image"

        self._session_provider: AsyncSessionProvider = session_provider
        self._thread_executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=MAIN_DASHBOARD_RENDERING_MAX_THREADS
        )

        self._renderer: LiveStateRenderer = renderer

    @staticmethod
    def _get_msg_for_response_error(exc: ClientResponseError) -> str:
        return f"(status: {exc.status}, message: {exc.message})"

    def _html_render(self, dto: LiveStateForRender) -> str:
        return self._renderer.render(dto, ref_ts=dto.metadata.frame_timestamp)

    async def _get_session(self) -> ClientSession:
        return await self._session_provider.get_session()

    async def _get_annotated_image(self, frame_id: str) -> ImageType:
        """
        Get the annotated image for the given frame ID. Runs in a separate asyncio task.
        """
        image_url: str = f"{self._image_endpoint}/{frame_id}"
        session: ClientSession = await self._get_session()
        async with session.get(
                image_url, raise_for_status=True
        ) as response:  # type: ClientResponse
            img_bytes: bytes = await response.read()
        image: ImageType = pil_from_bytes(img_bytes)
        return image

    async def _get_rendered_html(self, obj_to_render: LiveStateForRender) -> str:
        """
        Render and return the HTML string for the provided object.
        Runs in the thread pool executor injected into this instance.
        """
        loop: AbstractEventLoop = asyncio.get_running_loop()
        rendered: str = await loop.run_in_executor(self._thread_executor,
                                                   self._html_render,
                                                   obj_to_render)
        return rendered

    async def get_newest(self) -> Tuple[
        ImageType | None, str | None, str | None
    ]:
        """
        Get the latest available state (if one is available).
        :return: (annotated_image, html, json)
        """
        annotated_image: ImageType | None = None
        rendered_html: str | None = None
        exported_json: str | None = None

        # get artefacts for the newest state
        session: ClientSession = await self._get_session()
        async with session.get(
                self._latest_endpoint, raise_for_status=True
        ) as artefacts_response: # type: ClientResponse
            try:
                # deserialise
                artefacts_data: Any = await artefacts_response.json()
            except ClientResponseError as exc:
                # no actual payloads to return if could not get pipeline artefacts;
                # log a warning and return nulls for everything
                self._logger.warning(f"Failed to get artefacts for the latest state: "
                                     f"{self._get_msg_for_response_error(exc)}")
                return annotated_image, rendered_html, exported_json
        try:
            # validate as the pipeline's output model
            artefacts = PipelineArtefacts(**artefacts_data)
            exported_json = artefacts_data

            # get the annotated image for this state in a separate task
            frame_id: str = artefacts.frame_metadata.frame_id
            img_retrieval_task: Task[ImageType] = asyncio.create_task(
                self._get_annotated_image(frame_id)
            )

            # ... render HTML in the meantime:
            # get the live state portion of the output
            live_state: LiveAnalyticsState = artefacts.live_state
            # transform before rendering
            # (move tram platforms to be beside their containing tracks
            # for easier rendering with templates)
            transformed: LiveStateForRender = LiveStateForRender.from_source(live_state)
            # render the HTML for the main dashboard from the transformed live state
            rendered_html = await self._get_rendered_html(transformed)

            # with the HTML now rendered, await the image
            try:
                annotated_image = await img_retrieval_task
            except ClientResponseError as exc:
                # log a warning and continue -- will return null for the image
                self._logger.warning(f"Failed to get image: {frame_id} "
                                     f"{self._get_msg_for_response_error(exc)}")

        except ValidationError:
            self._logger.warning(f"Couldn't parse artefacts: {str(artefacts_data)}")

        return annotated_image, rendered_html, exported_json

async def run_gradio_until_signal(app: Blocks) -> None:

    loop: AbstractEventLoop = asyncio.get_running_loop()
    stop_event: Event = Event()

    def _handle_signal() -> None:
        stop_event.set()

    stop_signals: Set[int] = {signal.SIGINT, signal.SIGTERM}

    for sig in stop_signals: # type: int
        loop.add_signal_handler(sig, _handle_signal)

    try:
        logging.debug("Launching the dashboard ...")
        app.queue().launch(
            # apply a simple theme
            theme=gr.themes.Base(),
            server_name=DASHBOARD_HOST, server_port=DASHBOARD_PORT,
            # disable Gradio's default footer
            footer_links=[],
            favicon_path=FAVICON_PATH,
            # do not lock the thread; managed manually
            prevent_thread_lock=True
        )
        logging.debug("Launched the dashboard")
        await stop_event.wait()
        logging.debug("Got a close signal")
    except KeyboardInterrupt:
        pass
    finally:
        logging.debug("Closing the dashboard ...")
        app.close()
        logging.debug("Closed the dashboard")
        for sig in stop_signals:
            loop.remove_signal_handler(sig)


async def async_run_dashboard(*, dashboard_config_path: str | Path,
                              live_state_renderer_config_path: str | Path) -> None:

    dashboard_config: DashboardConfig = parse_yaml_file_as(
        DashboardConfig, dashboard_config_path
    )
    live_state_renderer_config: LiveStateRendererConfig = parse_yaml_file_as(
        LiveStateRendererConfig, live_state_renderer_config_path
    )

    async with AsyncSessionProvider() as session_provider: # type: AsyncSessionProvider

        # client for the API server for the pipeline,
        # with an injected HTML renderer for the main dashboard
        updater: AsyncLiveUpdateGetter = AsyncLiveUpdateGetter(
            pipeline_server_hostname=PIPELINE_SERVER_HOST,
            pipeline_server_port=PIPELINE_SERVER_PORT,
            renderer=LiveStateRenderer(live_state_renderer_config),
            session_provider=session_provider
        )

        # styles for the main dashboard section
        css_template: str = MAIN_DASHBOARD_STYLESHEET_PATH.read_text(encoding="utf8")

        with Blocks(title=dashboard_config.app_title,
                    fill_width=True,
                    fill_height=True,
                    analytics_enabled=False) as dashboard: # type: Blocks
            timer: Timer = Timer(dashboard_config.update_interval)

            with Column():
                with Row(scale=1):
                    with Column(scale=1):
                        # JSON (full dump, everything produced by the pipeline)
                        live_updates_json: JSON = JSON(label="Current state")
                    with Column(scale=1):
                        # annotated image
                        canvas: Image = Image(interactive=False,
                                              show_label=False,
                                              label="Current frame")
                with Row(scale=2):
                    # main dashboard (custom HTML and CSS)
                    live_updates_html: HTML = HTML(label="Current state",
                                                   css_template=css_template,
                                                   apply_default_css=False)

            # update the elements in `outputs` each period by calling `updater.get_newest()`
            timer.tick(fn=updater.get_newest,
                       inputs=[],
                       outputs=[canvas, live_updates_html, live_updates_json])

            dashboard.load(_display_speeds_disclaimer)

            await run_gradio_until_signal(dashboard)
