__version__ = "0.2.0"

from typing import Dict, List, Self, Tuple

import gradio as gr
from gradio import Blocks, Row, Column, Accordion, HTML, Image, Timer, JSON

import requests
from requests import Response, Session
from PIL.Image import Image as ImageType
from pydantic import ValidationError

from common.utils.img.img_bytes_conversion import pil_from_bytes
from archive.v1.src.v_0_1_0.pipeline.pipeline import PipelineArtefacts_Old

UPDATE_INTERVAL_S: float = 1.0

def _display_speeds_disclaimer():
    gr.Info(message="NOTE: All speeds are approximations from a single camera.",
            duration=None)

class LiveUpdateGetter:

    def __init__(self, latest_endpoint: str, image_endpoint: str):
        self._latest_endpoint: str = latest_endpoint
        self._image_endpoint: str = image_endpoint
        self._session: Session | None = None

    def __enter__(self) -> Self:
        self._session = requests.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._session.close()

    def get_newest(self) -> Tuple[ImageType | None, Dict | List | None]:
        if self._session is None:
            raise RuntimeError("Called _get_state with _session set to None. "
                               "LiveUpdateGetter should be used as a resource.")

        artefacts_dumped: PipelineArtefacts_Old | None = None
        annotated_image: ImageType | None = None

        # get artefacts for the newest state
        artefacts_response: Response = self._session.get(self._latest_endpoint)
        if artefacts_response.status_code != 200:
            print("Failed to get artefacts for the latest state")
        else:
            artefacts_data: Dict = artefacts_response.json()
            try:
                artefacts = PipelineArtefacts_Old(**artefacts_data)
                artefacts_dumped = artefacts.model_dump()
                frame_id: str = artefacts.frame_metadata.frame_id
                # get the image for this state
                image_url: str = f"{self._image_endpoint}/{frame_id}"
                image_response: Response = self._session.get(image_url)
                if image_response.status_code != 200:
                    print(f"Failed to get image: {frame_id}")
                else:
                    annotated_image = pil_from_bytes(image_response.content)
            except ValidationError:
                print(f"Couldn't parse artefacts: {str(artefacts_data)}")

        return annotated_image, artefacts_dumped


def run_dashboard():

    updater: LiveUpdateGetter = LiveUpdateGetter(
        latest_endpoint="http://localhost:8081/latest",
        image_endpoint="http://localhost:8081/image"
    )

    with Blocks() as dashboard, updater: # type: Blocks, LiveUpdateGetter

        timer: Timer = Timer(UPDATE_INTERVAL_S)

        with Row():
            with Column(scale=3):
                with Accordion(label="Live updates", open=True):
                    live_updates_json: JSON = JSON(
                        label="Current state"
                    )
                with Accordion(label="Historical statistics", open=False):
                    hist_stats_text: HTML = HTML(
                        "<p><em>Historical statistics</em></p>"
                    )
            with Column(scale=5):
                canvas: Image = Image(
                    interactive=False,
                    show_label=False,
                    label="Current frame"
                )

        timer.tick(fn=updater.get_newest,
                   inputs=[],
                   outputs=[canvas, live_updates_json])

        dashboard.load(_display_speeds_disclaimer)

    dashboard.queue().launch(theme=gr.themes.Base(),
                             server_name="localhost", server_port=8091)

if __name__ == "__main__":
    run_dashboard()