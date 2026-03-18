__version__ = "0.1.0"

from dataclasses import dataclass
from timeit import default_timer

import gradio as gr
from gradio import Blocks, Row, Column, Accordion, HTML, Image, Timer

from PIL.Image import Image as ImageType
import requests
from requests import Response

from common.utils.img.img_bytes_conversion import pil_from_bytes

def _display_speeds_disclaimer():
    gr.Info(message="NOTE: All speeds are approximations from a single camera.",
            duration=None)

@dataclass
class ImageUpdaterTimes:
    response: float
    img_conversion: float

def _get_image_updater_timing_log_line(times: ImageUpdaterTimes) -> str:
    total: float = times.response + times.img_conversion
    out: str = f"Frame processed in {total:.3f} s"
    out += " | " + " | ".join(
        f"{name}: {value:.3f}" for name, value in zip(
            ["response time", "image conversion"],
            [times.response, times.img_conversion]
        )
    )
    return out

class ImageUpdater:

    def __init__(self, frame_endpoint: str, *, with_timing: bool = True):
        self._frame_endpoint: str = frame_endpoint
        self._with_timing: bool = with_timing

    def get_next_image(self) -> ImageType:
        start_ts: float = default_timer()
        resp: Response = requests.get(self._frame_endpoint)
        response_ts: float = default_timer()
        img: ImageType = pil_from_bytes(resp.content)
        img_converted_ts: float = default_timer()
        if self._with_timing:
            msg: str = _get_image_updater_timing_log_line(
                ImageUpdaterTimes(response=response_ts - start_ts,
                                  img_conversion=img_converted_ts - response_ts)
            )
            print(msg)
        return img


def run_demo():

    img_updater: ImageUpdater = ImageUpdater("http://localhost:8081/frame")

    with Blocks() as demo: # type: Blocks

        timer: Timer = Timer(1.0)

        with Row():
            with Column(scale=3):
                with Accordion(label="Live updates", open=True):
                    live_updates_html: HTML = HTML(
                        value="<p><em>Live updates</em></p>",
                        # every=timer
                    )
                with Accordion(label="Historical statistics", open=False):
                    hist_stats_text: HTML = HTML(
                        "<p><em>Historical statistics</em></p>"
                    )
            with Column(scale=5):
                canvas: Image = Image(
                    value=img_updater.get_next_image,
                    every=timer,
                    interactive=False,
                    label="Current frame"
                )

        demo.load(_display_speeds_disclaimer)

    demo.queue().launch(theme=gr.themes.Base(),
                        server_name="localhost", server_port=8091)

if __name__ == "__main__":
    run_demo()
