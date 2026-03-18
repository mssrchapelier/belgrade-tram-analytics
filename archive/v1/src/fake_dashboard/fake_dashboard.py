from typing import Generator

import gradio as gr
from gradio import Blocks, Row, Column, Accordion, HTML, Image, Timer

from archive.v1.src.fake_dashboard.fake_response_generator import FakeTram, FakeResponse, _fake_response_generator

FAKE_UPDATES_HTML_TEMPLATE: str = """
<p>Number of trams: {num_trams}<p>
<ul>{trams_items}</ul>
"""
TRAMS_ITEM_HTML_TEMPLATE: str = """
<li>ID: {tram_id}, speed: {speed}</li>
"""

def _display_speeds_disclaimer():
    gr.Info(message="NOTE: All speeds are approximations from a single camera.",
            duration=None)

class FakeUpdater:

    def __init__(self):
        self._gen: Generator[FakeResponse] = _fake_response_generator()

    @staticmethod
    def _build_html(response: FakeResponse) -> str:
        list_items: str = "".join(
            TRAMS_ITEM_HTML_TEMPLATE.format(
                tram_id=tram.tram_id[:6],
                speed=round(tram.speed)
            )
            for tram in response.trams # type: FakeTram
        )
        html: str = FAKE_UPDATES_HTML_TEMPLATE.format(
            num_trams=response.num_trams,
            trams_items=list_items
        )
        return html

    def get_next_html(self) -> str:
        response: FakeResponse = next(self._gen)
        html: str = self._build_html(response)
        return html


def run_demo():

    updater: FakeUpdater = FakeUpdater()

    with Blocks() as demo: # type: Blocks

        timer: Timer = Timer(1.0)

        with Row():
            with Column(scale=3):
                with Accordion(label="Live updates", open=True):
                    live_updates_html: HTML = HTML(value=updater.get_next_html,
                                                   every=timer)
                with Accordion(label="Historical statistics", open=False):
                    hist_stats_text: HTML = HTML("<p><em>Historical statistics</em></p>")
            with Column(scale=5):
                canvas: Image = Image(interactive=False,
                                      label="Current frame")
        demo.load(_display_speeds_disclaimer())

    demo.queue().launch(theme=gr.themes.Base())


if __name__ == "__main__":
    run_demo()