from datetime import datetime
from pathlib import Path

from jinja2 import Environment, Template, FileSystemLoader

# data model
from tram_analytics.v1.dashboard.models.live_state import LiveStateForRender
# config
from tram_analytics.v1.dashboard.render.config import LiveStateRendererConfig
# filters
from tram_analytics.v1.dashboard.render.filters import FILTERS


class LiveStateRenderer:

    def __init__(self, config: LiveStateRendererConfig):
        self._root_dir: Path = Path(__file__).resolve().parent
        # templates
        self._template_dir: Path = self._root_dir / "templates"

        # initialise template engine
        self._loader: FileSystemLoader = FileSystemLoader(self._template_dir)
        self._env: Environment = Environment(loader=self._loader,
                                             trim_blocks=True,
                                             lstrip_blocks=True)
        # register filters
        self._env.filters.update(FILTERS)
        # initialise template
        self._template: Template = self._env.get_template("live_state.html")
        # init render config
        self._config: LiveStateRendererConfig = config

    def render(self, dto: LiveStateForRender,
               *, ref_ts: datetime) -> str:
        """
        Renders the `LiveStateForRender` instance using Jinja2 templates
        and returns the rendered HTML.

        :param dto: the DTO to be rendered (a `LiveStateForRender` instance)
        :param ref_ts: the timestamp in reference to which to display elapsed durations
          (i. e. "20 s ago").
        :return: the rendered HTML
        """
        rendered: str = self._template.render({"data": dto,
                                               "config": self._config,
                                               "ref_ts": ref_ts})
        return rendered
