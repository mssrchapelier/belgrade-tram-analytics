from typing import override
from pathlib import Path
import asyncio
from multiprocessing import Process
from abc import ABC, abstractmethod
import logging

from common.utils.logging_utils.logging_utils import configure_global_logging, configure_global_logging_to_tcp_socket
from tram_analytics.v1.pipeline.server.pipeline_server import run_pipeline_server
from tram_analytics.v1.dashboard.dashboard import async_run_dashboard
from common.utils.logging_utils.logging_server import run_logging_server
from common.settings.constants import (
    LOGGING_SERVER_HOST, LOGGING_SERVER_PORT, LOGGING_LEVEL,
    PIPELINE_CONFIG, DASHBOARD_CONFIG, LIVE_STATE_RENDERER_CONFIG
)

# child processes for components

class BaseProcessWithTCPLogging(Process, ABC):

    """
    A helper process that sets logging to the defined TCP socker.
    """

    @abstractmethod
    def _run_impl(self) -> None:
        pass

    @override
    def run(self) -> None:
        configure_global_logging_to_tcp_socket(
            host=LOGGING_SERVER_HOST,
            port=LOGGING_SERVER_PORT,
            level=LOGGING_LEVEL
        )
        self._run_impl()


class PipelineServerProcessRunner(BaseProcessWithTCPLogging):

    def __init__(self, pipeline_config_path: Path, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pipeline_config_path: Path = pipeline_config_path

    @override
    def _run_impl(self) -> None:
        run_pipeline_server(str(self._pipeline_config_path))

class DashboardProcessRunner(BaseProcessWithTCPLogging):

    def __init__(self, *, dashboard_config_path: Path,
                 live_state_renderer_config_path: Path,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self._dashboard_config_path: Path = dashboard_config_path
        self._live_state_renderer_config_path: Path = live_state_renderer_config_path

    @override
    def _run_impl(self) -> None:
        asyncio.run(
            async_run_dashboard(dashboard_config_path=self._dashboard_config_path,
                                live_state_renderer_config_path=self._live_state_renderer_config_path)
        )

class LoggingServerProcessRunner(Process):

    @override
    def run(self) -> None:
        configure_global_logging(level=logging.DEBUG)
        run_logging_server()


def run_joint(
        *, pipeline_config_path: Path,
        dashboard_config_path: Path,
        live_state_renderer_config_path: Path,
) -> None:
    """
    Run the following configuration:
    (1) pipeline and API server -- in a child process logging to the defined TCP socket;
    (2) dashboard -- likewise;
    (3) logging server -- in a child process; consuming from the socket and logging everything to stderr.
    """
    logging_server_runner: LoggingServerProcessRunner = LoggingServerProcessRunner()
    dashboard_runner: DashboardProcessRunner = DashboardProcessRunner(
        dashboard_config_path=dashboard_config_path,
        live_state_renderer_config_path=live_state_renderer_config_path
    )
    pipeline_runner: PipelineServerProcessRunner = PipelineServerProcessRunner(
        pipeline_config_path
    )

    # start everything
    logging_server_runner.start()
    dashboard_runner.start()
    pipeline_runner.start()

    # Wait for completion. In practice, only the pipeline runner
    # is expected join on its own if the stream ends;
    # the dashboard and logging server should only join on errors.
    # The normal stopping procedure is through a keyboard interrupt
    # which will be propagated to these child processes.
    pipeline_runner.join()
    dashboard_runner.join()
    logging_server_runner.join()

def launch():
    """
    A wrapper around `run_joint(...)` that loads the arguments from environment variables.
    """
    run_joint(
        pipeline_config_path=PIPELINE_CONFIG,
        dashboard_config_path=DASHBOARD_CONFIG,
        live_state_renderer_config_path=LIVE_STATE_RENDERER_CONFIG
    )
