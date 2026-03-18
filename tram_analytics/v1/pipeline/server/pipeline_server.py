import logging
from contextlib import asynccontextmanager
from multiprocessing import Queue
from threading import Thread
from typing import AsyncGenerator

from common.settings.cpu_settings import configure_cpu_inference_runtime

configure_cpu_inference_runtime()
from common.settings.constants import PIPELINE_SERVER_HOST, PIPELINE_SERVER_PORT
from tram_analytics.v1.pipeline.server.helpers.pipeline_cache import StaleReferenceException, PipelineCache
from tram_analytics.v1.pipeline.server.worker.worker import PipelineQueue, PipelineWrapper, _buffer_to_cache_worker
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts

import uvicorn
from fastapi import FastAPI, Response
from classy_fastapi import Routable, get

PIPELINE_CACHE_MAX_LEN: int = 50

class AppRoutes(Routable):

    def __init__(self, cache: PipelineCache):
        super().__init__()
        self._cache: PipelineCache = cache

    @get("/latest")
    async def get_latest_state(self) -> PipelineArtefacts:
        state: PipelineArtefacts = self._cache.get_latest_artefacts()
        return state

    @get("/image/{frame_id}")
    async def get_image(self, frame_id: str) -> Response | None:
        try:
            image: bytes = self._cache.get_image_by_id(frame_id)
            return Response(content=image,
                            media_type="image/jpeg")
        except StaleReferenceException as e:
            raise RuntimeError(f"Could not retrieve frame {frame_id}") from e


def _get_app(buffer: PipelineQueue) -> FastAPI:
    """
    A factory for the FastAPI app.
    """

    cache: PipelineCache = PipelineCache(max_len=PIPELINE_CACHE_MAX_LEN)

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI) -> AsyncGenerator[None]:
        # TODO: change from a daemon thread to a shutdownable one
        buffer_to_cache_worker: Thread = Thread(
            target=_buffer_to_cache_worker,
            args=(buffer, cache),
            daemon=True
        )
        buffer_to_cache_worker.start()
        yield

    app: FastAPI = FastAPI(
        lifespan=lifespan
    )
    routes: AppRoutes = AppRoutes(cache)
    app.include_router(routes.router)
    return app


def _build_pipeline_wrapper(*, config_path: str,
                            buffer: PipelineQueue):
    pipeline_wrapper: PipelineWrapper = PipelineWrapper(
        buffer=buffer, config_path=config_path
    )
    return pipeline_wrapper

def run_pipeline_server(pipeline_config_path: str):
    buffer: PipelineQueue = Queue()
    try:
        pipeline: PipelineWrapper = _build_pipeline_wrapper(
            config_path=pipeline_config_path,
            buffer=buffer
        )
        app: FastAPI = _get_app(buffer)
        with pipeline:
            logging.info("Starting the pipeline")
            uvicorn.run(app=app,
                        host=PIPELINE_SERVER_HOST,
                        port=PIPELINE_SERVER_PORT)
    finally:
        logging.info("Stopped the pipeline")
        buffer.close()
