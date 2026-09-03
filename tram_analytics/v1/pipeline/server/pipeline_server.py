import logging
from contextlib import asynccontextmanager
from multiprocessing import Queue
from threading import Thread
from typing import AsyncGenerator, TypedDict, Dict, Any

from common.settings.cpu_settings import configure_cpu_inference_runtime

configure_cpu_inference_runtime()
from common.settings.constants import PIPELINE_SERVER_HOST, PIPELINE_SERVER_PORT
from tram_analytics.v1.pipeline.server.helpers.pipeline_cache import FrameNotFoundException, PipelineCache
from tram_analytics.v1.pipeline.server.worker.worker import PipelineQueue, PipelineWrapper, _buffer_to_cache_worker
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts

import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException, status
from classy_fastapi import Routable, get

PIPELINE_CACHE_MAX_LEN: int = 50

class AppState(TypedDict):
    # To be used as the lifespan state for the main application.
    cache: PipelineCache

class APIRoutes(Routable):

    @get("/latest", response_model=PipelineArtefacts)
    async def get_latest_state(self, request: Request) -> PipelineArtefacts:
        cache: PipelineCache = request.state.cache
        state: PipelineArtefacts = cache.get_latest_artefacts()
        return state

    _IMAGE_ENDPOINT_RESPONSES: Dict[int | str, Dict[str, Any]] | None = {
        status.HTTP_200_OK: {
            "content": {"image/jpeg": {}},
            "description": "Return the annotated image"
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Image with the specified frame ID was not found"
        }
    }

    @get("/image/{frame_id}",
         responses=_IMAGE_ENDPOINT_RESPONSES,
         # specify the response class explicitly: needed for correct routing
         # (without it, the OpenAPI docs additionally list
         # an `application/json` option for the 200 response,
         # which is not what it returns)
         response_class=Response
    )
    async def get_image(self, frame_id: str, request: Request) -> Response:
        try:
            cache: PipelineCache = request.state.cache
            image: bytes = cache.get_image_by_id(frame_id)
            return Response(content=image,
                            media_type="image/jpeg")
        except FrameNotFoundException as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Image with ID {frame_id} not found"
            ) from e

def _get_app(buffer: PipelineQueue) -> FastAPI:
    """
    A factory for the FastAPI app.
    """

    cache: PipelineCache = PipelineCache(max_len=PIPELINE_CACHE_MAX_LEN)

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI) -> AsyncGenerator[AppState]:
        # TODO: change from a daemon thread to a shutdownable one
        buffer_to_cache_worker: Thread = Thread(
            target=_buffer_to_cache_worker,
            args=(buffer, cache),
            daemon=True
        )
        buffer_to_cache_worker.start()

        # put the cache in a lifespan state to be accessed by sub-applications
        state: AppState = AppState(cache=cache)
        yield state

    api_subapp: FastAPI = FastAPI()
    api_routes: APIRoutes = APIRoutes()
    api_subapp.include_router(api_routes.router)

    wrapper_app: FastAPI = FastAPI(lifespan=lifespan)
    wrapper_app.mount("/api", api_subapp)

    return wrapper_app

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
