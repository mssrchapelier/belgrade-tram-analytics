import logging
from contextlib import asynccontextmanager
from multiprocessing import Queue
from threading import Thread
from typing import AsyncGenerator, AsyncIterator, TypedDict, Dict, Any

from common.settings.cpu_settings import configure_cpu_inference_runtime
configure_cpu_inference_runtime()
from common.utils.asgi_utils import make_partial_combined_lifespan
from common.settings.constants import PIPELINE_SERVER_HOST, PIPELINE_SERVER_PORT
from tram_analytics.v1.pipeline.server.helpers.pipeline_cache import FrameNotFoundException, PipelineCache
from tram_analytics.v1.pipeline.server.worker.worker import PipelineQueue, PipelineWrapper, _buffer_to_cache_worker
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.pipeline.server.mcp_server import MCP_SERVER_NAME, get_latest_state as mcp_get_latest_state

import uvicorn
from starlette.applications import Starlette
from fastapi import FastAPI, Request, Response, HTTPException, status
from classy_fastapi import Routable, get
from fastmcp import FastMCP
from fastmcp.server.lifespan import lifespan as fastmcp_lifespan

PIPELINE_CACHE_MAX_LEN: int = 50

class AppState(TypedDict):
    # To be used as the lifespan state for the main application.
    cache: PipelineCache

class APIRoutes(Routable):

    @get("/latest", response_model=PipelineArtefacts)
    async def _get_latest_state(self, request: Request) -> PipelineArtefacts:
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

def _get_app(cache: PipelineCache) -> FastAPI:
    """
    A factory for the FastAPI app.
    """

    @asynccontextmanager
    async def _wrapper_app_lifespan(fastapi_app: Starlette) -> AsyncGenerator[AppState]:
        # put the cache in a lifespan state to be accessed by sub-applications
        state: AppState = AppState(cache=cache)
        yield state

    @fastmcp_lifespan
    async def _mcp_bareapp_lifespan(app: FastMCP) -> AsyncIterator[Dict[str, Any]]:
        yield {"cache": cache}

    api_subapp: FastAPI = FastAPI()
    api_routes: APIRoutes = APIRoutes()
    api_subapp.include_router(api_routes.router)

    mcp_subapp_bare: FastMCP = FastMCP(
        name=MCP_SERVER_NAME,
        lifespan=_mcp_bareapp_lifespan
    )
    mcp_subapp_bare.add_tool(mcp_get_latest_state)

    mcp_subapp_asgi: Starlette = mcp_subapp_bare.http_app("/")

    wrapper_app: FastAPI = FastAPI(
        # combine own lifespan of the wrapper application
        # and those of the sub-applications
        lifespan=make_partial_combined_lifespan(own_lifespan=_wrapper_app_lifespan)
    )
    wrapper_app.mount("/api", api_subapp)
    wrapper_app.mount("/mcp", mcp_subapp_asgi)

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
        cache: PipelineCache = PipelineCache(max_len=PIPELINE_CACHE_MAX_LEN)
        buffer_to_cache_worker: Thread = Thread(
            target=_buffer_to_cache_worker,
            args=(buffer, cache),
            daemon=True
        )
        buffer_to_cache_worker.start()
        
        pipeline: PipelineWrapper = _build_pipeline_wrapper(
            config_path=pipeline_config_path,
            buffer=buffer
        )
        
        app: FastAPI = _get_app(cache)
        with pipeline:
            logging.info("Starting the pipeline")
            uvicorn.run(app=app,
                        host=PIPELINE_SERVER_HOST,
                        port=PIPELINE_SERVER_PORT)
    finally:
        logging.info("Stopped the pipeline")
        buffer.close()
