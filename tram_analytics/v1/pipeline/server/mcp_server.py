from fastmcp import FastMCP, Context
from fastmcp.tools import tool

from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.pipeline.server.helpers.pipeline_cache import PipelineCache

MCP_SERVER_NAME: str = "TramAnalyticsServer"

@tool(
    name="get_latest_tram_monitoring_state",
    description="Get the latest state dump from the tram monitoring platform.",
    title="get_latest_tram_monitoring_state"
)
async def get_latest_state(ctx: Context) -> PipelineArtefacts:
    cache: PipelineCache = ctx.lifespan_context["cache"]
    state: PipelineArtefacts = cache.get_latest_artefacts()
    return state
