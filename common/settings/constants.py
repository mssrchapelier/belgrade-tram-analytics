from typing import Literal, TypeAlias, TypeIs
from pathlib import Path

from common.utils.envvar import load_envvar_as_str, load_envvar_as_int

# --- Load runtime configuration from environment variables ---

# IMPORTANT: Prior to importing the following variables,
# make sure the underlying ones are available in the environment
# (locally: e. g. by loading from `.env`; Docker: by defining in a Compose file)

# The TCP server collecting log records from all processes
LOGGING_SERVER_HOST: str = load_envvar_as_str("LOGGING_SERVER_HOST")
LOGGING_SERVER_PORT: int = load_envvar_as_int("LOGGING_SERVER_PORT")

# The pipeline server
PIPELINE_SERVER_HOST: str = load_envvar_as_str("PIPELINE_SERVER_HOST")
PIPELINE_SERVER_PORT: int = load_envvar_as_int("PIPELINE_SERVER_PORT")

# The dashboard server
DASHBOARD_HOST: str = load_envvar_as_str("DASHBOARD_HOST")
DASHBOARD_PORT: int = load_envvar_as_int("DASHBOARD_PORT")
# The number of threads for a ThreadPoolExecutor
# to which to offload the rendering of the Jinja2 templates for the main HTML dashboard,
# to avoid blocking the main thread in the Gradio dashboard server.
MAIN_DASHBOARD_RENDERING_MAX_THREADS = load_envvar_as_int("MAIN_DASHBOARD_RENDERING_MAX_THREADS")

# Multiprocessing start method
MpStartMethod: TypeAlias = Literal["fork", "forkserver", "spawn"]

def is_mp_start_method(method: str) -> TypeIs[MpStartMethod]:
    return method in {"fork", "forkserver", "spawn"}

def ensure_is_mp_start_method(method: str) -> MpStartMethod:
    if not is_mp_start_method(method):
        raise ValueError(f"Invalid multiprocessing start method: {method}")
    return method

# NOTE:
# Set to `forkserver` to prevent signal handler conflicts inside the pipeline worker process
# (which did happen at times with `fork` due to FastAPI's own handlers being injected).
# It is possible for `forkserver` to still cause conflicts -- set to `spawn` in that case
# (potentially slower startup, but inherits less of the parent process's state).
MULTIPROCESSING_START_METHOD: MpStartMethod = ensure_is_mp_start_method(
    load_envvar_as_str("MULTIPROCESSING_START_METHOD")
)

def _load_assets_dir() -> Path:
    as_str: str = load_envvar_as_str("ASSETS_DIR")
    as_path: Path = Path(as_str).resolve()
    if not (as_path.exists() and as_path.is_dir()):
        raise RuntimeError(f"Path does not exist or is not a directory: {as_path}")
    return as_path

ASSETS_DIR: Path = _load_assets_dir()
