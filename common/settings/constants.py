from typing import Literal, TypeAlias, TypeIs
from pathlib import Path

from common.utils.envvar import load_envvar_as_str, load_envvar_as_int
from common.utils.logging_utils.log_levels import LoggingLevelSetting, get_logging_level_from_setting

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

# The global logging level setting
def _load_logging_level() -> int:
    as_str: str = load_envvar_as_str("LOGGING_LEVEL")
    try:
        # load as enum
        as_level_setting: LoggingLevelSetting = LoggingLevelSetting(as_str)
        # load as int
        level: int = get_logging_level_from_setting(as_level_setting)
        return level
    except ValueError as exc:
        raise ValueError(f"Invalid value as LOGGING_LEVEL: {as_str}") from exc

LOGGING_LEVEL: int = _load_logging_level()

def _load_asset_file(env_var: str, *, check_exists: bool) -> Path:
    """
    Loads the relative path from environment variables by the specified key,
    resolves relative to ASSETS_DIR, and returns the absolute path.
    :param env_var: environment variable name
    :param check_exists: whether to check that the resolved path exists
    """
    as_str: str = load_envvar_as_str(env_var)
    as_rel_path: Path = Path(as_str)
    if as_rel_path.is_absolute():
        raise ValueError(f"env_var must specify a relative path, got absolute: {as_rel_path}")
    # resolve relative to ASSETS_DIR
    as_abs_path: Path = (ASSETS_DIR / as_rel_path).resolve()
    if check_exists and not as_abs_path.exists():
        raise RuntimeError(f"Path does not exist: {as_abs_path}")
    return as_abs_path

PIPELINE_CONFIG: Path = _load_asset_file("PIPELINE_CONFIG", check_exists=True)
DASHBOARD_CONFIG: Path = _load_asset_file("DASHBOARD_CONFIG", check_exists=True)
LIVE_STATE_RENDERER_CONFIG: Path = _load_asset_file("LIVE_STATE_RENDERER_CONFIG", check_exists=True)
