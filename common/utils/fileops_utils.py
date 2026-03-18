from datetime import datetime
from pathlib import Path

from statx import _Statx, statx

def get_file_creation_time(filepath: str) -> datetime:
    # TODO: Implement a cross-platform implementation (this one is Linux-specific).
    stats: _Statx = statx(filepath)
    btime: float = stats.btime
    timestamp: datetime = datetime.fromtimestamp(btime)
    return timestamp

def resolve_rel_path(rel_path: Path, rel_to: Path) -> Path:
    """
    Resolves `resource_id` to an absolute path by resolving relative to `rel_to`.

    Notes:
    - In this project: used for resolving relative paths from configs relative to ASSETS_DIR.
      Generalised for use with other targets, hence the `rel_to` argument.
    """
    if rel_path.is_absolute():
        raise ValueError("resource_id must be a relative path")
    abs_path: Path = rel_to / rel_path
    return abs_path