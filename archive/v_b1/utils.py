from datetime import datetime
from uuid import uuid4

from statx import statx, _Statx

def get_file_creation_time(filepath: str) -> datetime:
    # TODO: Implement a cross-platform implementation (this one is Linux-specific).
    stats: _Statx = statx(filepath)
    btime: float = stats.btime
    timestamp: datetime = datetime.fromtimestamp(btime)
    return timestamp

def get_uuid() -> str:
    return uuid4().hex