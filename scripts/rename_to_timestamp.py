from datetime import datetime
from pathlib import Path
from multiprocessing import Pool
from multiprocessing.pool import Pool as PoolType

from scripts.constants import TIMESTAMP_FORMAT
from scripts.utils import worker_filerenamer


def get_creation_timestamp(file: Path) -> datetime:
    # Note: Time of most recent content modification expressed in seconds
    # See: https://docs.python.org/3/library/os.html#os.stat_result
    return datetime.fromtimestamp(file.stat().st_mtime)

def _get_new_path(src_path: Path) -> Path:
    """
    Changes the stem to the modification time timestamp, preserving any suffixes.
    """
    suffixes_concatenated: str = "".join(src_path.suffixes)
    timestamp: str = get_creation_timestamp(src_path).strftime(TIMESTAMP_FORMAT)
    dest_path: Path = src_path.parent.joinpath(f"{timestamp}{suffixes_concatenated}")
    return dest_path


def rename_all_in_dir(dir_path: Path) -> None:
    """
    Rename all files in the directory, changing their stem to the timestamps representing their modification time.
    Extensions are preserved.
    """

    src_paths: list[Path] = list(dir_path.iterdir())
    with Pool() as pool: # type: PoolType
        dest_paths: list[Path] = pool.map(_get_new_path, src_paths)
        pool.map(worker_filerenamer, zip(src_paths, dest_paths))
    print("done")
