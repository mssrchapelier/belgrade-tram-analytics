from typing import Callable
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool
from multiprocessing.pool import Pool as PoolType
from functools import partial

from scripts.constants import TIMESTAMP_FORMAT
from scripts.utils import worker_filerenamer

"""
dataset directory structure

YYYY_MM_DD
|
+-- HH_MM (hour start)
    |
    +-- HH_00
    +-- HH_01_val
    +-- HH_07
    +-- HH_08_test
    +-- HH_14
    +-- HH_15_train
"""

def path_to_datetime(filepath: Path) -> datetime:
    return datetime.strptime(filepath.stem, TIMESTAMP_FORMAT)

def _get_hour_subdir_name(time_obj: datetime) -> str:
    minute: int = time_obj.minute
    if minute >= 15:
        name_suffix = "15_train"
    elif minute >= 14:
        name_suffix = "14"
    elif minute >= 8:
        name_suffix = "08_test"
    elif minute >= 7:
        name_suffix = "07"
    elif minute >= 1:
        name_suffix = "01_val"
    else:
        name_suffix = "00"
    name: str = f"{time_obj.hour:02d}_{name_suffix}"
    return name

def _get_new_dir_rel_path(filename_stem: str) -> str:
    # example: 20251107_174027 -> "2025_11_07/17_00/17_15_train"
    time_obj: datetime = datetime.strptime(filename_stem, TIMESTAMP_FORMAT)
    # 2025_11_07
    date_dir_name: str = time_obj.strftime("%Y_%m_%d")
    # 17_00
    hour_dir_name: str = "{}_00".format(time_obj.strftime("%H"))
    # 17_15_train
    hour_subdir_name: str = _get_hour_subdir_name(time_obj)
    dest_dir_rel_path: str = "/".join([date_dir_name, hour_dir_name, hour_subdir_name])
    return dest_dir_rel_path

def _old_to_new_path(old_path: Path, *, new_dir: Path) -> Path:
    filename: str = old_path.name
    filename_stem: str = old_path.stem
    new_dir_rel_path: str = _get_new_dir_rel_path(filename_stem)
    new_path: Path = new_dir.joinpath(new_dir_rel_path, filename)
    return new_path

def _worker_getparent(filepath: Path) -> Path:
    return filepath.parent

def _worker_mkdir(dirpath: Path) -> None:
    dirpath.mkdir(parents=True)

def rearrange_dir(old_dir: Path, new_dir: Path) -> None:

    new_dir.mkdir(parents=True, exist_ok=True)
    old_paths: list[Path] = list(old_dir.iterdir())
    func: Callable[[Path], Path] = partial(_old_to_new_path, new_dir=new_dir)
    with Pool() as pool: # type: PoolType
        # build dest paths
        dest_paths: list[Path] = pool.map(func, old_paths)
        # get their parents' paths
        dest_dirs: set[Path] = set(pool.map(_worker_getparent, dest_paths))
        # create the parent dirs
        pool.map(_worker_mkdir, dest_dirs)
        # move files
        pool.map(worker_filerenamer, zip(old_paths, dest_paths))
    print("done")

def build_dir_manifest(root_dir: Path, dest_txt_path: Path) -> None:
    with dest_txt_path.open(mode="w", encoding="utf8") as fout:
        for date_dir in sorted([path for path in root_dir.iterdir() if path.is_dir()],
                               key=lambda p: p.name): # type: Path
            # ## YYYY_MM_DD
            date_dir_name: str = date_dir.name
            fout.write(f"## {date_dir_name}\n\n")
            for hour_dir in sorted([path for path in date_dir.iterdir() if path.is_dir()],
                                   key=lambda p: p.name): # type: Path
                # ### HH_MM
                hour_dir_name: str = hour_dir.name
                fout.write(f"### {hour_dir_name}\n\n")
                for hour_subdir in sorted([path for path in hour_dir.iterdir() if path.is_dir()],
                                          key=lambda p: p.name): # type: Path
                    # HH_08_test
                    hour_subdir_name: str = hour_subdir.name
                    fout.write(f"{hour_subdir_name}\n")
                fout.write("\n")
    print("done")
