"""
Sets of colours for tracks (lines, markers, track ID text for the bounding box),
to randomly choose from for each new track ID.
"""

import re
from pathlib import Path
from re import Pattern, Match
from typing import List, Tuple
from warnings import deprecated

from pydantic import BaseModel, RootModel
from pydantic_yaml import parse_yaml_file_as

from common.utils.custom_types import ColorTuple


class LineColorPaletteItem(BaseModel):
    line: ColorTuple
    marker: ColorTuple

class TrackLineMarkerColorPalette(BaseModel):
    confirmed_matched: LineColorPaletteItem
    confirmed_unmatched: LineColorPaletteItem
    unconfirmed_matched: LineColorPaletteItem
    unconfirmed_unmatched: LineColorPaletteItem

class TrackColourPaletteItem(BaseModel):
    lines_markers: TrackLineMarkerColorPalette

    # for the bounding box
    trackid_bg_color: ColorTuple
    trackid_text_color: ColorTuple

class TrackColourPalette(RootModel[List[TrackColourPaletteItem]]):
    pass

@deprecated(
    "Deprecated; put TrackColourPaletteItem descriptions into a single YAML file as a list "
    + "and parse a TrackColourPalette directly with parse_yaml_file_as() instead."
)
def load_palette_from_dir(configs_dir: str) -> TrackColourPalette:
    """
    Loads palette items from a directory.
    The files must be named as `colors_N.yaml`.
    The order of items in the returned list corresponds
    to the ascending order of the indices `N` in the filenames.
    """
    name_pattern: Pattern[str] = re.compile(r"^colors_(?P<idx>\d+)\.yaml$")

    # list files in the directory
    dir_as_path: Path = Path(configs_dir)
    filepaths: List[Path] = [filepath for filepath in Path.iterdir(dir_as_path)]

    # [ (index, filepath), ... ]
    idx_path_tuples: List[Tuple[int, Path]] = []

    # read indices from paths
    for filepath in filepaths: # type: Path
        name: str = filepath.name
        match: Match[str] | None = name_pattern.match(name)
        if match is None:
            raise ValueError(f"File names must be of form: colors_N.yaml, got: {name}")
        idx: int = int(match.group("idx"))
        idx_path_tuples.append((idx, filepath))
    # sort the list by index
    idx_path_tuples.sort(key=lambda t: t[0])
    # build palette items
    palette_items = [
        parse_yaml_file_as(TrackColourPaletteItem, filepath)
        for idx, filepath in idx_path_tuples
    ]
    palette: TrackColourPalette = TrackColourPalette.model_validate(palette_items)
    return palette
