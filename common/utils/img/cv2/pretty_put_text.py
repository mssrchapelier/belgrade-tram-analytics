"""
OVERVIEW:

All of the `anchor_...` functions allow the user to put a text box on a NumPy image in a certain way:
with the specified corner (`which`) of the box (`tl`, `tr`, `bl`, `br`) located at the specified `offset`
from the specified `anchor` point.

Functions:
- `anchor_text()`: the usual OpenCV text (a wrapper around `cv2.putText()`);
- `anchor_line_with_bg()`: a text line on a solid-coloured background,
  with the specified `padding` around the text;
- `anchor_lines_with_bg()`: multiple such lines, each on its own background,
  with a vertical spacing of `line_spacing` between the line boxes (i. e. including the background);
- `anchor_lines_on_rect_overlay`: multiple lines of text, separated by a vertical spacing of `line_spacing`,
  drawn on a solid-coloured rectangle with the specified `padding` around the entire text.
"""

from typing import Tuple, Literal, TypeAlias, Set, List, Sequence, TypeIs
from warnings import deprecated

import cv2
from numpy import uint8
from numpy.typing import NDArray

from common.utils.custom_types import PixelPosition, ColorTuple

Corner: TypeAlias = Literal["tl", "tr", "bl", "br"]

DEFAULT_FONT_FACE: int = cv2.FONT_HERSHEY_PLAIN

def _is_valid_text_size(ret: Sequence[int]) -> TypeIs[Tuple[int, int]]:
    return len(ret) == 2 and isinstance(ret[0], int) and isinstance(ret[1], int)

def _get_text_size(*args, **kwargs) -> Tuple[int, int]:
    """
    Wrapper for cv2.getTextSize(). Returns just the `(width, height)` tuple, without the baseline.
    """
    text_size, baseline = cv2.getTextSize(*args, **kwargs) # type: Sequence[int], int
    if not _is_valid_text_size(text_size):
        raise ValueError(f"Got invalid text size: {str(text_size)}")
    return text_size

def _get_text_size_with_bg(text: str, *,
                           padding: Tuple[int, int],  # horizontal, vertical
                           # cv2 arguments
                           font_face: int,
                           font_scale: float,
                           thickness: int) -> Tuple[int, int]:
    """
    Calculate the width and height of the box
    to be drawn by `write_line_with_bg()`.
    """
    padding_hor, padding_vert = padding  # type: int, int
    # get the width and height of the text line
    text_size: Tuple[int, int] = _get_text_size(text, font_face, font_scale, thickness)
    text_w, text_h = text_size  # type: int, int
    # the width and height of the background
    bg_w: int = text_w + 2 * padding_hor
    bg_h: int = text_h + 2 * padding_vert
    return bg_w, bg_h

def _get_text_size_lines_with_bg(text_lines: List[str], *,
                                 line_spacing: int,
                                 padding: Tuple[int, int],  # horizontal, vertical
                                 # cv2 arguments
                                 font_face: int,
                                 font_scale: float,
                                 thickness: int
                                 ) -> Tuple[int, int]:
    """
    Calculate the width and height of the box (the one containing all text lines)
    to be drawn by `write_lines_with_bg()`.
    """
    padding_hor, padding_vert = padding  # type: int, int
    num_lines: int = len(text_lines)

    # calculate the maximum width and the height of one line (without the background)
    line_text_widths: Set[int] = set()
    line_text_heights: Set[int] = set()
    for line in text_lines: # type: str
        line_text_size: Tuple[int, int] = _get_text_size(
            line, font_face, font_scale, thickness
        )
        line_text_w, line_text_h = line_text_size # type: int, int
        line_text_widths.add(line_text_w)
        line_text_heights.add(line_text_h)
    assert len(line_text_heights) == 1
    line_text_h = line_text_heights.pop()
    line_text_w = max(line_text_widths)
    box_w: int = line_text_w + 2 * padding_hor
    bg_line_h: int = line_text_h + 2 * padding_vert
    box_h: int = bg_line_h * num_lines + line_spacing * (num_lines - 1)
    return box_w, box_h

def _get_text_size_lines_on_rect_overlay(text_lines: List[str], *,
                                         line_spacing: int,
                                         padding: Tuple[int, int],  # horizontal, vertical
                                         # cv2 arguments
                                         font_face: int,
                                         font_scale: float,
                                         thickness: int
                                         ) -> Tuple[int, int]:
    """
    Calculate the width and height of the box (the one containing all text lines)
    to be drawn by `write_lines_on_rect_overlay()`.
    """
    padding_hor, padding_vert = padding  # type: int, int
    num_lines: int = len(text_lines)

    # calculate the maximum width and the height of one line (without the background)
    line_text_widths: Set[int] = set()
    line_text_heights: Set[int] = set()
    for line in text_lines: # type: str
        line_text_size: Tuple[int, int] = _get_text_size(
            line, font_face, font_scale, thickness
        )
        line_text_w, line_text_h = line_text_size # type: int, int
        line_text_widths.add(line_text_w)
        line_text_heights.add(line_text_h)
    assert len(line_text_heights) == 1
    line_text_h = line_text_heights.pop()
    line_text_w = max(line_text_widths)
    box_w: int = line_text_w + 2 * padding_hor
    box_h: int = line_text_h * num_lines + line_spacing * (num_lines - 1) + 2 * padding_vert
    return box_w, box_h

def _get_anchored_container_xyxy_coords(*, anchor: PixelPosition,
                                        which: Corner,
                                        offset: Tuple[int, int],
                                        textbox_size: Tuple[int, int]) -> Tuple[int, int, int, int]:
    """
    Calculate the coordinates of the box with its corner at position `which`
    anchored at `anchor` with a margin of `offset`,
    containing the inner textbox with the size of `textbox_size`.

    Returns: `(x1, y1, x2, y2)`.
    """
    anchor_x, anchor_y = anchor  # type: int, int
    offset_x, offset_y = offset  # type: int, int
    textbox_w, textbox_h = textbox_size # type: int, int

    box_x1: int = anchor_x + offset_x if which in ("tl", "bl") else anchor_x - offset_x - textbox_w
    box_x2: int = box_x1 + textbox_w
    box_y1: int = anchor_y + offset_y if which in ("tl", "tr") else anchor_y - offset_y - textbox_h
    box_y2: int = box_y1 + textbox_h

    return box_x1, box_y1, box_x2, box_y2


@deprecated("Use anchor_text_line instead.")
def pretty_put_text(img: NDArray[uint8], text: str,
                    *, offset_from: PixelPosition,
                    offset: Tuple[int, int], # x, y
                    color: ColorTuple,
                    font_face: int,
                    font_scale: float,
                    thickness: int,
                    line_type: int):
    """
    Calls cv2.putText(), offsetting the corner of the text box that is closest to the point `offset_from`
    with respect to `offset_from`.
    """

    offset_from_x, offset_from_y = offset_from # type: int, int
    # the offset of the corner of the text box CLOSEST to offset_from
    offset_x, offset_y = offset # type: int, int
    textbox_w: int
    textbox_h: int
    baseline: int
    (textbox_w, textbox_h), baseline = cv2.getTextSize(text, font_face, font_scale, thickness)
    # the offset of the top-left corner of the text box
    offset_bl_x: int = offset_x if offset_x >= 0 else offset_x - textbox_w
    offset_bl_y: int = offset_y if offset_y <= 0 else offset_y + textbox_h
    # compute the position of the bottom-left corner of the text box
    textbox_bl_x: int = offset_from_x + offset_bl_x
    textbox_bl_y: int = offset_from_y + offset_bl_y
    # call the OpenCV function
    cv2.putText(org=(textbox_bl_x, textbox_bl_y), bottomLeftOrigin=False,
                img=img, text=text,
                # the rest of the arguments are simply passed as is
                color=color, fontFace=font_face, fontScale=font_scale,
                thickness=thickness, lineType=line_type)

def write_line_with_bg(img: NDArray[uint8],
                       text: str, *,
                       # the top left corner of the text line including the background
                       line_pos_tl: PixelPosition,
                       padding: Tuple[int, int] = (0, 0), # horizontal, vertical
                       bg_color: ColorTuple = (0, 0, 0),
                       font_color: ColorTuple = (255, 255, 255),
                       # cv2 arguments
                       font_face: int = DEFAULT_FONT_FACE,
                       font_scale: float = 1.0,
                       thickness: int = 1,
                       line_type: int = cv2.LINE_8
                       ):
    padding_hor, padding_vert = padding # type: int, int
    # the width and height of the background
    bg_w, bg_h = _get_text_size_with_bg(
        text, padding=padding, font_face=font_face, font_scale=font_scale, thickness=thickness
    ) # type: int, int
    # the top left corner of the background
    bg_tl: PixelPosition = line_pos_tl
    # the bottom right corner of the background
    bg_br: PixelPosition = (bg_tl[0] + bg_w, bg_tl[1] + bg_h)
    # the bottom left corner of the text
    text_x1: int = bg_tl[0] + padding_hor
    text_y2: int = bg_br[1] - padding_vert
    text_pos_bl: PixelPosition = (text_x1, text_y2)
    # draw the background
    cv2.rectangle(img, bg_tl, bg_br, bg_color, cv2.FILLED)
    # draw the text
    cv2.putText(img, text, text_pos_bl, font_face, font_scale,
                font_color, thickness, lineType=line_type)


def write_lines_with_bg(img: NDArray[uint8],
                        text_lines: List[str],
                        *, lines_pos_tl: PixelPosition,
                        line_spacing: int = 5,
                        padding: Tuple[int, int] = (0, 0),  # horizontal, vertical
                        bg_color: ColorTuple = (0, 0, 0),
                        font_color: ColorTuple = (255, 255, 255),
                        # cv2 arguments
                        font_face: int = DEFAULT_FONT_FACE,
                        font_scale: float = 1.0,
                        thickness: int = 1,
                        line_type: int = cv2.LINE_8):
    """
    Writes several lines of text, each on its own coloured background line,
    separated by `line_spacing` pixels.
    """
    # calculate the height of one line of text
    bg_line_size: Tuple[int, int] = _get_text_size_with_bg(
        "", padding=padding, font_face=font_face, font_scale=font_scale, thickness=thickness
    )
    bg_line_h: int = bg_line_size[1]
    # the vertical step for the top-left corner of each next line
    vertical_step: int = bg_line_h + line_spacing
    for line_idx, text_line in enumerate(text_lines): # type: int, str
        # the top-left corner of the current line of text
        cur_line_pos_tl: PixelPosition = (
            lines_pos_tl[0],
            lines_pos_tl[1] + vertical_step * line_idx
        )
        write_line_with_bg(
            img, text_line, line_pos_tl=cur_line_pos_tl, padding=padding,
            bg_color=bg_color, font_color=font_color, font_face=font_face,
            font_scale=font_scale, thickness=thickness, line_type=line_type
        )

def write_lines_on_rect_overlay(img: NDArray[uint8],
                                text_lines: List[str],
                                *, overlay_tl: PixelPosition,
                                line_spacing: int = 5,
                                padding: Tuple[int, int] = (0, 0),  # horizontal, vertical
                                bg_color: ColorTuple = (0, 0, 0),
                                font_color: ColorTuple = (255, 255, 255),
                                # cv2 arguments
                                font_face: int = DEFAULT_FONT_FACE,
                                font_scale: float = 1.0,
                                thickness: int = 1,
                                line_type: int = cv2.LINE_8):
    """
    Writes several lines of text, separated by `line_spacing` pixels,
    on a rectangular overlay filled with `bg_color`.
    """
    padding_hor, padding_vert = padding # type: int, int
    overlay_x1, overlay_y1 = overlay_tl # type: int, int
    # calculate the height of one line of text
    line_size: Tuple[int, int] = _get_text_size("", font_face, font_scale, thickness)
    line_h: int = line_size[1]
    # draw the overlay
    overlay_w, overlay_h = _get_text_size_lines_on_rect_overlay(
        text_lines, line_spacing=line_spacing, padding=padding,
        font_face=font_face, font_scale=font_scale, thickness=thickness
    ) # type: int, int
    overlay_br: PixelPosition = (overlay_x1 + overlay_w,
                                 overlay_y1 + overlay_h)
    cv2.rectangle(img, overlay_tl, overlay_br, bg_color, -1)
    # draw the text lines
    for line_idx, text_line in enumerate(text_lines): # type: int, str
        # the top-left corner of the current line of text
        line_x1: int = overlay_x1 + padding_hor
        line_y2: int = overlay_y1 + padding_vert + line_h * (line_idx + 1) + line_spacing * line_idx
        cv2.putText(img, text_line, (line_x1, line_y2), font_face, font_scale,
                    font_color, thickness, lineType=line_type)

def anchor_text(img: NDArray[uint8], text: str,
                *, anchor: PixelPosition,
                which: Corner,
                color: ColorTuple,
                offset: Tuple[int, int] = (0, 0),  # x, y
                font_face: int = DEFAULT_FONT_FACE,
                font_scale: float = 1.0,
                thickness: int = 1,
                line_type: int = cv2.LINE_8):
    text_size: Tuple[int, int] = _get_text_size(text, font_face, font_scale, thickness)
    box_x1, box_y1, box_x2, box_y2 = _get_anchored_container_xyxy_coords(
        anchor=anchor, which=which, offset=offset, textbox_size=text_size
    )
    cv2.putText(org=(box_x1, box_y2), bottomLeftOrigin=False,
                img=img, text=text,
                # the rest of the arguments are simply passed as is
                color=color, fontFace=font_face, fontScale=font_scale,
                thickness=thickness, lineType=line_type)

def anchor_line_with_bg(img: NDArray[uint8],
                        text: str, *,
                        anchor: PixelPosition,
                        which: Corner,
                        offset: Tuple[int, int] = (0, 0),  # x, y
                        padding: Tuple[int, int] = (0, 0),  # horizontal, vertical
                        bg_color: ColorTuple = (0, 0, 0),
                        font_color: ColorTuple = (255, 255, 255),
                        # cv2 arguments
                        font_face: int = DEFAULT_FONT_FACE,
                        font_scale: float = 1.0,
                        thickness: int = 1,
                        line_type: int = cv2.LINE_8):
    line_size: Tuple[int, int] = _get_text_size_with_bg(
        text, padding=padding, font_face=font_face, font_scale=font_scale, thickness=thickness
    )
    box_x1, box_y1, box_x2, box_y2 = _get_anchored_container_xyxy_coords(
        anchor=anchor, which=which, offset=offset, textbox_size=line_size
    )
    # the top left corner of the text line including the background
    write_line_with_bg(
        img, text, line_pos_tl=(box_x1, box_y1), padding=padding, bg_color=bg_color,
        font_color=font_color, font_face=font_face, font_scale=font_scale,
        thickness=thickness, line_type=line_type
    )

def anchor_lines_with_bg(img: NDArray[uint8],
                         text_lines: List[str], *,
                         line_spacing: int = 5,
                         anchor: PixelPosition,
                         which: Corner,
                         offset: Tuple[int, int] = (0, 0),  # x, y
                         padding: Tuple[int, int] = (0, 0),  # horizontal, vertical
                         bg_color: ColorTuple = (0, 0, 0),
                         font_color: ColorTuple = (255, 255, 255),
                         # cv2 arguments
                         font_face: int = DEFAULT_FONT_FACE,
                         font_scale: float = 1.0,
                         thickness: int = 1,
                         line_type: int = cv2.LINE_8):
    box_size_without_offset: Tuple[int, int] = _get_text_size_lines_with_bg(
        text_lines, line_spacing=line_spacing, padding=padding,
        font_face=font_face, font_scale=font_scale, thickness=thickness
    )
    box_x1, box_y1, box_x2, box_y2 = _get_anchored_container_xyxy_coords(
        anchor=anchor, which=which, offset=offset, textbox_size=box_size_without_offset
    )
    write_lines_with_bg(
        img, text_lines, lines_pos_tl=(box_x1, box_y1), padding=padding, bg_color=bg_color,
        font_color=font_color, font_face=font_face, font_scale=font_scale,
        thickness=thickness, line_type=line_type
    )

def anchor_lines_on_rect_overlay(img: NDArray[uint8],
                                 text_lines: List[str], *,
                                 line_spacing: int = 5,
                                 anchor: PixelPosition,
                                 which: Corner,
                                 offset: Tuple[int, int] = (0, 0),  # x, y
                                 padding: Tuple[int, int] = (0, 0),  # horizontal, vertical
                                 bg_color: ColorTuple = (0, 0, 0),
                                 font_color: ColorTuple = (255, 255, 255),
                                 # cv2 arguments
                                 font_face: int = DEFAULT_FONT_FACE,
                                 font_scale: float = 1.0,
                                 thickness: int = 1,
                                 line_type: int = cv2.LINE_8):
    box_size_without_offset: Tuple[int, int] = _get_text_size_lines_on_rect_overlay(
        text_lines, line_spacing=line_spacing, padding=padding,
        font_face=font_face, font_scale=font_scale, thickness=thickness
    )
    box_x1, box_y1, box_x2, box_y2 = _get_anchored_container_xyxy_coords(
        anchor=anchor, which=which, offset=offset, textbox_size=box_size_without_offset
    )
    write_lines_on_rect_overlay(
        img, text_lines, overlay_tl=(box_x1, box_y1), padding=padding, bg_color=bg_color,
        font_color=font_color, font_face=font_face, font_scale=font_scale, line_spacing=line_spacing,
        thickness=thickness, line_type=line_type
    )
