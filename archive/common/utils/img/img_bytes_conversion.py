from contextlib import contextmanager
from io import BytesIO
from typing import Generator
from warnings import deprecated

from PIL import Image
from PIL.Image import Image as ImageType


@deprecated("Deprecated, use pil_from_bytes() instead")
@contextmanager
def pil_from_bytes_old(img: bytes) -> Generator[ImageType]:
    with BytesIO(img) as img_stream:  # type: BytesIO
        with Image.open(fp=img_stream) as pil_img: # type: ImageType
            # allocate storage for the image so that the stream can be closed
            pil_img.load()
            yield pil_img
