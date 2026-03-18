from io import BytesIO

from PIL import Image
from PIL.Image import Image as ImageType

def pil_from_bytes(img: bytes) -> ImageType:
    with BytesIO(img) as img_stream:  # type: BytesIO
        pil_img: ImageType = Image.open(fp=img_stream)
        # allocate storage for the image so that the stream can be closed
        pil_img.load()
    return pil_img

def bytes_from_pil(pil_img: ImageType, *, img_format: str) -> bytes:
    with BytesIO() as stream: # type: BytesIO
        pil_img.save(fp=stream, format=img_format)
        img: bytes = stream.getvalue()
    return img

def read_img_bytes_from_path(filepath: str, *, img_format: str) -> bytes:
    with Image.open(filepath) as pil_img: # type: ImageType
        img: bytes = bytes_from_pil(pil_img, img_format=img_format)
    return img

def write_img_bytes_to_path(img: bytes, filepath: str, *, img_format: str) -> None:
    with pil_from_bytes(img) as pil_img: # type: ImageType
        pil_img.save(filepath, format=img_format)
