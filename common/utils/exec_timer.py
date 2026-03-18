from typing import Generator
from time import perf_counter
from contextlib import contextmanager
import logging
from logging import Logger

logger: Logger = logging.getLogger(__name__)

@contextmanager
def timed(label: str) -> Generator[None]:
    start: float = perf_counter()
    try:
        yield None
    finally:
        elapsed: float = perf_counter() - start
        logger.debug(f"timed | {label:>50} | {elapsed:.4f} s")