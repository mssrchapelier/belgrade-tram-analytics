from typing import Iterable, Sequence, List
import itertools
from urllib.parse import urlparse

def stringify_list_of_floats(values: Iterable[float], *, precision: int, separator: str = ", ") -> str:
    if not precision >= 0:
        raise ValueError("precision must be a positive integer")
    # e. g. "{:.2f}"
    format_for_value: str = f"{{:.{precision}f}}"
    result: str = separator.join(format_for_value.format(value) for value in values)
    return result

def concatenate_sequences[T](sequences: Sequence[Sequence[T]]) -> List[T]:
    return list(itertools.chain(*sequences))

def is_url(url: str) -> bool:
    return urlparse(url).scheme != ""
