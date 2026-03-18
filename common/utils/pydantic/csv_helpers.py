from csv import DictReader
from io import TextIOWrapper
from pathlib import Path
from typing import Type, List

from pydantic import BaseModel

def empty_str_to_none(s: str) -> str | None:
    """
    For use as a field validator in Pydantic models that are parsed from CSV files:
    an empty string will be interpreted as null.
    """
    if s == "":
        return None
    return s


def load_models_from_csv[ModelT: BaseModel](
        src_path: str | Path, model_type: Type[ModelT]
) -> List[ModelT]:
    """
    Converts each row in the CSV file into an instance
    of the specified subtype of Pydantic's `BaseModel`.
    """
    with open(src_path, "r", encoding="utf8") as fin: # type: TextIOWrapper
        reader: DictReader[str] = DictReader(fin)
        cases: List[ModelT] = [
            model_type.model_validate(row)
            for row in reader
        ]
    return cases
