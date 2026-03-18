from typing import Annotated, TypeAlias

from pydantic import confloat, Field, AfterValidator

from common.utils.custom_types import _validate_odd

OpenUnitIntervalValue: TypeAlias = Annotated[float, confloat(ge=0.0, le=1.0)]
OddPositiveInt: TypeAlias = Annotated[int, Field(gt=0), AfterValidator(_validate_odd)]
