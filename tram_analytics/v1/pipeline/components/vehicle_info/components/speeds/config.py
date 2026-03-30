from enum import Enum
from typing import Self, Literal, TypeAlias, Annotated

from pydantic import BaseModel, NonNegativeFloat, model_validator, Field


class SpeedSmoothingMethod(str, Enum):
    MEAN_VELOCITY = "mean_velocity"

class BaseSpeedSmoothingMethodConfig(BaseModel):
    pass

class MeanVelocitySpeedSmoothingMethodConfig(BaseSpeedSmoothingMethodConfig):
    method_name: Literal[SpeedSmoothingMethod.MEAN_VELOCITY] = SpeedSmoothingMethod.MEAN_VELOCITY

SpeedSmoothingMethodConfig: TypeAlias = Annotated[
    MeanVelocitySpeedSmoothingMethodConfig,
    Field(discriminator="method_name")
]

class SpeedSmoothingWindowConfig(BaseModel):
    min_duration: NonNegativeFloat = 0.0
    max_duration: NonNegativeFloat | None

    @model_validator(mode="after")
    def _validate(self) -> Self:
        if self.max_duration is not None and self.min_duration > self.max_duration:
            raise ValueError("window_min_duration must be less than or equal to window_max_duration "
                             "if both are specified")
        return self

class SpeedSmoothingConfig(BaseModel):
    window: SpeedSmoothingWindowConfig
    method: SpeedSmoothingMethodConfig

class SpeedCalculatorConfig(BaseModel):
    smoothing: SpeedSmoothingConfig

