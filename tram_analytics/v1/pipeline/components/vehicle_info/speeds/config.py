from enum import Enum
from typing import Self, Literal, TypeAlias, Annotated

from pydantic import BaseModel, NonNegativeFloat, model_validator, Field


class SpeedSmoothingMethod(str, Enum):
    MEAN_VELOCITY = "mean_velocity"
    MOCK = "mock"

class BaseSpeedSmoothingMethodConfig(BaseModel):
    method_name: Literal[SpeedSmoothingMethod.MEAN_VELOCITY, SpeedSmoothingMethod.MOCK]

class MeanVelocitySpeedSmoothingMethodConfig(BaseSpeedSmoothingMethodConfig):
    method_name: Literal[SpeedSmoothingMethod.MEAN_VELOCITY] = SpeedSmoothingMethod.MEAN_VELOCITY

class MockSpeedSmoothingMethodConfig(BaseSpeedSmoothingMethodConfig):
    method_name: Literal[SpeedSmoothingMethod.MOCK] = SpeedSmoothingMethod.MOCK

SpeedSmoothingMethodConfig: TypeAlias = Annotated[
    MeanVelocitySpeedSmoothingMethodConfig | MockSpeedSmoothingMethodConfig,
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

