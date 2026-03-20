from pydantic import BaseModel, NonNegativeFloat

from tram_analytics.v1.models.common_types import SpeedType

# --- global server settings ---

class ServerSettings(BaseModel):

    # whether the moving/stationary status of a vehicle is determined
    # based on its raw or smoothed speed
    # - smoothed is more reliable but slower, and will in particular
    #   make the status of a vehicle undefined until enough raw speed samples
    #   have been collected for smoothing
    # - raw allows for instant updates but is less reliable
    speed_type_for_stationary_determination: SpeedType

    # the speed threshold below which a vehicle is considered stationary
    is_stationary_threshold_ms: NonNegativeFloat

    # the duration threshold for a tram's stop on a platform
    # above which the pass is classified as a stopping pass
    # (a "stop" meaning a continuous period of time during
    # which the tram was stationary)
    # platform_stopping_pass_stationary_for_threshold_s: NonNegativeFloat
