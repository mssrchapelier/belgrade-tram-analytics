from pydantic import BaseModel, PositiveFloat

class DashboardConfig(BaseModel):
    # tab title
    app_title: str
    # how often to poll for updates
    update_interval: PositiveFloat
