SHAPELY_POINT_ON_LINE_DISTANCE_TOLERANCE_WORLDCOORDS: float = 1e-8
SHAPELY_POINT_ON_LINE_DISTANCE_TOLERANCE_IMAGECOORDS: float = 1e-6

# The maximum size of track state and vehicle info history stored by subclasses of BaseZoneAssigner
# for the purpose of zone assignment and speed estimation.
MAX_VEHICLE_HISTORY_SIZE: int = 10

SPEED_SMOOTHING_WINDOW_SIZE: int = 5