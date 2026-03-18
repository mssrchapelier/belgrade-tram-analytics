from typing import Dict

from tram_analytics.v1.pipeline.components.tracking.tracking import SingleClassSortParams

TRACKER_PARAMS: Dict[int, SingleClassSortParams] = {
    0: SingleClassSortParams(max_age=3, min_hits=2),
    2: SingleClassSortParams(max_age=3, min_hits=2)
}

# TRACKER_PARAMS: Dict[int, SingleClassSortParams] = {
#     0: SingleClassSortParams(max_age=3, min_hits=2),
#     1: SingleClassSortParams(max_age=3, min_hits=2)
# }