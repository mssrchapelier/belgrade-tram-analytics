from typing import List, Self, override

from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed import (
    GlobalMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import (
    BaseStatusVehicleEvent,
    BaseSceneEventsContainer
)

# --- (global) motion status events ---

# Any vehicle is assigned one of three status values with respect to whether it is considered moving or not:
# (1) stationary; (2) moving; (3) undefined.
# Under the current approach, the undefined status is possible only in the beginning,
# before any assignment of a definite status has been made
# (because the status might be computed based on smoothed speeds
# which might not yet be available, or for other reasons);
# once the vehicle was classified as stationary or moving,
# any "undefined" status updates preserve its previous classification.
#
# --- DEPRECATED PART ---
# The following events are meant to be emitted on any change to "stationary" and "moving" status:
# - stationary start event: undefined -> stationary OR moving -> stationary;
# - stationary end event: stationary -> moving.
# (moving -> undefined, stationary -> undefined are not possible, see above.)
#
# Edge cases as handled by the event generator:
# - On a vehicle's lifetime end, if it is still registered stationary,
#   a stationary end event must be emitted.
#
# --- UPDATED PART ---
# The following events are meant to be emitted on any change to "stationary" and "moving" status:
# - stationary status start event: undefined -> stationary OR moving -> stationary;
# - moving status start event: undefined -> moving (new) OR stationary -> moving.
# (moving -> undefined, stationary -> undefined are not possible, see above.)
#
# Edge cases as handled by the event generator:
# - On a vehicle's lifetime end, if it is still registered stationary,
#   a stationary end event must be emitted.

# --- events ---

# --- (1) momentary and confirmed motion status ---

class MotionStatusUpdate(BaseStatusVehicleEvent):
    """
    A container for two types of the motion status that are assigned at every moment to every specific vehicle:

    - Momentary: determined on the basis of the current speed only; no constraints on it being undefined at any time.
    - Confirmed:
      A confirmed motion status can be undefined, stationary or moving.
      - The vehicle begins with the undefined status.
      - The first stationary or moving momentary status changes the confirmed status to stationary/moving respectively.
      - Subsequent undefined momentary statuses do NOT trigger a change in the confirmed status.
      - When a vehicle that has had stationary confirmed status receives a moving momentary status,
        its confirmed status changes to moving. Similarly for moving -> stationary confirmed status change
        (triggered on the first stationary momentary status being received).
    """

    momentary: MotionStatus
    confirmed: MotionStatus


class MotionStatusEventsContainer(BaseSceneEventsContainer):

    """
    A container for all events related to vehicles' motion status emitted for the given frame:
    - motion status updates for every vehicle that is alive;
    - start/end events for periods of any given vehicle continuously holding the same status.
    """

    status_updates: List[MotionStatusUpdate]
    changes_in_confirmed_status: GlobalMotionStatusPeriodBoundaryEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(status_updates=[],
                   changes_in_confirmed_status=GlobalMotionStatusPeriodBoundaryEventsContainer.create_empty_container())

class MotionStatusUpdatesContainer(BaseSceneEventsContainer):

    status_updates: List[MotionStatusUpdate]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(status_updates=[])
