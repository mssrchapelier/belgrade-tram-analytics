import json
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

import numpy as np
from numpy import float64
from numpy.typing import NDArray
from pydantic import ConfigDict

from common.utils.random.id_gen import get_uuid
from tram_analytics.v1.models.common_types import BoundingBox, VehicleType
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.tracking import TrackState, DetectionToTrackState
from tram_analytics.v1.pipeline.components.tracking.settings import SingleClassSortParams
from vendor.sort.sort import Sort


def _dumps_args(sort_output: NDArray[float64], dets: List[Detection]) -> str:
    return (
        "--- sort_output ---\n"
        + f"{str(sort_output)}\n"
        + "--- dets ---\n"
        + "\n".join(json.dumps(det.model_dump_json()) for det in dets)
    )

def detections_to_sort_input(dets: List[Detection]) -> NDArray[float64]:
    """
    Transforms a `List` of `Detection` instances into a NumPy array
    to be used as the input for a SORT tracker.

    The output is a NumPy array of shape `(num_detections, 5)`
    with the following structure:

    ```
    [ [x1, y1, x2, y2, confidence],
      ... ]
    ```

    The dtype of the array is `float64`.

    """
    values: List[List[float]] = [
        [det.raw_detection.bbox.x1,
         det.raw_detection.bbox.y1,
         det.raw_detection.bbox.x2,
         det.raw_detection.bbox.y2,
         det.raw_detection.confidence]
        for det in dets
    ]
    output: NDArray[np.float64] = (
        np.array(values, dtype=np.float64) if len(values) > 0
        else np.empty(shape=(0, 5), dtype=np.float64)
    )
    return output

def match_sort_output_to_detections(sort_output: NDArray[float64], dets: List[Detection]) -> List[int | None]:
    """
    Returns track IDs in the order in which detections appear in `dets`.
    If there is a detection for which there is no corresponding row in `sort_output`,
    puts `None` in place of the track ID.
    """

    assert len(sort_output.shape) == 2
    assert sort_output.shape[1] == 5
    if len(dets) != sort_output.shape[0]:
        print(f"WARNING: Different number of detections ({len(dets)}) and track IDs ({sort_output.shape[0]})")

    # match detection indices in `dets` to the respective track ID, initialise track IDs with None
    # { idx_in_dets -> track_id }
    idx_to_track_id: Dict[int, int | None] = {
        idx: None
        for idx in range(len(dets))
    }

    # match the coordinates tuple for every detection in `dets` to its index
    # { (x1, y1, x2, y2) -> idx_in_dets }
    coords_to_src_idx: Dict[Tuple[float, float, float, float], int] = {
        (
            det.raw_detection.bbox.x1, det.raw_detection.bbox.y1,
            det.raw_detection.bbox.x2, det.raw_detection.bbox.y2
        ): idx
        for idx, det in enumerate(dets)
    }

    # extract coordinates tuples and track IDs for every detection in `sort_output`
    # [ ( (x1, y1, x2, y2), track_id ), ... ]
    track_data: List[Tuple[Tuple[float, float, float, float], int]] = [
        ((track_row[0], track_row[1], track_row[2], track_row[3]),
         int(track_row[4]))
        for track_row in sort_output.tolist()
    ]

    # check that the same coordinate tuple does not appear twice in `track_data`
    if len(set(row[0] for row in track_data)) != len(track_data):
        arg_dump: str = _dumps_args(sort_output, dets)
        raise ValueError(f"Duplicate coordinates in track_data:\n{arg_dump}")

    # iterate over `track_data` and match track IDs
    # with the indices of the respective bounding boxes in `dets`
    for coords, track_id in track_data: # type: Tuple[float, float, float, float], int
        if coords not in coords_to_src_idx:
            arg_dump = _dumps_args(sort_output, dets)
            raise ValueError(f"Got track bbox coordinates absent from detections:\n{arg_dump}")
        # get the index of this bounding box in `dets`
        idx: int = coords_to_src_idx[coords]
        # assign the track ID to this index
        idx_to_track_id[idx] = track_id

    # if there are detections for which no track ID has been assigned, trigger a warning
    if None in idx_to_track_id.values():
        # get the indices of such bounding boxes
        indices_with_no_track_id: List[int] = [
            idx
            for idx, track_id in idx_to_track_id.items()
            if track_id is None
        ]
        arg_dump = _dumps_args(sort_output, dets)
        print(
            f"WARNING: There were detections for which no track ID was present in the tracker's output:\n"
            + f"{arg_dump}\n"
            +  "indices of detections with no track ID: {}".format(
                ", ".join(str(idx) for idx in indices_with_no_track_id)
            )
        )

    # transform the mapping `idx_to_track_id` to a list of track IDs
    # appearing in the order the indices are ascending
    sorted_track_ids: List[int | None] = [
        idx_to_track_id[idx]
        for idx in range(len(idx_to_track_id))
    ]
    return sorted_track_ids


@dataclass(frozen=True, slots=True, kw_only=True)
class SortResults:

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

    # the predicted tracker bounding boxes and tracker IDs
    # for ALL matched tracks (both confirmed and unconfirmed)
    # shape: `(num_tracker_bboxes, 5)`
    # each row: `[x1 y1 x2 y2 internal_track_id]`
    track_states: NDArray[np.float64]

    # internal track ID -> detection row index in input to `Sort`
    track_id_to_det_idx: Dict[int, int]

    # track IDs: not associated with any of the detections,
    # but for which the Kalman tracker is still alive
    unmatched_track_ids: Set[int]

    # the track IDs for confirmed tracks
    matched_confirmed_track_ids: Set[int]

    # the track IDs for unconfirmed tracks
    matched_unconfirmed_track_ids: Set[int]


class SortWrapper:

    """
    Class-aware SORT wrapper: a separate instance of `Sort` is maintained for every class;
    detections belonging to each class are being tracked separately.
    Internal track IDs generated by `Sort` trackers are converted to UUIDs properly
    (i. e. each track ID from each tracker for each specific class is mapped to a unique UUID).
    """

    def __init__(self, *, camera_id: str, class_params: Dict[VehicleType, SingleClassSortParams]) -> None:
        """
        Create a tracker for each class.
        :param class_params: a `Dict` mapping vehicle types to the keyword arguments
        with which to initialise the instance `Sort` for the respective class.
        """

        self._camera_id: str = camera_id

        # { vehicle_type: int -> tracker_for_class: Sort }
        self._trackers: Dict[VehicleType, Sort] = {
            vehicle_type: Sort(**params.model_dump())
            for vehicle_type, params in class_params.items()
        }

        # A wrapper to assign UUIDs to each track.
        # { vehicle_type: VehicleType -> { sort_internal_track_id: int -> track_uuid: str } }
        self._uuid_map: Dict[VehicleType, Dict[int, str]] = {
            vehicle_type: dict()
            for vehicle_type in class_params.keys()
        }

    def _split_dets_by_vehicle_type(self, dets: List[Detection]) -> Dict[VehicleType, List[Detection]]:
        """
        Build mappings between all vehicle types for which trackers have been initialised
        and the list of detections in `dets` that have this vehicle type.
        """
        # get the vehicle types for all Sort trackers
        vehicle_types: List[VehicleType] = list(self._trackers.keys())
        # initialise empty lists of detections for all vehicle types
        vehicle_type_to_dets: Dict[VehicleType, List[Detection]] = {
            vehicle_type: []
            for vehicle_type in vehicle_types
        }
        # put each detection into the corresponding list
        for det in dets:  # type: Detection
            vehicle_type: VehicleType = det.raw_detection.vehicle_type
            if vehicle_type not in vehicle_types:
                raise ValueError(
                    f"No tracker present for vehicle type: {vehicle_type}.\n)"
                    + f"Detection ID: {det.detection_id}\n"
                    + "Available classes: {}".format(
                        ", ".join(str(c_id) for c_id in sorted(list(self._trackers.keys()))))
                )
            vehicle_type_to_dets[vehicle_type].append(det)
        return vehicle_type_to_dets

    @staticmethod
    def _sort_output_to_coord_list(sort_output: NDArray[float64]) -> List[List[float]]:
        """
        Extracts a list of lists `[x1, y1, x2, y2]` from rows in the output of `Sort.update()`.
        Each row corresponds to the coordinates of a tracker bounding box.
        """
        arr_shape: Tuple[int, ...] = sort_output.shape
        if not (len(arr_shape) == 2 and arr_shape[1] == 5):
            raise ValueError(f"Unexpected shape of sort_output: {str(arr_shape)}")
        out: List[List[float]] = sort_output[:, :4].tolist()
        return out

    @staticmethod
    def _sort_results_to_str(results: SortResults) -> str:
        msg: str = "\n".join(
            "{:>20} | {}".format(name, str(getattr(results, name)))
            for name in ["det_idx_to_track_id", "unmatched_track_ids",
                         "matched_confirmed_track_ids", "matched_unconfirmed_track_ids"]
        )
        msg += "\n\ntrack_estimates:\n\n{}".format(str(results.track_states))
        return msg

    def _remove_ids_for_dead_tracks(self, vehicle_type: VehicleType,
                                    sort_results: SortResults) -> None:
        """
        From this instance's _uuid_map, for `class_id`, remove mappings
        for track IDs no longer present in the tracker's output
        """
        if vehicle_type not in self._trackers:
            raise ValueError(f"Vehicle type {vehicle_type} is not registered in _trackers.")
        if vehicle_type not in self._uuid_map:
            raise ValueError(f"Vehicle type {vehicle_type} is not registered in _uuid_map.")

        # get the internal track ID -> track UUID mapping for this class
        mappings_for_class: Dict[int, str] = self._uuid_map[vehicle_type]
        ids_in_mappings: Set[int] = set(mappings_for_class.keys())
        # get the set of track IDs still alive
        ids_alive: Set[int] = set.union(sort_results.matched_confirmed_track_ids,
                                        sort_results.matched_unconfirmed_track_ids,
                                        sort_results.unmatched_track_ids)
        # get the set of track IDs that are present in the map but absent from `existing_ids`
        ids_to_remove: Set[int] = set.difference(ids_in_mappings, ids_alive)
        # remove mappings with these IDs
        for track_id in ids_to_remove:  # type: int
            mappings_for_class.pop(track_id)

    def _build_entities_from_sort_results_for_vehicle_type(
            self, sort_results: SortResults,
            *, vehicle_type: VehicleType,
            dets: List[Detection]
    ) -> Tuple[List[TrackState], List[DetectionToTrackState]]:

        internal_id_to_uuid_map: Dict[int, str] = self._uuid_map[vehicle_type]

        states: List[TrackState] = []
        det_state_mappings: List[DetectionToTrackState] = []

        for row in sort_results.track_states:  # type: NDArray[float64]

            # --- build TrackState ---
            internal_track_id: int = int(row[4].item())

            # match the internal ID to UUIDs
            # - get the UUID (if any) mapped to this internal ID
            track_uuid: str | None = internal_id_to_uuid_map.get(internal_track_id)
            if track_uuid is None:
                # new internal track ID encountered
                # - generate a new UUID
                track_uuid = get_uuid()
                # - create a new mapping for this track
                # NOTE: Modifies the instance's state here.
                internal_id_to_uuid_map[internal_track_id] = track_uuid

            state_id: str = get_uuid()
            is_confirmed_track: bool = internal_track_id in sort_results.matched_confirmed_track_ids
            bbox: BoundingBox = BoundingBox(**{
                name: row[idx].item()
                for idx, name in enumerate(["x1", "y1", "x2", "y2"])
            })

            # --- build TrackStateToDetection ---

            det_idx: int | None = sort_results.track_id_to_det_idx.get(internal_track_id)
            is_matched: bool = det_idx is not None
            if det_idx is not None:
                track_det_mapping: DetectionToTrackState = DetectionToTrackState(
                    track_state_id=state_id, detection_id=dets[det_idx].detection_id
                )
                det_state_mappings.append(track_det_mapping)

            state: TrackState = TrackState(track_state_id=state_id,
                                           vehicle_type=vehicle_type,
                                           track_id=track_uuid,
                                           is_matched=is_matched,
                                           is_confirmed_track=is_confirmed_track,
                                           bbox=bbox)
            states.append(state)

        return states, det_state_mappings

    def _update_for_vehicle_type(
            self, *, vehicle_type: VehicleType, dets: List[Detection]
    ) -> Tuple[List[TrackState], List[DetectionToTrackState]]:
        """
        Update this class's tracker and return the track UUIDs.
        All `dets` must belong to class `class_id`.

        NOTE: Also removes from `_uuid_map` mappings for inactive tracks
        (i. e. those for which no Kalman trackers are present in the respective `Sort` tracker).
        """
        # TODO: handle None in dets
        if not all(det.raw_detection.vehicle_type == vehicle_type for det in dets):
            raise ValueError(f"Invalid dets: all detections must have vehicle type {vehicle_type}.")
        # check whether the frame ID is the same in all dets
        if len(set(det.frame_id for det in dets)) > 1:
            raise ValueError(f"Invalid dets: all detections must have frame ID")

        tracker: Sort = self._trackers[vehicle_type]

        # transform detections to a NumPy array to be passed to the tracker
        inputs: NDArray[np.float64] = detections_to_sort_input(dets)

        sort_results: SortResults = SortResults(**tracker.update(inputs))

        states, det_state_mappings = self._build_entities_from_sort_results_for_vehicle_type(
            sort_results, vehicle_type=vehicle_type, dets=dets
        ) # type: List[TrackState], List[DetectionToTrackState]

        # remove mappings for inactive tracks
        self._remove_ids_for_dead_tracks(vehicle_type, sort_results)

        return states, det_state_mappings

    def update(
            self, dets: List[Detection]
    ) -> Tuple[List[TrackState], List[DetectionToTrackState]]:
        """
        For each vehicle type, update the respective vehicle type's tracker
        and return the track states and their mappings to detection IDs.

        For vehicle types not present among detections in `dets`, update their trackers
        with an empty NDArray (as required by the implementation of the `Sort` tracker used).
        """
        vehicle_type_to_dets: Dict[VehicleType, List[Detection]] = self._split_dets_by_vehicle_type(dets)

        states_all: List[TrackState] = []
        det_to_state_mappings_all: List[DetectionToTrackState] = []

        # update the trackers per class
        for vehicle_type, dets_for_vehicle_type in vehicle_type_to_dets.items():  # type: VehicleType, List[Detection]
            states, track_det_mappings = self._update_for_vehicle_type(
                vehicle_type=vehicle_type, dets=dets_for_vehicle_type
            ) # type: List[TrackState], List[DetectionToTrackState]

            states_all.extend(states)
            det_to_state_mappings_all.extend(track_det_mappings)

        return states_all, det_to_state_mappings_all