import json
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

import numpy as np
from numpy import float64
from numpy.typing import NDArray
from pydantic import BaseModel, Field, ConfigDict

from common.utils.random.id_gen import get_uuid
from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.tracking import TrackState, DetectionToTrackState
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


class SingleClassSortParams(BaseModel):

    # If the track has not been detected for `max_age` frames, terminate it.
    max_age: int = Field(ge=0,
                         default=1)

    # The minimum number of frames for which a track must have been detected
    # before it starts to be included in the output.
    #
    # NOTE: Setting `min_hits` to a value higher than 1 will probably result
    # in detections corresponding to new tracks to not be assigned track IDs
    # for `min-hits - 1` frames.
    # TODO: Downstream logic depends on every detection having an associated track.
    #  Should probably hardcode always initialising `Sort` with `min_hits` set to `0`
    #  and remove this field.
    min_hits: int = Field(ge=0,
                          default=3)

    # The minimum IoU between the observed bounding box and the predicted one
    # for the same track to be assigned to the object.
    iou_threshold: float = Field(ge=0.0, le=1.0,
                                 default=0.3)

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

    def __init__(self, *, camera_id: str, class_params: Dict[int, SingleClassSortParams]) -> None:
        """
        Create a tracker for each class.
        :param class_params: a `Dict` mapping class IDs to the keyword arguments
        with which to initialise the instance `Sort` for the respective class.
        """

        self._camera_id: str = camera_id

        # { class_id: int -> tracker_for_class: Sort }
        self._trackers: Dict[int, Sort] = {
            class_id: Sort(**params.model_dump())
            for class_id, params in class_params.items()
        }

        # A wrapper to assign UUIDs to each track.
        # { class_id: int -> { sort_internal_track_id: int -> track_uuid: str } }
        self._uuid_map: Dict[int, Dict[int, str]] = {
            class_id: dict()
            for class_id in class_params.keys()
        }

    def _split_dets_by_class_id(self, dets: List[Detection]) -> Dict[int, List[Detection]]:
        """
        Build mappings between all class IDs for which trackers have been initialised
        and the list of detections in `dets` that have this class ID.
        """
        # get the class IDs for all Sort trackers
        class_ids: List[int] = list(self._trackers.keys())
        # initialise empty lists of detections for all class IDs
        class_to_dets: Dict[int, List[Detection]] = {
            class_id: []
            for class_id in class_ids
        }
        # put each detection into the corresponding list
        for det in dets:  # type: Detection
            class_id: int = det.raw_detection.class_id
            if class_id not in class_ids:
                raise ValueError(
                    f"No tracker present for class ID {class_id}.\n)"
                    + f"Detection ID: {det.detection_id}\n"
                    + "Available classes: {}".format(
                        ", ".join(str(c_id) for c_id in sorted(list(self._trackers.keys()))))
                )
            class_to_dets[class_id].append(det)
        return class_to_dets

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

    def _remove_ids_for_dead_tracks(self, class_id: int,
                                    sort_results: SortResults) -> None:
        """
        From this instance's _uuid_map, for `class_id`, remove mappings
        for track IDs no longer present in the tracker's output
        """
        if class_id not in self._trackers:
            raise ValueError(f"Class {class_id} is not registered in _trackers.")
        if class_id not in self._uuid_map:
            raise ValueError(f"Class {class_id} is not registered in _uuid_map.")

        # get the internal track ID -> track UUID mapping for this class
        mappings_for_class: Dict[int, str] = self._uuid_map[class_id]
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

    def _build_entities_from_sort_results_for_class(
            self, sort_results: SortResults,
            *, class_id: int,
            dets: List[Detection]
    ) -> Tuple[List[TrackState], List[DetectionToTrackState]]:

        internal_id_to_uuid_map: Dict[int, str] = self._uuid_map[class_id]

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
                                           class_id=class_id,
                                           track_id=track_uuid,
                                           is_matched=is_matched,
                                           is_confirmed_track=is_confirmed_track,
                                           bbox=bbox)
            states.append(state)

        return states, det_state_mappings

    def _update_for_class(
            self, *, class_id: int, dets: List[Detection]
    ) -> Tuple[List[TrackState], List[DetectionToTrackState]]:
        """
        Update this class's tracker and return the track UUIDs.
        All `dets` must belong to class `class_id`.

        NOTE: Also removes from `_uuid_map` mappings for inactive tracks
        (i. e. those for which no Kalman trackers are present in the respective `Sort` tracker).
        """
        # TODO: handle None in dets
        if not all(det.raw_detection.class_id == class_id for det in dets):
            raise ValueError(f"Invalid dets: all detections must have class ID {class_id}.")
        # check whether the frame ID is the same in all dets
        if len(set(det.frame_id for det in dets)) > 1:
            raise ValueError(f"Invalid dets: all detections must have frame ID")

        tracker: Sort = self._trackers[class_id]

        # transform detections to a NumPy array to be passed to the tracker
        inputs: NDArray[np.float64] = detections_to_sort_input(dets)

        sort_results: SortResults = SortResults(**tracker.update(inputs))

        states, det_state_mappings = self._build_entities_from_sort_results_for_class(
            sort_results, class_id=class_id, dets=dets
        ) # type: List[TrackState], List[DetectionToTrackState]

        # remove mappings for inactive tracks
        self._remove_ids_for_dead_tracks(class_id, sort_results)

        return states, det_state_mappings

    def update(
            self, dets: List[Detection]
    ) -> Tuple[List[TrackState], List[DetectionToTrackState]]:
        """
        For each class, update the respective class's tracker
        and return the track states and their mappings to detection IDs.

        For classes not present among detections in `dets`, update their trackers
        with an empty NDArray (as required by the implementation of the `Sort` tracker used).
        """
        class_to_dets: Dict[int, List[Detection]] = self._split_dets_by_class_id(dets)

        states_all: List[TrackState] = []
        det_to_state_mappings_all: List[DetectionToTrackState] = []

        # update the trackers per class
        for class_id, dets_for_class in class_to_dets.items():  # type: int, List[Detection]
            states, track_det_mappings = self._update_for_class(
                class_id=class_id, dets=dets_for_class
            ) # type: List[TrackState], List[DetectionToTrackState]

            states_all.extend(states)
            det_to_state_mappings_all.extend(track_det_mappings)

        return states_all, det_to_state_mappings_all