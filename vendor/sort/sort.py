# mypy: ignore-errors
"""
SORT: A Simple, Online and Realtime Tracker
Copyright (C) 2016-2020 Alex Bewley alex@bewley.ai

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

---

09 Dec 2025 - Modified by Kirill Karpenko <https://github.com/mssrchapelier>.
See the list of changes in the accompanying `README.md`.
"""

from typing import Dict, List, Set

import numpy as np
from numpy import float64
from numpy.typing import NDArray
from filterpy.kalman import KalmanFilter

np.random.seed(0)

def linear_assignment(cost_matrix):
    try:
        import lap
        _, x, y = lap.lapjv(cost_matrix, extend_cost=True)
        return np.array([[y[i],i] for i in x if i >= 0]) #
    except ImportError:
        from scipy.optimize import linear_sum_assignment
        x, y = linear_sum_assignment(cost_matrix)
        return np.array(list(zip(x, y)))


def iou_batch(bb_test, bb_gt):
    """
    From SORT: Computes IOU between two bboxes in the form [x1,y1,x2,y2]
    """
    bb_gt = np.expand_dims(bb_gt, 0)
    bb_test = np.expand_dims(bb_test, 1)

    xx1 = np.maximum(bb_test[..., 0], bb_gt[..., 0])
    yy1 = np.maximum(bb_test[..., 1], bb_gt[..., 1])
    xx2 = np.minimum(bb_test[..., 2], bb_gt[..., 2])
    yy2 = np.minimum(bb_test[..., 3], bb_gt[..., 3])
    w = np.maximum(0., xx2 - xx1)
    h = np.maximum(0., yy2 - yy1)
    wh = w * h
    o = wh / ((bb_test[..., 2] - bb_test[..., 0]) * (bb_test[..., 3] - bb_test[..., 1])
              + (bb_gt[..., 2] - bb_gt[..., 0]) * (bb_gt[..., 3] - bb_gt[..., 1]) - wh)
    return(o)


def convert_bbox_to_z(bbox):
    """
    Takes a bounding box in the form [x1,y1,x2,y2] and returns z in the form
      [x,y,s,r] where x,y is the centre of the box and s is the scale/area and r is
      the aspect ratio
    """
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    x = bbox[0] + w/2.
    y = bbox[1] + h/2.
    s = w * h    #scale is just area
    r = w / float(h)
    return np.array([x, y, s, r]).reshape((4, 1))


def convert_x_to_bbox(x,score=None):
    """
    Takes a bounding box in the centre form [x,y,s,r] and returns it in the form
      [x1,y1,x2,y2] where x1,y1 is the top left and x2,y2 is the bottom right
    """
    w = np.sqrt(x[2] * x[3])
    h = x[2] / w
    if(score==None):
        return np.array([x[0]-w/2.,x[1]-h/2.,x[0]+w/2.,x[1]+h/2.]).reshape((1,4))
    else:
        return np.array([x[0]-w/2.,x[1]-h/2.,x[0]+w/2.,x[1]+h/2.,score]).reshape((1,5))


class KalmanBoxTracker(object):
    """
    This class represents the internal state of individual tracked objects observed as bbox.
    """
    count = 0
    def __init__(self,bbox):
        """
        Initialises a tracker using initial bounding box.
        """
        #define constant velocity model
        self.kf = KalmanFilter(dim_x=7, dim_z=4)
        self.kf.F = np.array(
            [[1,0,0,0,1,0,0],
             [0,1,0,0,0,1,0],
             [0,0,1,0,0,0,1],
             [0,0,0,1,0,0,0],
             [0,0,0,0,1,0,0],
             [0,0,0,0,0,1,0],
             [0,0,0,0,0,0,1]]
        )
        self.kf.H = np.array(
            [[1,0,0,0,0,0,0],
             [0,1,0,0,0,0,0],
             [0,0,1,0,0,0,0],
             [0,0,0,1,0,0,0]]
        )

        self.kf.R[2:,2:] *= 10.
        self.kf.P[4:,4:] *= 1000. #give high uncertainty to the unobservable initial velocities
        self.kf.P *= 10.
        self.kf.Q[-1,-1] *= 0.01
        self.kf.Q[4:,4:] *= 0.01

        self.kf.x[:4] = convert_bbox_to_z(bbox)
        self.time_since_update = 0

        # MODIFIED: Kirill Karpenko
        # Assigning `count + 1` instead of `count` here,
        # rather than adding 1 to the tracker's ID in `Sort.update()`.
        self.id = KalmanBoxTracker.count + 1

        KalmanBoxTracker.count += 1
        self.history = []
        self.hits = 0
        self.hit_streak = 0
        self.age = 0

    def update(self,bbox):
        """
        Updates the state vector with observed bbox.
        """
        self.time_since_update = 0
        self.history = []
        self.hits += 1
        self.hit_streak += 1
        self.kf.update(convert_bbox_to_z(bbox))

    def predict(self):
        """
        Advances the state vector and returns the predicted bounding box estimate.
        """
        if((self.kf.x[6]+self.kf.x[2])<=0):
            self.kf.x[6] *= 0.0
        self.kf.predict()
        self.age += 1
        if(self.time_since_update>0):
            self.hit_streak = 0
        self.time_since_update += 1
        self.history.append(convert_x_to_bbox(self.kf.x))
        return self.history[-1]

    def get_state(self):
        """
        Returns the current bounding box estimate.
        """
        return convert_x_to_bbox(self.kf.x)


def associate_detections_to_trackers(detections,trackers,iou_threshold = 0.3):
    """
    Assigns detections to tracked object (both represented as bounding boxes)

    Returns 3 lists of matches, unmatched_detections and unmatched_trackers
    """
    if(len(trackers)==0):
        return np.empty((0,2),dtype=int), np.arange(len(detections)), np.empty((0,5),dtype=int)

    iou_matrix = iou_batch(detections, trackers)

    if min(iou_matrix.shape) > 0:
        a = (iou_matrix > iou_threshold).astype(np.int32)
        if a.sum(1).max() == 1 and a.sum(0).max() == 1:
            matched_indices = np.stack(np.where(a), axis=1)
        else:
            matched_indices = linear_assignment(-iou_matrix)
    else:
        matched_indices = np.empty(shape=(0,2))

    unmatched_detections = []
    for d, det in enumerate(detections):
        if(d not in matched_indices[:,0]):
            unmatched_detections.append(d)
    unmatched_trackers = []
    for t, trk in enumerate(trackers):
        if(t not in matched_indices[:,1]):
            unmatched_trackers.append(t)

    #filter out matched with low IOU
    matches = []
    for m in matched_indices:
        if(iou_matrix[m[0], m[1]]<iou_threshold):
            unmatched_detections.append(m[0])
            unmatched_trackers.append(m[1])
        else:
            matches.append(m.reshape(1,2))
    if(len(matches)==0):
        matches = np.empty((0,2),dtype=int)
    else:
        matches = np.concatenate(matches,axis=0)

    return matches, np.array(unmatched_detections), np.array(unmatched_trackers)


class Sort(object):
    def __init__(self, max_age=1, min_hits=3, iou_threshold=0.3):
        """
        Sets key parameters for SORT
        """
        self.max_age = max_age
        self.min_hits = min_hits
        self.iou_threshold = iou_threshold
        self.trackers: List[KalmanBoxTracker] = []
        self.frame_count: int = 0

    def update(self, dets: NDArray[float64] = np.empty((0, 5))):
        """
        Params:
          dets - a numpy array of detections in the format [[x1,y1,x2,y2,score],[x1,y1,x2,y2,score],...]
        Requires: this method must be called once for each frame even with empty detections (use np.empty((0, 5)) for frames without detections).
        Returns the a similar array, where the last column is the object ID.

        NOTE: The number of objects returned may differ from the number of detections provided.
        """
        self.frame_count += 1
        # get predicted locations from existing trackers.
        trks = np.zeros((len(self.trackers), 5))
        to_del = []
        ret = []
        for t, trk in enumerate(trks):
            pos = self.trackers[t].predict()[0]
            trk[:] = [pos[0], pos[1], pos[2], pos[3], 0]
            if np.any(np.isnan(pos)):
                to_del.append(t)
        trks = np.ma.compress_rows(np.ma.masked_invalid(trks))
        for t in reversed(to_del):
            self.trackers.pop(t)
        matched, unmatched_dets, unmatched_trks = associate_detections_to_trackers(dets,trks, self.iou_threshold)

        # MODIFIED: Kirill Karpenko
        # Objects to return:
        # - a dictionary matching tracker IDs (same as track IDs, in this implementation)
        #   to detection indices in `dets`.
        #   Is to hold both the mappings created by the association step in `associate_detections_to_trackers`
        #   and the newly created ones for unmatched detections.
        # tracker ID -> detection ID
        track_id_to_det_idx: Dict[int, int] = dict()
        # - a set containing tracker IDs (track IDs) with which no detections were matched.
        # NOTE: If a tracker is destroyed at the end of this update,
        # its ID is also removed from this set, if present.
        unmatched_track_ids: Set[int] = {
            self.trackers[idx].id
            for idx in unmatched_trks.tolist()
        }
        matched_confirmed_track_ids: Set[int] = set()
        matched_unconfirmed_track_ids: Set[int] = set()

        # MODIFIED: Kirill Karpenko
        # populate with matched detections and the corresponding track IDs
        track_id_to_det_idx.update({
            self.trackers[row[1].item()].id: row[0].item()
            for row in matched
        })

        # update matched trackers with assigned detections
        for m in matched:
            self.trackers[m[1]].update(dets[m[0], :])

        # create and initialise new trackers for unmatched detections
        for i in unmatched_dets:
            trk: KalmanBoxTracker = KalmanBoxTracker(dets[i,:])
            self.trackers.append(trk)
            # MODIFIED: Kirill Karpenko
            # add a mapping between the initialised tracker's ID and the detection index
            track_id_to_det_idx[trk.id] = i
        i = len(self.trackers)
        for trk in reversed(self.trackers): # type: KalmanBoxTracker
            d = trk.get_state()[0]
            # MODIFIED: Kirill Karpenko
            # - Now including in the output the predictions for ALL existing trackers,
            #   and passing sets with the IDs of confirmed vs unconfirmed vs unmatched tracks instead.
            # - Removed adding 1 to `trk.id` here and changed `KalmanBoxTracker` to start IDs from 1 instead.
            ret.append(np.concatenate((d, [trk.id])).reshape(1, -1))
            if trk.time_since_update < 1:
                is_confirmed_track: bool = (
                    trk.hit_streak >= self.min_hits
                    # MODIFIED: Kirill Karpenko
                    # Track confirmation logic now applies to first `min_hits` global frames too
                    # (the line below has been commented out).
                    # or self.frame_count <= self.min_hits
                )
                if is_confirmed_track:
                    matched_confirmed_track_ids.add(trk.id)
                else:
                    matched_unconfirmed_track_ids.add(trk.id)

            i -= 1
            # remove dead tracklet
            if trk.time_since_update > self.max_age:
                removed_tracker: KalmanBoxTracker = self.trackers.pop(i)
                # MODIFIED: Kirill Karpenko
                # remove this tracker's ID from `unmatched_track_ids`
                unmatched_track_ids.discard(removed_tracker.id)

        # MODIFIED: Kirill Karpenko
        # Instead of returning just ret (containing the tracker boxes and track IDs),
        # return a dictionary containing ret, matched, unmatched_dets, unmatched_trks.
        track_states = np.concatenate(ret) if len(ret) > 0 else np.empty((0,5))
        out_dict = {
            "track_states": track_states,
            "track_id_to_det_idx": track_id_to_det_idx,
            "unmatched_track_ids": unmatched_track_ids,
            "matched_confirmed_track_ids": matched_confirmed_track_ids,
            "matched_unconfirmed_track_ids": matched_unconfirmed_track_ids
        }
        return out_dict
