from typing import List, Dict

from archive.v_a1.data_models import Detection, PatchEmbedding, DetectionTrackMapping

class TrackerNotInitializedException(Exception):
    pass

class SingleTracker:

    async def update(
            self, detections: List[Detection], embeddings: List[PatchEmbedding] | None
    ) -> List[DetectionTrackMapping]:
        raise NotImplementedError

class TrackerService:

    # TODO: Possibly implement:
    #  - automatic stopping of a tracker if not updated for some time

    def __init__(self):
        # { camera_id: SingleTracker }
        self._trackers: Dict[str, SingleTracker] = dict()

    async def get_initialised_trackers(self) -> List[str]:
        """
        Return the camera IDs of cameras for which trackers have been initialised.
        """
        return sorted(list(self._trackers.keys()))

    async def start_for_cameras(self, camera_ids: List[str]) -> None:
        for camera_id in camera_ids:
            # if a tracker exists for this camera, ignore and do nothing
            # TODO: possibly pass a message to that effect
            if camera_id not in self._trackers:
                self._trackers[camera_id] = SingleTracker()

    async def stop_for_cameras(self, camera_ids: List[str]) -> None:
        # TODO: log or pass a message about tracker removal
        for camera_id in camera_ids:
            self._trackers.pop(camera_id, None)

    async def update_and_get_track_ids(
            self, detections: List[Detection], embeddings: List[PatchEmbedding] | None, camera_id: str
    ) -> List[DetectionTrackMapping]:
        if camera_id not in self._trackers:
            raise TrackerNotInitializedException(
                f"No tracker has been initialised for camera {camera_id}. Initialise one first."
            )
        return await self._trackers[camera_id].update(detections, embeddings)