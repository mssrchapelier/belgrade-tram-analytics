from typing import List
import asyncio
from asyncio import Queue, TaskGroup
from asyncio.tasks import Task
from logging import Logger

from archive.v_a1.events import Event, ImageProcessedEvent
from archive.v_a1.item_stream import ItemStream
from archive.v_a1.uuid_gen import generate_uuid
from archive.v_a1.data_models import FrameItem, Detection, ImagePatch, PatchEmbedding, DetectionTrackMapping
from archive.v_a1.tracker import TrackerService
from archive.v_a1.detector import DetectionService, DetectionRaw
from archive.v_a1.patch_extractor import PatchExtractionService, Bbox, ImagePatchRaw
from archive.v_a1.feature_extractor import FeatureExtractionService, FeatureExtractionResultRaw
from archive.v_a1.frame_producer import FrameProducingService, FrameItemRaw


class MasterProcessor:

    def __init__(self):
        self.with_reid: bool = None
        self.database_link: str = None
        self.object_storage_link: str = None
        self.vector_storage_link: str = None
        self.log_path: str = None
        # the IDs of the detector and extractor to be used
        self._detector_id: str = None
        self._extractor_id: str = None

        # Note: for all frames from any single camera, they are supposed to come
        # in the chronological order (for the tracker to work properly).
        # TODO: possibly implement checking by timestamp
        self._message_queue: Queue[Event] = None
        self._image_processed_event_stream: ItemStream[ImageProcessedEvent] = None
        self._logger: Logger = self._get_logger(self.log_path)

        self._frame_producing_service: FrameProducingService = None
        self._tracker_service: TrackerService = None
        self._detection_service: DetectionService = None
        self._patch_extraction_service: PatchExtractionService = None
        self._feature_extraction_service: FeatureExtractionService = None

    async def start(self) -> None:
        self._logger.info("Service started")
        async for frame_item_raw in self._frame_producing_service: # type: FrameItemRaw
            frame_item: FrameItem = FrameItem(
                frame_id=generate_uuid(),
                **frame_item_raw.model_dump()
            )
            await self._process_single_frame(frame_item)
            # emit a "new image processed" event
            await self._image_processed_event_stream.publish(
                ImageProcessedEvent(frame_item.frame_id)
            )

    async def stop(self):
        self._logger.info("Service stopped")
        # ...


    async def _process_single_frame(self, frame_item: FrameItem) -> None:
        camera_id: str = frame_item.camera_id

        detections: List[Detection] = await self._get_detections(frame_item)

        image_patches: List[ImagePatch] | None = None
        patch_embeddings: List[PatchEmbedding] | None = None
        if detections:
            if self.with_reid:
                image_patches = await self._extract_patches(frame_item, detections)
                patch_embeddings = await self._get_patch_embeddings(image_patches)

        # obtain track ids / update tracker
        # NOTE: Tracker must be updated even if there are no detections.
        detection_track_mappings: List[DetectionTrackMapping] = await self._tracker_service.update_and_get_track_ids(
            detections, patch_embeddings, camera_id
        )

        # store results (frame, detections, embeddings, track ids)
        await self._store_frame_processing_results(
            frame_item, detections, patch_embeddings, detection_track_mappings
        )

    async def _store_frame_processing_results(self, frame_item: FrameItem,
                                              detections: List[Detection],
                                              patch_embeddings: List[PatchEmbedding],
                                              detection_track_mappings: List[DetectionTrackMapping]) -> None:
        # Note: Do not store the frame image if there were no detections.
        raise NotImplementedError()

    def _get_logger(self, log_path: str) -> Logger:
        raise NotImplementedError()

    # --- Wrappers for calling services ---

    async def _get_detections(self, frame_item: FrameItem) -> List[Detection]:
        """
        A wrapper for calling the detection service.
        """
        detections_raw: List[DetectionRaw] = await self._detection_service.detect_objects(
            frame_item.image, self._detector_id
        )
        # generate detection IDs
        detections: List[Detection] = [
            Detection(frame_id=frame_item.frame_id,
                      detection_id=generate_uuid(),
                      **det_raw.model_dump())
            for det_raw in detections_raw # type: DetectionRaw
        ]
        return detections

    async def _extract_patches(self, frame_item: FrameItem, detections: List[Detection]) -> List[ImagePatch]:
        """
        A wrapper for calling the patch extraction service.
        """
        bboxes: List[Bbox] = [
            Bbox(x1=det.x1, x2=det.x2, y1=det.y1, y2=det.y2)
            for det in detections # type: Detection
        ]
        patches_raw: List[ImagePatchRaw] = await self._patch_extraction_service.extract_patches(
            frame_item.image, bboxes
        )
        patches: List[ImagePatch] = [
            ImagePatch(patch_id=generate_uuid(),
                       detection_id=det.detection_id,
                       image=patch_raw.image)
            for patch_raw, det in zip(patches_raw, detections)
        ]
        return patches

    async def _get_patch_embedding(self, patch: ImagePatch) -> PatchEmbedding:
        extraction_result: FeatureExtractionResultRaw = await self._feature_extraction_service.extract_features(
            patch.image, self._extractor_id
        )
        embedding: PatchEmbedding = PatchEmbedding(
            patch_embedding_id=generate_uuid(),
            patch_id=patch.patch_id,
            embedding=extraction_result.embedding
        )
        return embedding

    async def _get_patch_embeddings(self, patches: List[ImagePatch]) -> List[PatchEmbedding]:
        with asyncio.TaskGroup as tg:  # type: TaskGroup
            tasks: List[Task] = [tg.create_task(self._get_patch_embedding(patch)) for patch in patches]
        embeddings: List[PatchEmbedding] = [task.result() for task in tasks]
        return embeddings
