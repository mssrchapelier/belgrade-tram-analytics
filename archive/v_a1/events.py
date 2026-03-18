from abc import ABC

class Event(ABC):
    pass

class ImageProcessedEvent(Event):

    """
    Dummy event for when an image has been processed by the processing pipeline.
    """

    def __init__(self, frame_id: str):
        self.frame_id: str = frame_id