from asyncio import Queue

class DropHeadQueue[T](Queue[T]):

    def put_nowait_drophead(self, item: T) -> None:
        if self.full():
            # get the oldest item and discard it
            oldest: T = self.get_nowait()
        # put the new item
        self.put_nowait(item)