from abc import ABC, abstractmethod

# a base proxy to call individial pipeline modules,
# whether deployed in the same thread, in a different thread,
# in a different process, or on a different node

class BasePipelineStageClient[InputT, OutputT](ABC):

    @abstractmethod
    async def predict(self, input_item: InputT) -> OutputT:
        pass