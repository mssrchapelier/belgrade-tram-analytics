class ItemProcessingException(Exception):
    pass

class InputFetchTimeout(Exception):
    pass

class OutputPersistenceTimeout(Exception):
    pass


class IngestionDroppedItemException(Exception):
    description: str
