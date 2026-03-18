from uuid import uuid4

def generate_uuid() -> str:
    """
    A unified point of UUID generation (can use custom randomness sources, etc.)
    """
    return uuid4().hex