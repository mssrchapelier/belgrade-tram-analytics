from pydantic import BaseModel

class NumberObservation(BaseModel):
    frame_ts: float
    number: int

class EmittedNumber(BaseModel):
    frame_id: str
    number: int

class EmittedNumberAsStored(BaseModel):
    camera_id: str
    frame_id: str
    frame_ts: float
    session_id: int
    seq_num: int

    number: int

class Square(BaseModel):
    frame_id: str
    square: int

class SumSquares(BaseModel):
    frame_id: str
    sum: int