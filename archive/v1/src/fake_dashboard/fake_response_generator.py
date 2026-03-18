from random import random, randint
from typing import List, Generator
from uuid import uuid4

from pydantic import BaseModel

MIN_TRAM_SPEED: float = 0.0
MAX_TRAM_SPEED: float = 50.0
MIN_TRAMS: int = 0
MAX_TRAMS: int = 5


class FakeTram(BaseModel):
    tram_id: str
    speed: float


class FakeResponse(BaseModel):
    num_trams: int
    trams: List[FakeTram]


def _fake_tram_generator() -> FakeTram:
    tram_id: str = uuid4().hex
    speed: float = random() * (MAX_TRAM_SPEED - MIN_TRAM_SPEED)
    return FakeTram(tram_id=tram_id, speed=speed)


def _fake_response_generator() -> Generator[FakeResponse]:
    while True:
        num_trams: int = randint(MIN_TRAMS, MAX_TRAMS)
        trams: List[FakeTram] = list(map(lambda _: _fake_tram_generator(), range(num_trams)))
        yield FakeResponse(num_trams=num_trams, trams=trams)
