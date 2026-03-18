from typing import TypeAlias, Tuple, TypeIs

PlanarPosition: TypeAlias = Tuple[float, float] # (x, y)
PixelPosition: TypeAlias = Tuple[int, int] # (x, y)
ColorTuple: TypeAlias = Tuple[int, int, int]

def is_planar_position(pt: Tuple[float, ...]) -> TypeIs[PlanarPosition]:
    return (isinstance(pt, tuple)
            and len(pt) == 2
            and all(isinstance(coord, float) for coord in pt))

def ensure_is_planar_position(pt: Tuple[float, ...]) -> PlanarPosition:
    if not is_planar_position(pt):
        raise ValueError(f"Not a PlanarPosition: {pt}")
    return pt

def _validate_odd(value: int) -> int:
    if value % 2 == 0:
        raise ValueError(f"value must be an odd number, received: {value}")
    return value
