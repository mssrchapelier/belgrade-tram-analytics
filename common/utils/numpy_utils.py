from numpy import float64
from numpy.typing import NDArray, DTypeLike
from numpy.random import Generator, default_rng

def generate_random(*, n_items: int = 100,
                    upper_bound: float = 1000.0,
                    lower_bound: float = -1000.0,
                    seed: int | None = None) -> NDArray[float64]:
    if n_items <= 0:
        raise ValueError("n_items must be a positive integer")
    if not lower_bound <= upper_bound:
        raise ValueError("lower_bound must be less than or equal to upper_bound")
    scale: float = upper_bound - lower_bound

    gen: Generator = default_rng(seed)
    full_arr_unitinterval: NDArray[float64] = gen.random(size=n_items)
    full_arr: NDArray[float64] = full_arr_unitinterval * scale + lower_bound

    return full_arr