from typing import TypeAlias, Never, TYPE_CHECKING

from tqdm.std import tqdm

if TYPE_CHECKING:
    # the stub for tqdm is defined is being generic; defining the type as Never for type checkers
    # see more here: https://github.com/tqdm/tqdm/issues/1601
    ManualTqdm: TypeAlias = tqdm[Never]
else:
    # the generic parameter instantiation will be treated as subscription;
    # define as the bare class for runtime
    ManualTqdm: TypeAlias = tqdm