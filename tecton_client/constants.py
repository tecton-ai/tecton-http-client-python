from typing import Final

DEFAULT_PARALLEL_REQUEST_TIMEOUT: Final[int] = 2

SUPPORTED_JOIN_KEY_VALUE_TYPES: Final[set] = {int, str, type(None)}
SUPPORTED_REQUEST_CONTEXT_MAP_TYPES: Final[set] = {int, str, float}

MIN_MICRO_BATCH_SIZE: Final[int] = 1
MAX_MICRO_BATCH_SIZE: Final[int] = 10
DEFAULT_MICRO_BATCH_SIZE: Final[int] = 5

DEFAULT_PARALLEL_REQUEST_TIMEOUT: Final[int] = 2
