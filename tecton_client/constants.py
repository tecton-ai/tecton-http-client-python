from typing import Final

SUPPORTED_JOIN_KEY_VALUE_TYPES: Final[set] = {int, str, type(None)}
"""set: The set of supported join key value types for :class:`GetFeaturesRequestData` object."""
SUPPORTED_REQUEST_CONTEXT_MAP_TYPES: Final[set] = {int, str, float}
"""set: The set of supported request context map value types for :class:`GetFeaturesRequestData` object."""

MIN_MICRO_BATCH_SIZE: Final[int] = 1
"""int: The minimum micro batch size allowed while making a :class:`GetFeaturesBatchRequest`."""
MAX_MICRO_BATCH_SIZE: Final[int] = 5
"""int: The maximum micro batch size allowed while making a :class:`GetFeaturesBatchRequest`."""
DEFAULT_MICRO_BATCH_SIZE: Final[int] = 1
"""int: The default micro batch size while making a :class:`GetFeaturesBatchRequest`."""
