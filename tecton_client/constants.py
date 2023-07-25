from typing import Final

SUPPORTED_JOIN_KEY_VALUE_TYPES: Final[set] = {int, str, type(None)}
"""set: The set of supported data types for join key values for :class:`GetFeaturesRequestData` object."""
SUPPORTED_REQUEST_CONTEXT_MAP_TYPES: Final[set] = {int, str, float}
"""set: The set of supported data types for request context map values for :class:`GetFeaturesRequestData` object."""

MIN_MICRO_BATCH_SIZE: Final[int] = 1
"""int: The minimum micro batch size allowed while making a :class:`GetFeaturesBatchRequest`, currently set to 1."""
MAX_MICRO_BATCH_SIZE: Final[int] = 5
"""int: The maximum micro batch size allowed while making a :class:`GetFeaturesBatchRequest`, currently set to 5."""
DEFAULT_MICRO_BATCH_SIZE: Final[int] = 1
"""int: The default micro batch size while making a :class:`GetFeaturesBatchRequest`, currently set to 1."""
