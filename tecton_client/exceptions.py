from enum import Enum
from typing import Optional

from tecton_client.constants import MAX_MICRO_BATCH_SIZE
from tecton_client.constants import MIN_MICRO_BATCH_SIZE


class TectonException(Exception):
    """Base class for all Tecton specific exceptions."""


class TectonClientError(TectonException):
    """Raised for errors thrown by the Python client."""


class TectonServerException(TectonException):
    """Base class for exceptions raised when the Tecton API returns an error response."""

    STATUS_CODE: int


def INVALID_SERVER_RESPONSE(status_code: int, reason_phrase: str, message: str) -> str:
    """Error message for invalid server response.

    Args:
        status_code (int): The HTTP status code.
        reason_phrase (str): The HTTP reason phrase.
        message (str): The error message.

    Returns:
        str: The error message string.

    """
    error_message = f"{status_code} {reason_phrase}: {message}"
    return error_message


class InvalidParameterError(TectonClientError):
    """Raised when one or more parameters passed in the request are invalid."""


class InvalidURLError(TectonClientError):
    """Raised when the URL passed is invalid or empty."""


class InvalidMicroBatchSizeError(TectonClientError):
    """Raised when the micro batch size is invalid."""

    def __init__(self) -> None:
        """Initialize the error class with a custom error message related to micro batch size."""
        message = (
            f"Micro batch size for get features batch request must be between {MIN_MICRO_BATCH_SIZE} and "
            f"{MAX_MICRO_BATCH_SIZE}"
        )
        super().__init__(message)


class InvalidParameterMessage(str, Enum):
    """Error Messages for Invalid Parameters passed."""

    KEY = (
        "API Key cannot be empty. Please ensure that you have set the TECTON_API_KEY environment variable, "
        "or provided a valid API key."
    )
    URL = "Cannot connect to Tecton because the URL is invalid"

    WORKSPACE_NAME = "Workspace Name cannot be None or empty"
    FEATURE_SERVICE_NAME = "FeatureService Name cannot be None or empty"
    EMPTY_MAPS = "Both Join Key map and Request Context Map cannot be empty"


class UnsupportedTypeError(TectonClientError):
    """Raised when the data type of one or more parameters passed in is not supported by Tecton."""


def EMPTY_KEY_VALUE(key: str, value: str) -> str:
    """Exception message for when the key or value is empty.

    Args:
        key (str): The key provided.
        value (str): The value provided.

    Returns:
        str: The error message string.

    """
    message = f"Key/Value cannot be None or empty. Given key is: {key} and value is: {value}"
    return message


def INVALID_TYPE_KEY_VALUE(
    map_type: str, allowed_types: Optional[tuple] = None, key: Optional[str] = None, value: Optional[str] = None
) -> str:
    """Exception message for when the key or value in the map is not of a valid type.

    Args:
        map_type (str): Type of the request map (one of Join Key Map or Request Context Map).
        allowed_types (Optional[tuple]): Tuple of data types allowed for the value in the map.
        key (Optional[str]): The key provided.
        value (Optional[str]): The value provided.

    Returns:
        str: The error message string.

    """
    if key:
        return f"{map_type} keys can only be of (str) type. Given key for {map_type} is {key} of type {type(key)}"
    if value:
        allowed_types = tuple([str(val_type).split("'")[1] for val_type in allowed_types])
        return (
            f"{map_type} values can only be of types {allowed_types}. "
            f"Given value for {map_type} is {value} of type {type(value)}"
        )


class BadRequestError(TectonServerException):
    """Raised when the Tecton API returns a 400 Bad Request error."""

    STATUS_CODE = 400


class UnauthorizedError(TectonServerException):
    """Raised when the Tecton API returns a 401 Unauthorized error."""

    STATUS_CODE = 401


class ForbiddenError(TectonServerException):
    """Raised when the Tecton API returns a 403 Forbidden error."""

    STATUS_CODE = 403


class NotFoundError(TectonServerException):
    """Raised when the Tecton API returns a 404 Not Found error."""

    STATUS_CODE = 404


class ResourceExhaustedError(TectonServerException):
    """Raised when the Tecton API returns a 429 Resources Exhausted error."""

    STATUS_CODE = 429


class ServiceUnavailableError(TectonServerException):
    """Raised when the Tecton API returns a 503 Service Unavailable error."""

    STATUS_CODE = 503


class GatewayTimeoutError(TectonServerException):
    """Raised when the Tecton API returns a 504 Gateway Timeout error."""

    STATUS_CODE = 504


SERVER_ERRORS: dict = {error.STATUS_CODE: error for error in TectonServerException.__subclasses__()}
