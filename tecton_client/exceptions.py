from enum import Enum
from typing import Optional


class TectonException(Exception):
    """Base class for all Tecton specific exceptions."""


class TectonClientException(TectonException):
    """Raised for exceptions thrown by the Python client."""


class TectonServerException(TectonException):
    """Raised for error responses received from the Tecton API."""


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


class InvalidParameterError(TectonClientException):
    """Raised when one or more parameters passed in the request are invalid."""


class InvalidURLError(TectonClientException):
    """Raised when the URL passed is invalid, such as the URL being empty or not having a valid netloc."""


class InvalidParameterMessage(str, Enum):
    """Error Messages for Invalid Parameters passed."""

    KEY = "API Key cannot be empty"
    URL = "Cannot connect to Tecton because the URL is invalid"

    WORKSPACE_NAME = "Workspace Name cannot be None or empty"
    FEATURE_SERVICE_NAME = "FeatureService Name cannot be None or empty"
    EMPTY_MAPS = "Both Join Key map and Request Context Map cannot be empty"


class UnsupportedTypeError(TectonClientException):
    """Raised when the type of parameter passed is not supported by the client."""


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


class ResponseRelatedErrorMessage(str, Enum):
    """Error messages for response related exceptions."""

    MALFORMED_FEATURE_NAME = (
        "Feature name provided is not in the expected format of 'namespace.name'."
        "If problem persists, please contact Tecton Support for assistance."
    )


def UNKNOWN_TYPE(data_type: str) -> str:
    """Error message when the type of one or more feature values in the response received is not known.

    Args:
        data_type (str): Type of the response received

    Returns:
        str: The error message string.
    """
    message = (
        f"Received unknown data type {data_type} in the response."
        f"If problem persists, please contact Tecton Support for assistance."
    )
    return message


def MISMATCHED_TYPE(value: str, data_type: str) -> str:
    """Error message for when the type of one or more feature values in the response received
    does not match the expected type.

    Args:
        value (str): Value of the response received.
        data_type (str): Type of the response received.

    Returns:
        str: The error message string.
    """
    message = (
        f"Unexpected Error occurred while parsing the feature value {value} to data type {data_type}. "
        f"If problem persists, please contact Tecton Support for assistance."
    )
    return message


def MISSING_EXPECTED_METADATA(metadata: str) -> str:
    """Error message for when the expected metadata is missing from the response.

    Args:
        metadata (str): The metadata field.

    Returns:
        str: The error message string.

    """
    message = (
        f"Required metadata {metadata} is missing from the response."
        f"If problem persists, please contact Tecton Support for assistance."
    )
    return message
