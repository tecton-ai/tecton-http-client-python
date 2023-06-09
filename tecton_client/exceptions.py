from enum import Enum
from typing import Optional


class TectonException(Exception):
    """Base class for all Tecton specific exceptions"""


class TectonClientException(TectonException):
    """Base Class for exceptions thrown by the Python client"""


class TectonServerException(TectonException):
    """Base Class for exceptions representing error response from Tecton API"""


def INVALID_SERVER_RESPONSE(status_code: int, reason_phrase: str, message: str) -> str:
    """Error message for invalid server response

    :param status_code: HTTP status code
    :param reason_phrase: HTTP reason phrase
    :param message: Error message
    """
    error_message = f"{status_code} {reason_phrase}: {message}"

    return error_message


class EmptyParameterException(TectonClientException):
    """Class for exceptions raised when one or more parameters passed are invalid"""


class InvalidURLException(TectonClientException):
    """Class for exceptions raised when the URL passed is invalid"""


class InvalidParameterMessage(str, Enum):
    """Error Messages for Invalid Parameters passed."""

    KEY = "API Key cannot be empty"
    URL = "Cannot connect to Tecton because the URL is invalid"

    WORKSPACE_NAME = "Workspace Name cannot be None or empty"
    FEATURE_SERVICE_NAME = "FeatureService Name cannot be None or empty"
    EMPTY_MAPS = "Both Join Key map and Request Context Map cannot be empty"


class UnsupportedTypeException(TectonClientException):
    """Class for exceptions raised when the type of parameter passed is not supported by the client"""


def EMPTY_KEY_VALUE(key: str, value: str) -> str:
    """Exception message for when the key or value is empty

    :param key: key provided
    :param value: value provided
    :return: Error message string
    """
    message = f"Key/Value cannot be None or empty. Given key is: {key} and value is: {value}"
    return message


def INVALID_TYPE_KEY_VALUE(map_type: str, key: Optional[str] = None, value: Optional[str] = None) -> str:
    """Exception message for when the key in the map is not of a valid type

    :param key: key provided
    :param map_type: name of the map that's raising exception (one of Join Key Map or Request Context Map)
    :param value: value provided
    :return: Error message string
    """

    if key:
        return f"Join Key-Map keys and Request Context Map keys can only be of (str) type. " \
               f"Given key for {map_type} is: {key}"
    if value:
        if map_type == "Join Key-Map":
            message = f"Join Key-Map Values can only be of types (int, str). Given value is: {value}"
        else:
            message = f"Request Context Map values can only be of types (int, str, float). Given value is: {value}"

        return message
