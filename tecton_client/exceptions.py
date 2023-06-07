from enum import Enum


class TectonException(Exception):
    """
    Base class for all Tecton specific exceptions
    """


class TectonClientException(TectonException):
    """
    Base class for exceptions thrown by the Python client
    """


class TectonServerException(TectonException):
    """
    Base class for exceptions representing error
    response from Tecton API
    """


class InvalidParameterException(TectonClientException):
    """
    Base class for exceptions raised when one or more
    parameters passed are invalid
    """


class InvalidParameterMessage(str, Enum):
    """
    Error Messages for Invalid Parameters passed
    """

    KEY = "API Key cannot be empty"
    URL = "Cannot connect to Tecton because the URL is invalid"

    KEY_VALUE = "Key/Value cannot be None or empty"

    WORKSPACE_NAME = "Workspace Name cannot be None or empty"
    FEATURE_SERVICE_NAME = "FeatureService Name cannot be None or empty"
    REQUEST_MAPS = "Both Join Key map and Request Context Map " \
                   "cannot be empty"


def INVALID_TYPE_KEY(key: str, map: str) -> None:
    """
    Exception raised for when the key in the map is not of a valid type
    :param key: key provided
    :param map: name of the map
    :return: InvalidParameterException
    """
    msg = f"Join Key-Map keys and Request Context Map keys " \
          f"can only be of (str) type. Given key for {map} is: {key}"
    raise InvalidParameterException(msg)


def INVALID_TYPE_JOIN_VALUE(value: str) -> None:
    """
    Exception raised for when the value in the Join Key-map
    is not of a valid type

    :param value: key provided
    :return: InvalidParameterException
    """
    msg = f"Join Key-Map Values can only be of types " \
          f"(int, str). Given value is: {value}"
    raise InvalidParameterException(msg)


def INVALID_TYPE_REQ_VALUE(value: str) -> None:
    """
    Exception raised for when the value in the Request
    Context Map is not of a valid type

    :param value: key provided
    :return: InvalidParameterException
    """
    msg = f"Request Context Map values can only be of types " \
          f"(int, str, float). Given value is: {value}"
    raise InvalidParameterException(msg)
