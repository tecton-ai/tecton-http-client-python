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
    TYPE_JOIN_VALUE = "Join Key-Map Values can only be of types " \
                      "(int, str). Given value is:"
    TYPE_REQ_VALUE = "Request Context Map values can only be of types " \
                     "(int, str, float). Given value is:"
    TYPE_KEY = "Join Key-Map keys and Request Context Map keys " \
               "can only be of (str) type. Given key for %s is:"

    WORKSPACE_NAME = "Workspace Name cannot be None or empty"
    FEATURE_SERVICE_NAME = "FeatureService Name cannot be None or empty"
    REQUEST_MAPS = "Both Join Key map and Request Context Map " \
                   "cannot be empty"
