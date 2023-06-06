from enum import Enum


class TectonException(Exception):
    """
    Base class for all Tecton specific exceptions
    """


class TectonClientException(TectonException):
    """
    Raised for exceptions thrown by the client
    """


class TectonServerException(TectonException):
    """
    Raised when there are exceptions thrown by the server
    """


class InvalidParameterException(TectonClientException):
    """
    Raised when one or more parameters passed are empty or None
    """


class InvalidParameterMessages(str, Enum):
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
    FEATURE_VECTOR = "Received empty feature vector from Tecton"
