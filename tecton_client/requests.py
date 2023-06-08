import json
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from types import NoneType
from typing import Dict
from typing import Final
from typing import Optional
from typing import Self
from typing import Set
from typing import Union

from tecton_client.exceptions import EMPTY_KEY_VALUE
from tecton_client.exceptions import INVALID_TYPE_JOIN_VALUE
from tecton_client.exceptions import INVALID_TYPE_KEY
from tecton_client.exceptions import INVALID_TYPE_REQ_VALUE
from tecton_client.exceptions import InvalidParameterException
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.exceptions import UnsupportedTypeException

SUPPORTED_JOIN_KEY_VALUE_TYPES: Final[set] = {int, str, NoneType}
SUPPORTED_REQUEST_CONTEXT_MAP_TYPES: Final[set] = {int, str, float}


class MetadataOptions(str, Enum):
    """Options for retrieving metadata for get-features request.

    Attributes:
        NAME: Include the name of each feature in the vector
        EFFECTIVE_TIME: Include the timestamp of the most recent feature value
        that was written to the online store
        DATA_TYPE: Include the data types of each feature in the vector
        SLO_INFO: Include information about the server response time
        FEATURE_STATUS: Include feature serving status information
        of the feature
    """

    NAME = "include_names"
    EFFECTIVE_TIME = "include_effective_times"
    DATA_TYPE = "include_data_types"
    SLO_INFO = "include_slo_info"
    FEATURE_STATUS = "include_serving_status"

    @staticmethod
    def defaults() -> Set["MetadataOptions"]:
        """Setting default options to include names and data types
        :return: Set["MetadataOptions"]
        """
        return {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}


@dataclass
class GetFeatureRequestData:
    """Class for request data needed for get-features queries.

    Attributes:
        join_key_map: (Optional) Join keys used for table-based FeatureViews
        The key of this map is the join key name and the value is
        the join key value for this request
        For string keys, the value should be of type (str)
        For int64 keys, the value should be (str) of the decimal
        representation of the integer

        request_context_map: (Optional) Request context used for
        OnDemand FeatureViews
        The key of this map is the request context name and the value
        is the request context value for this request
        For string values, the value should be of type (str)
        For int64 values, the value should be (str) of the decimal
        representation of the integer
        For double values, the value should be of type (float)
    """

    def __init__(self: Self, join_key_map: Optional[Dict[str,
                 Union[int, str, NoneType]]] = None,
                 request_context_map: Optional[Dict[str,
                                               Union[int, str, float]]]
                 = None) -> None:
        """Initializing a GetFeaturesRequestData instance with the
        given parameters

        :param join_key_map: (Optional) Join keys used for table-based
        FeatureViews
        :param request_context_map: (Optional) Request context used for
        OnDemand FeatureViews
        """

        if join_key_map is None and request_context_map is None:
            raise InvalidParameterException(
                InvalidParameterMessage.EMPTY_MAPS.value)

        self.join_key_map = \
            self.validate_parameters(join_key_map,
                                     True,
                                     SUPPORTED_JOIN_KEY_VALUE_TYPES) \
            if join_key_map else None

        self.request_context_map = \
            self.validate_parameters(request_context_map, False,
                                     SUPPORTED_REQUEST_CONTEXT_MAP_TYPES) \
            if request_context_map else None

    @staticmethod
    def validate_parameters(map: dict,
                            allow_none: bool,
                            allowed_types: set) -> dict:
        """Validates the parameters of the request
        :param map: The map to validate
        :param allow_none: Whether the map allows None values or not
        :param allowed_types: The allowed types for the values in the map
        """

        for key, value in map.items():
            if not key:
                raise InvalidParameterException(EMPTY_KEY_VALUE(key, value))

            if not allow_none and not value:
                raise InvalidParameterException(EMPTY_KEY_VALUE(key, value))
            if allow_none and value == "":
                raise InvalidParameterException(EMPTY_KEY_VALUE(key, value))

            if not isinstance(key, str):
                message = INVALID_TYPE_KEY(key,
                                           "Join Key-Map" if allow_none
                                           else "Request Context Map")

                raise UnsupportedTypeException(message)

            if not isinstance(value, tuple(allowed_types)):
                message = INVALID_TYPE_JOIN_VALUE(value) if allow_none \
                    else INVALID_TYPE_REQ_VALUE(value)
                raise UnsupportedTypeException(message)

            map[key] = str(value) if isinstance(value, int) else value

        return map


@dataclass
class TectonRequest(ABC):
    """Base class for all requests to the Tecton API.

    Attributes:
        endpoint: HTTP endpoint string to send request to
        workspace_name: Name of the workspace in which the
        Feature Service is defined (string)
        feature_service_name: Name of the Feature Service for
        which the feature vector is being requested (string)
    """

    def __init__(self: Self, endpoint: str,
                 workspace_name: str,
                 feature_service_name: str) -> None:

        """Initializing parameters required to make a request
        to the Tecton API

        :param endpoint: HTTP endpoint to send request to
        :param workspace_name: Name of the workspace in which the
        Feature Service is defined
        :param feature_service_name: Name of the Feature Service for which
        the feature vector is being requested
        """

        if not workspace_name:
            raise InvalidParameterException(
                InvalidParameterMessage.WORKSPACE_NAME.value)
        if not feature_service_name:
            raise InvalidParameterException(
                InvalidParameterMessage.FEATURE_SERVICE_NAME.value)

        self.endpoint = endpoint
        self.feature_service_name = feature_service_name
        self.workspace_name = workspace_name


@dataclass
class AbstractGetFeaturesRequest(TectonRequest):
    """Base class for all requests to fetch feature values from Tecton API.

    Attributes:
        metadata_options: A set of options for retrieving
        additional metadata about feature values of type MetadataOptions
    """

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str,
                 metadata_options: Set["MetadataOptions"]
                 = MetadataOptions.defaults()) -> None:
        """Initializing an object with the given parameters

        :param endpoint: HTTP endpoint to send request to
        :param workspace_name: Name of the workspace in which
        the Feature Service is defined
        :param feature_service_name: Name of the Feature Service for which
        the feature vector is being requested
        :param metadata_options: Options for retrieving additional metadata
        about feature values
        """

        super().__init__(endpoint, workspace_name, feature_service_name)
        self.metadata_options = metadata_options.union(
            MetadataOptions.defaults())


@dataclass
class GetFeaturesRequest(AbstractGetFeaturesRequest):
    """Class that represents a request to the /get-features endpoint

    Attributes:
        request_data: Request parameters for the query,
        consisting of a Join Key Map and/or a Request context Map
        sent as a GetFeaturesRequestData object
        ENDPOINT: get-features endpoint string to send requests to
    """

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features"

    def __init__(self: Self, workspace_name: str,
                 feature_service_name: str,
                 request_data: GetFeatureRequestData,
                 metadata_options: Optional[Set["MetadataOptions"]] =
                 MetadataOptions.defaults()) -> None:
        """Initializing the GetFeaturesRequest object
        with the given parameters

        :param workspace_name: Name of the workspace in which
        the Feature Service is defined
        :param feature_service_name: Name of the Feature Service for which
        the feature vector is being requested
        :param request_data: Request parameters for the query
        :param metadata_options: (Optional) Options for retrieving additional
        metadata about feature values
        """

        super().__init__(GetFeaturesRequest.ENDPOINT,
                         workspace_name,
                         feature_service_name,
                         metadata_options)

        self.request_data = request_data

    def to_dict(self: Self) -> dict:

        fields_to_remove = ["endpoint", "request_data"]
        self_dict = {key: value for key, value in
                     vars(self).items()
                     if key not in fields_to_remove}

        if self.request_data.join_key_map:
            self_dict["join_key_map"] = self.request_data.join_key_map
        if self.request_data.request_context_map:
            self_dict["request_context_map"] = \
                self.request_data.request_context_map

        self_dict["metadata_options"] = {option.value: True for option in
                                         sorted(self.metadata_options,
                                                key=lambda x: x.value)} \
            if self.metadata_options else {}

        return self_dict

    def to_json_string(self: Self) -> str:
        """Returns a JSON representation of the GetFeaturesRequest
        :return: JSON formatted string
        """

        request_value_dictionary = self.to_dict()
        return json.dumps({"params": request_value_dictionary})
