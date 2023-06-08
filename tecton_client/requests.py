import json
from abc import ABC
from copy import deepcopy
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
    """Options for retrieving metadata for get-features request."""

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
    """Class for request data needed for get-features queries."""

    def __init__(self: Self, join_key_map: Optional[Dict[str,
                 Union[int, str, NoneType]]] = None,
                 request_context_map: Optional[Dict[str,
                                               Union[int, str, float]]]
                 = None) -> None:
        """Initializing a GetFeaturesRequestData instance with a
        join key map and/or request context map

        :param join_key_map: (Optional) Join keys used for table-based
        FeatureViews
        :param request_context_map: (Optional) Request context used for
        OnDemand FeatureViews
        """

        if join_key_map is None and request_context_map is None:
            raise InvalidParameterException(
                InvalidParameterMessage.EMPTY_MAPS.value)

        self.join_key_map: dict = \
            self.validate_parameters(join_key_map,
                                     True,
                                     SUPPORTED_JOIN_KEY_VALUE_TYPES) \
            if join_key_map else None

        self.request_context_map: dict = \
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
    """Base class for all requests to the Tecton API."""

    def __init__(self: Self, endpoint: str,
                 workspace_name: str,
                 feature_service_name: str) -> None:

        """Initializing the request endpoint, workspace name and
        feature service name

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
    """Base class for all requests to fetch feature values from Tecton API."""

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str,
                 metadata_options: Set["MetadataOptions"]
                 = MetadataOptions.defaults()) -> None:
        """Initializing the request endpoint, workspace name,
        feature service name and metadata options

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
    """Class that represents a request to the /get-features endpoint"""

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features"

    def __init__(self: Self, workspace_name: str,
                 feature_service_name: str,
                 request_data: GetFeatureRequestData,
                 metadata_options: Optional[Set["MetadataOptions"]] =
                 MetadataOptions.defaults()) -> None:
        """Initializing the workspace name, feature service name,
        request data and metadata options

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

        self_dict = deepcopy(vars(self))

        if self.request_data.join_key_map:
            self_dict["join_key_map"] = self.request_data.join_key_map
        if self.request_data.request_context_map:
            self_dict["request_context_map"] = \
                self.request_data.request_context_map

        fields_to_remove = ["endpoint", "request_data"]
        for field in fields_to_remove:
            self_dict.pop(field)

        final_metadata_options = {option.value: True for option in
                                  sorted(self.metadata_options,
                                         key=lambda x: x.value)} \
            if self.metadata_options else {}
        self_dict["metadata_options"] = final_metadata_options

        return self_dict

    def to_json_string(self: Self) -> str:
        """Returns a JSON representation of the GetFeaturesRequest
        :return: JSON formatted string
        """

        request_value_dictionary = self.to_dict()
        return json.dumps({"params": request_value_dictionary})
