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
from tecton_client.exceptions import INVALID_TYPE_KEY_VALUE
from tecton_client.exceptions import InvalidParameterException
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.exceptions import UnsupportedTypeException

SUPPORTED_JOIN_KEY_VALUE_TYPES: Final[set] = {int, str, NoneType}
SUPPORTED_REQUEST_CONTEXT_MAP_TYPES: Final[set] = {int, str, float}


class MetadataOptions(str, Enum):
    """Options for retrieving metadata for get-features request."""

    NAME = "include_names"
    """Include the name of each feature in the vector"""

    EFFECTIVE_TIME = "include_effective_times"
    """Include the timestamp of the most recent feature value that was written to the online store"""

    DATA_TYPE = "include_data_types"
    """Include the data types of each feature in the vector"""

    SLO_INFO = "include_slo_info"
    """Include information about the server response time"""

    FEATURE_STATUS = "include_serving_status"
    """Include feature serving status information of the feature"""


def _defaults() -> Set["MetadataOptions"]:
    """Set the default options to include names and data types.

    Returns:
        Set[":class:`MetadataOptions`"]: The set of default :class:`MetadataOptions`.

    """
    return {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}


@dataclass
class GetFeatureRequestData:
    """Class for request data needed for get-features queries.

    Attributes:
        join_key_map (Optional[Dict[str, Union[int, str, NoneType]]]): Join keys used for Batch and Stream FeatureViews
            The values can be of type (int, str, NoneType) and are encoded as follows:
                    For string keys, the value should be a string.
                    For int keys, the value should be a string of the decimal representation of the integer.
        request_context_map (Optional[Dict[str, Union[int, str, float]]]):
            Request context used for OnDemand FeatureViews.
            The Request Context values can be of type (int, str, float) and are encoded as follows:
                    For string values, the value should be a string.
                    For int values, the value should be a string of the decimal representation of the integer.
                    For float values, the value should be a number.
    """

    def __init__(
        self: Self,
        join_key_map: Optional[Dict[str, Union[int, str, NoneType]]] = None,
        request_context_map: Optional[Dict[str, Union[int, str, float]]] = None,
    ) -> None:
        """Initializes a :class:`GetFeaturesRequestData` instance with the given parameters.

        Args:
            join_key_map (Optional[Dict[str, Union[int, str, NoneType]]]): Join keys used for table-based FeatureViews.
                The key of this map is the join key name and the value is the join key value for this request.
            request_context_map (Optional[Dict[str, Union[int, str, float]]]):
                Request context used for OnDemand FeatureViews.
                The key of this map is the request context name,
                and the value is the request context value for this request.

        Raises:
            InvalidParameterException: If both join_key_map and request_context_map are None,
                or key or value in either map is empty.
            UnsupportedTypeException: If the key is not a string or the value is not one of the allowed types.
            InvalidParameterException: If the value is None when allow_none is False,
                or if the value is an empty string when allow_none is True.
        """
        if join_key_map is None and request_context_map is None:
            raise InvalidParameterException(InvalidParameterMessage.EMPTY_MAPS.value)

        self.join_key_map = (
            self._get_processed_map(join_key_map, True, SUPPORTED_JOIN_KEY_VALUE_TYPES, map_type="Join Key-Map")
            if join_key_map
            else None
        )

        self.request_context_map = (
            self._get_processed_map(
                request_context_map, False, SUPPORTED_REQUEST_CONTEXT_MAP_TYPES, map_type="Request Context Map"
            )
            if request_context_map
            else None
        )

    @staticmethod
    def _get_processed_map(request_map: dict, allow_none: bool, allowed_types: set, map_type: str) -> dict:
        """Validates the parameters of the request.

        Args:
            request_map (dict): The map to validate.
            allow_none (bool): Whether the map allows None values or not.
            allowed_types (set): The allowed types for the values in the map.
            map_type (str): The type of the map to validate (Join Key-Map or Request Context Map).

        Returns:
            dict: The validated map with appropriate types for values.

        Raises:
            InvalidParameterException: If key or value in either map is empty.
            UnsupportedTypeException: If the key is not a string or the value is not one of the allowed types.
            InvalidParameterException: If the value is None when allow_none is False,
                or if the value is an empty string when allow_none is True.

        """
        for key, value in request_map.items():
            if not key:
                raise InvalidParameterException(EMPTY_KEY_VALUE(key, value))

            if type(key) != str:
                message = INVALID_TYPE_KEY_VALUE(map_type=map_type, key=key)
                raise UnsupportedTypeException(message)

            if value is not None:
                if type(value) not in tuple(allowed_types):
                    message = INVALID_TYPE_KEY_VALUE(map_type=map_type, allowed_types=tuple(allowed_types), value=value)
                    raise UnsupportedTypeException(message)

            if not allow_none and not value:
                raise InvalidParameterException(EMPTY_KEY_VALUE(key, value))
            if allow_none and value == "":
                raise InvalidParameterException(EMPTY_KEY_VALUE(key, value))

            request_map[key] = str(value) if type(value) == int else value

        return request_map


@dataclass
class TectonRequest(ABC):
    """Base class for all requests to the Tecton API.

    Attributes:
        endpoint (str): HTTP endpoint string to send the request to.
        workspace_name (str): Name of the workspace in which the Feature Service is defined.
        feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
    """

    def __init__(self: Self, endpoint: str, workspace_name: str, feature_service_name: str) -> None:
        """Initializing parameters required to make a request to the Tecton API.

        Args:
            endpoint (str): HTTP endpoint to send the request to.
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.

        Raises:
            InvalidParameterException: If the workspace_name or feature_service_name is empty.
        """
        if not workspace_name:
            raise InvalidParameterException(InvalidParameterMessage.WORKSPACE_NAME.value)
        if not feature_service_name:
            raise InvalidParameterException(InvalidParameterMessage.FEATURE_SERVICE_NAME.value)

        self.endpoint = endpoint
        self.feature_service_name = feature_service_name
        self.workspace_name = workspace_name


@dataclass
class AbstractGetFeaturesRequest(TectonRequest):
    """Base class for all requests to fetch feature values from the Tecton API.

    Attributes:
        metadata_options (Set[":class:`MetadataOptions`"]): Set of options for retrieving additional metadata about
            features.

    """

    def __init__(
        self: Self,
        endpoint: str,
        workspace_name: str,
        feature_service_name: str,
        metadata_options: Set["MetadataOptions"] = _defaults(),
    ) -> None:
        """Initializing an object with the given parameters.

        Args:
            endpoint (str): HTTP endpoint to send the request to.
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
            metadata_options (Set["MetadataOptions"]): Options for retrieving additional metadata about feature
                values. Defaults to the default set of metadata options.

        """
        super().__init__(endpoint, workspace_name, feature_service_name)
        self.metadata_options = metadata_options.union(_defaults())


@dataclass
class GetFeaturesRequest(AbstractGetFeaturesRequest):
    """Class representing a request to the /get-features endpoint.

    Attributes:
        request_data: Request parameters for the query, consisting of a Join Key Map and/or a Request Context Map
            sent as a :class:`GetFeaturesRequestData` object.
        ENDPOINT: Endpoint string for the get-features API.

    Examples:
        >>> request_data = GetFeaturesRequestData(join_key_map={"user_id": 1234})
        >>> get_features_request = GetFeaturesRequest("my_workspace", "my_feature_service", request_data=request_data)
        >>> get_features_request.to_json_string()
            {"params":{"feature_service_name": "my_feature_service","workspace_name": "my_workspace",
            "metadata_options": {"include_data_types": True, "include_names": True},"join_key_map": {"user_id": 1234}}}
    """

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features"

    def __init__(
        self: Self,
        workspace_name: str,
        feature_service_name: str,
        request_data: GetFeatureRequestData,
        metadata_options: Set["MetadataOptions"] = _defaults(),
    ) -> None:
        """Initializing the :class:`GetFeaturesRequest` object with the given parameters.

        Args:
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
            request_data (GetFeatureRequestData): Request parameters for the query.
            metadata_options (Set["MetadataOptions"]): Options for retrieving additional metadata about feature
                values.
        """
        super().__init__(GetFeaturesRequest.ENDPOINT, workspace_name, feature_service_name, metadata_options)
        self.request_data = request_data

    def to_json_string(self: Self) -> str:
        """Returns a JSON representation of the :class:`GetFeaturesRequest` object.

        Returns:
            JSON formatted string.
        """
        fields_to_remove = ["endpoint", "request_data"]
        self_dict = {key: value for key, value in vars(self).items() if key not in fields_to_remove}

        if self.request_data.join_key_map:
            self_dict["join_key_map"] = self.request_data.join_key_map
        if self.request_data.request_context_map:
            self_dict["request_context_map"] = self.request_data.request_context_map

        self_dict["metadata_options"] = (
            {option.value: True for option in sorted(self.metadata_options, key=lambda x: x.value)}
            if self.metadata_options
            else {}
        )

        return json.dumps({"params": self_dict})
