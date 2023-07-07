from abc import ABC
from dataclasses import dataclass
from enum import Enum
from itertools import zip_longest
from typing import Dict
from typing import Final
from typing import List
from typing import Optional
from typing import Set
from typing import Union

from tecton_client.constants import DEFAULT_MICRO_BATCH_SIZE
from tecton_client.constants import MAX_MICRO_BATCH_SIZE
from tecton_client.constants import MIN_MICRO_BATCH_SIZE
from tecton_client.constants import SUPPORTED_JOIN_KEY_VALUE_TYPES
from tecton_client.constants import SUPPORTED_REQUEST_CONTEXT_MAP_TYPES
from tecton_client.exceptions import EMPTY_KEY_VALUE
from tecton_client.exceptions import INVALID_TYPE_KEY_VALUE
from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.exceptions import UnsupportedTypeError


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


def _defaults() -> Set[MetadataOptions]:
    """Set the default options to include names and data types.

    Returns:
        Set[MetadataOptions]: The set of default :class:`MetadataOptions`.

    """
    return {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}


@dataclass
class GetFeaturesRequestData:
    """Class for request data needed for get-features queries.

    Attributes:
        join_key_map (Optional[Dict[str, Union[int, str, type(None)]]]):
            Join keys used for Batch and Stream FeatureViews
            The values can be of type (int, str, type(None)) and are encoded as follows:
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
        self,
        join_key_map: Optional[Dict[str, Union[int, str, type(None)]]] = None,
        request_context_map: Optional[Dict[str, Union[int, str, float]]] = None,
    ) -> None:
        """Initializes a :class:`GetFeaturesRequestData` instance with the given parameters.

        Args:
            join_key_map (Optional[Dict[str, Union[int, str, type(None)]]]):
                Join keys used for table-based FeatureViews.
                The key of this map is the join key name and the value is the join key value for this request.
            request_context_map (Optional[Dict[str, Union[int, str, float]]]):
                Request context used for OnDemand FeatureViews.
                The key of this map is the request context name,
                and the value is the request context value for this request.

        Raises:
            InvalidParameterError: Raised if
                (1) both join_key_map and request_context_map are None
                (2) key or value in either map is empty, or value in request_context_map is None
            UnsupportedTypeError: If the key is not a string or the value is not one of the allowed types.

        """
        if join_key_map is None and request_context_map is None:
            raise InvalidParameterError(InvalidParameterMessage.EMPTY_MAPS.value)

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
            InvalidParameterError: Raised if the key or value in either map is empty,
                or value in request_context_map is None
            UnsupportedTypeError: If the key is not a string or the value is not one of the allowed types.

        """
        for key, value in request_map.items():
            if not key:
                raise InvalidParameterError(EMPTY_KEY_VALUE(key, value))

            if type(key) != str:
                message = INVALID_TYPE_KEY_VALUE(map_type=map_type, key=key)
                raise UnsupportedTypeError(message)

            if value is not None:
                if type(value) not in tuple(allowed_types):
                    message = INVALID_TYPE_KEY_VALUE(map_type=map_type, allowed_types=tuple(allowed_types), value=value)
                    raise UnsupportedTypeError(message)

            if not allow_none and not value:
                raise InvalidParameterError(EMPTY_KEY_VALUE(key, value))
            if allow_none and value == "":
                raise InvalidParameterError(EMPTY_KEY_VALUE(key, value))

            request_map[key] = str(value) if type(value) == int else value

        return request_map


@dataclass
class TectonRequest(ABC):
    """Base class for all requests to the Tecton API.

    Attributes:
        workspace_name (str): Name of the workspace in which the Feature Service is defined.
        feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
    """

    def __init__(self, workspace_name: str, feature_service_name: str) -> None:
        """Initializing parameters required to make a request to the Tecton API.

        Args:
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.

        Raises:
            InvalidParameterError: If the workspace_name or feature_service_name is empty.

        """
        if not workspace_name:
            raise InvalidParameterError(InvalidParameterMessage.WORKSPACE_NAME.value)
        if not feature_service_name:
            raise InvalidParameterError(InvalidParameterMessage.FEATURE_SERVICE_NAME.value)

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
        self,
        workspace_name: str,
        feature_service_name: str,
        metadata_options: Set[MetadataOptions] = _defaults(),
    ) -> None:
        """Initializing an object with the given parameters.

        Args:
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
            metadata_options (Set[MetadataOptions]): Options for retrieving additional metadata about feature
                values. Defaults to the default set of metadata options.

        """
        super().__init__(workspace_name, feature_service_name)
        self.metadata_options = metadata_options.union(_defaults())


@dataclass
class GetFeaturesRequest(AbstractGetFeaturesRequest):
    """Class representing a request to the /get-features endpoint.

    Attributes:
        request_data: Request parameters for the query, consisting of a Join Key Map and/or a Request Context Map
            sent as a :class:`GetFeaturesRequestData` object.
        ENDPOINT: Endpoint string for the get-features API.

    Examples:
        >>> request_data = GetFeatureRequestData(join_key_map={"user_id": 1234})
        >>> get_features_request = GetFeaturesRequest("my_workspace", "my_feature_service", request_data=request_data)
        >>> get_features_request.to_json()
            {"params":{"feature_service_name": "my_feature_service","workspace_name": "my_workspace",
            "metadata_options": {"include_data_types": True, "include_names": True},"join_key_map": {"user_id": 1234}}}
    """

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features"

    def __init__(
        self,
        workspace_name: str,
        feature_service_name: str,
        request_data: GetFeaturesRequestData,
        metadata_options: Set[MetadataOptions] = _defaults(),
    ) -> None:
        """Initializing the :class:`GetFeaturesRequest` object with the given parameters.

        Args:
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
            request_data (GetFeaturesRequestData): Request parameters for the query.
            metadata_options (Set[MetadataOptions]): Options for retrieving additional metadata about feature
                values.

        """
        super().__init__(workspace_name, feature_service_name, metadata_options)
        self.request_data = request_data

    def to_json(self) -> dict:
        """Returns a JSON representation of the :class:`GetFeaturesRequest` object.

        Returns:
            JSON request to be sent to the API as a dictionary.

        """
        fields_to_remove = ["ENDPOINT", "request_data"]
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

        return {"params": self_dict}


@dataclass
class GetFeaturesMicroBatchRequest(AbstractGetFeaturesRequest):
    """Class representing a micro-batch request sent to the /get-features-batch endpoint.

    Attributes:
        ENDPOINT: Endpoint string for the get-features-batch API.
        request_data: Request parameters for the query, consisting of a list of :class:`GetFeatureRequestData` objects.
    """

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features-batch"

    def __init__(
        self,
        workspace_name: str,
        feature_service_name: str,
        request_data_list: List[GetFeatureRequestData],
        metadata_options: Set[MetadataOptions] = _defaults(),
    ) -> None:
        """Initializing the :class:`GetFeaturesMicroBatchRequest` object with the given parameters.

        Args:
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
            request_data_list (List[GetFeatureRequestData]): List of request parameters for the batch query.
            metadata_options (Set[MetadataOptions]): Options for retrieving additional metadata about feature
                values.

        """
        super().__init__(workspace_name, feature_service_name, metadata_options)
        self.request_data = request_data_list

    def to_json(self) -> dict:
        """Returns a JSON representation of the :class:`GetFeaturesMicroBatchRequest` object.

        Returns:
            JSON request to be sent to the API as a dictionary.

        """
        fields_to_remove = ["ENDPOINT"]
        self_dict = {key: value for key, value in vars(self).items() if key not in fields_to_remove}
        self_dict["metadata_options"] = (
            {option.value: True for option in sorted(self.metadata_options, key=lambda x: x.value)}
            if self.metadata_options
            else {}
        )
        self_dict["request_data"] = [
            {k: v for k, v in vars(request_data).items() if v} for request_data in self.request_data
        ]
        return {"params": self_dict}


class GetFeaturesBatchRequest(AbstractGetFeaturesRequest):
    """A class that represents a batch request to retrieve a list of feature vectors from the feature server.

    The class can be used to make parallel requests to retrieve multiple feature vectors from the feature server API.
    The actual number of concurrent calls depends on the `max_connections` configuration in
    :class:`TectonClientOptions` and the size of the connection pool.

    :class:`GetFeaturesBatchRequest` uses either the /get-features or the /get-features-batch endpoint depending on the
    configuration `micro_batch_size`. By default, the `micro_batch_size` is set to {DEFAULT_MICRO_BATCH_SIZE}.
    It can be configured to any value in the range [{MIN_MICRO_BATCH_SIZE}, {MAX_MICRO_BATCH_SIZE}].

    For a :class:`GetFeaturesBatchRequest` with a :class:`GetFeatureRequestData` of size `n` and a `micro_batch_size`
    of 1, the client enqueues `n` HTTP calls to be sent parallelly to the /get-features endpoint. The client waits
    until all calls are complete and returns a List of :class:`GetFeaturesResponse` objects of size `n`.

    For a :class:`GetFeaturesBatchRequest` with a :class:`GetFeatureRequestData` of size `n` and a `micro_batch_size`
    of `k` where `k` is in the range [{MIN_MICRO_BATCH_SIZE}, {MAX_MICRO_BATCH_SIZE}], the client enqueues
    math.ceil(n/k) microbatch requests to be sent parallelly to the /get-features-batch endpoint, waits until all
    microbatch requests are complete and returns a List of :class:`GetFeaturesResponse` objects of size `n`.


    Attributes:
        ENDPOINT: Endpoint string for the get-features-batch API.
        request_list: List of :class:`GetFeaturesRequest` objects or :class:`GetFeaturesMicroBatchRequest` objects,
            based on the `micro_batch_size` configuration.

    Examples:
        >>> request_data = GetFeatureRequestData(join_key_map={"user_id": 1234})
        >>> request = GetFeaturesBatchRequest(
        ...      workspace_name="my_workspace",
        ...      feature_service_name="my_feature_service",
        ...      request_data_list=[request_data, request_data],
        ...      metadata_options={MetadataOptions.NAME, MetadataOptions.DATA_TYPE},
        ...      micro_batch_size=2
        ...     )
        >>> request.to_json_list()
            {"params": {"feature_service_name": "my_feature_service", "workspace_name": "my_workspace",
            "metadata_options": {"include_data_types": true, "include_names": true},
            "request_data": [{"join_key_map": {"user_id": 1234}}, {"join_key_map": {"user_id": 1234}}]}}
    """

    def __init__(
        self,
        workspace_name: str,
        feature_service_name: str,
        request_data_list: List[GetFeatureRequestData],
        metadata_options: Set[MetadataOptions] = _defaults(),
        micro_batch_size: int = DEFAULT_MICRO_BATCH_SIZE,
    ) -> None:
        """Initializing the :class:`GetFeaturesBatchRequest` object with the given parameters.

        Args:
            workspace_name (str): Name of the workspace in which the Feature Service is defined.
            feature_service_name (str): Name of the Feature Service for which the feature vector is being requested.
            request_data_list (List[GetFeatureRequestData]): List of request parameters for the batch query.
            metadata_options (Set[MetadataOptions]): Options for retrieving additional metadata about feature
                values.
            micro_batch_size (int): (Optional) Number of requests to be sent in a single batch request. Defaults to
                {DEFAULT_MICRO_BATCH_SIZE}.

        """
        self._validate_batch_parameters(request_data_list, micro_batch_size)
        super().__init__(workspace_name, feature_service_name, metadata_options)

        if micro_batch_size == 1:
            self.request_list = [
                GetFeaturesRequest(
                    workspace_name=self.workspace_name,
                    feature_service_name=self.feature_service_name,
                    request_data=request_data,
                    metadata_options=self.metadata_options,
                )
                for request_data in request_data_list
            ]
            self.ENDPOINT = GetFeaturesRequest.ENDPOINT
        else:
            self.request_list = [
                GetFeaturesMicroBatchRequest(
                    workspace_name=self.workspace_name,
                    feature_service_name=self.feature_service_name,
                    metadata_options=self.metadata_options,
                    request_data_list=list(filter(None, sublist)),
                )
                for sublist in zip_longest(*[iter(request_data_list)] * micro_batch_size)
            ]
            self.ENDPOINT = GetFeaturesMicroBatchRequest.ENDPOINT

    def _validate_batch_parameters(self, request_data_list: List[GetFeatureRequestData], micro_batch_size: int) -> None:
        """Validates the parameters for the batch request.

        Args:
            request_data_list (List[GetFeatureRequestData]): List of request parameters for the batch query.
            micro_batch_size (int): Number of requests to be sent in a single batch.

        Raises:
            InvalidParameterError: If the request_list is empty or contains None values,
                or if the micro_batch_size is not in the expected range of values ({MIN_MICRO_BATCH_SIZE},
                {MAX_MICRO_BATCH_SIZE}).

        """
        if not request_data_list or None in request_data_list:
            message = "List of GetFeaturesRequestData for get-features-batch/ cannot be empty"
            raise InvalidParameterError(message)
        if micro_batch_size < MIN_MICRO_BATCH_SIZE or micro_batch_size > MAX_MICRO_BATCH_SIZE:
            message = (
                f"Micro batch size for get-features-batch/ must be between {MIN_MICRO_BATCH_SIZE} and "
                f"{MAX_MICRO_BATCH_SIZE}"
            )
            raise InvalidParameterError(message)

    def to_json_list(self) -> List[dict]:
        """Returns a list of JSON representations for requests in the :class:`GetFeaturesRequest` object.

        Returns:
            JSON requests to be sent to the API as a list of dictionaries.

        """
        return [request.to_json() for request in self.request_list]
