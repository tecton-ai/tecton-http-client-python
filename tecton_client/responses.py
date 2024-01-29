from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from tecton_client.constants import MAX_MICRO_BATCH_SIZE
from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import DataType
from tecton_client.data_types import FloatType
from tecton_client.data_types import get_data_type
from tecton_client.data_types import IntType
from tecton_client.data_types import NameAndType
from tecton_client.data_types import StringType
from tecton_client.data_types import StructType
from tecton_client.exceptions import InvalidMicroBatchSizeError
from tecton_client.exceptions import TectonClientError
from tecton_client.http_client import HTTPResponse
from tecton_client.utils import parse_string_to_isotime


class Value:
    """Represents an object containing a feature value with a specific type."""

    def __init__(self, data_type: DataType, feature_value: Union[str, None, list]) -> None:
        """Set the value of the feature in the specified type.

        Args:
            data_type (DataType): The type of the feature value.
            feature_value (Union[str, None, list]): The value of the feature that needs to be converted to the specified
                type.

        Raises:
            TectonClientError: If the feature value cannot be converted to the specified type or
                if the specified type is not supported.

        """
        self._value = {}
        self._data_type = data_type

        type_conversion_map = {
            IntType: int,
            FloatType: float,
            StringType: lambda x: x,
            BoolType: bool,
            ArrayType: lambda x: [Value(data_type.element_type, value) for value in x],
            StructType: lambda x: {
                field.name: Value(field.data_type, x[i]) for i, field in enumerate(data_type.fields)
            },
        }

        if data_type.__class__ in type_conversion_map:
            convert = type_conversion_map[data_type.__class__]

            try:
                self._value[data_type.__str__()] = None if feature_value is None else convert(feature_value)
            except Exception:
                message = (
                    f"Unexpected Error occurred while parsing the feature value {feature_value} "
                    f"to data type {data_type.__str__()}. "
                    f"If problem persists, please contact Tecton Support for assistance."
                )
                raise TectonClientError(message)
        else:
            message = (
                f"Received unknown data type {data_type.__str__()} in the response."
                f"If problem persists, please contact Tecton Support for assistance."
            )
            raise TectonClientError(message)

    @property
    def value(self) -> Union[int, float, str, bool, list, dict, None]:
        """Return the feature value of the feature in the specified type.

        Returns:
            Union[int, float, str, bool, list, dict, None]: The value of the feature in the specified type.

        """
        if self._value[self._data_type.__str__()] is None:
            return None

        if isinstance(self._data_type, StructType):
            return {field: value.value for field, value in self._value[self._data_type.__str__()].items()}

        elif isinstance(self._data_type, ArrayType):
            return [value.value for value in self._value[self._data_type.__str__()]]

        else:
            return self._value[self._data_type.__str__()]


class FeatureStatus(str, Enum):
    """Enum to represent the serving status of a feature."""

    PRESENT = "PRESENT"
    """The feature values were found in the online store for the join keys requested."""

    MISSING_DATA = "MISSING_DATA"
    """The feature values were not found in the online store either because the join keys do not exist
    or the feature values are outside ttl."""

    UNKNOWN = "UNKNOWN"
    """An unknown status code occurred, most likely because an error occurred during feature retrieval."""


class FeatureValue:
    """Class encapsulating all the data for a Feature value returned from a GetFeatures API call.

    Attributes:
        data_type (:class:`DataType`): The type of the feature value. Tecton supports the following data types:
            Int, Float, String, Bool, Array, and Struct.
        feature_value (Union[str, int, float, bool, list, dict, None]): The value of the feature.
        feature_namespace (str): The namespace that the feature belongs to.
        feature_name (str): The name of the feature.
        feature_status (:class:`FeatureStatus`): The status of the feature.
        effective_time (datetime): The effective serving time for this feature.
            This is the most recent time that's aligned to the interval for which a full aggregation is available for
            this feature. Passing this in the spine of an offline feature request should guarantee retrieving the same
            value as is in this response.
    """

    def __init__(
        self,
        name: str,
        data_type: str,
        feature_value: Union[str, None, list],
        effective_time: Optional[str] = None,
        element_type: Optional[dict] = None,
        fields: Optional[list] = None,
        feature_status: Optional[str] = None,
    ) -> None:
        """Initialize a :class:`FeatureValue` object.

        Args:
            name (str): The name of the feature.
            data_type (str): String that indicates the type of the feature value.
            feature_value (Union[str, None, list]): The value of the feature.
            effective_time (Optional[str]): The effective serving time of the feature, sent as ISO-8601 format string.
            element_type (Optional[dict]): A dict representing the type of the elements in the array,
                present when the data_type is :class:`ArrayType`.
            fields (Optional[list]):  A list representing the fields of the struct, present when the data_type is
                :class:`StructType`.
            feature_status (Optional[str]): The status string of the feature value.

        Raises:
            TectonClientError: If the name of the feature is not in the format of <namespace>.<feature_name>.

        """
        try:
            self.feature_namespace, self.feature_name = name.split(".")
        except ValueError:
            message = (
                f"Feature name provided {name} is not in the expected format of 'namespace.name'."
                f"If problem persists, please contact Tecton Support for assistance."
            )
            raise TectonClientError(message)

        self.feature_status = FeatureStatus(feature_status) if feature_status else None

        self.effective_time = parse_string_to_isotime(effective_time) if effective_time else None
        if effective_time and not self.effective_time:
            message = f"Invalid datetime string: {effective_time}. Please contact Tecton Support for assistance."
            raise TectonClientError(message)

        self.data_type = get_data_type(data_type, element_type, fields)
        self.feature_value = Value(self.data_type, feature_value).value

    def __str__(self) -> str:
        """Return a string representation of the FeatureValue object."""
        return f"{self.feature_namespace}.{self.feature_name}: {self.feature_value}"


class SloIneligibilityReason(str, Enum):
    """Reasons due to which the Feature Serving Response may be ineligible for SLO."""

    UNKNOWN = "UNKNOWN"
    """Reason is unknown."""

    DYNAMODB_RESPONSE_SIZE_LIMIT_EXCEEDED = "DYNAMODB_RESPONSE_SIZE_LIMIT_EXCEEDED"
    """The 2MiB limit for DynamoDB response size was exceeded."""

    REDIS_RESPONSE_SIZE_LIMIT_EXCEEDED = "REDIS_RESPONSE_SIZE_LIMIT_EXCEEDED"
    """The 2MiB limit for Redis response size was exceeded."""

    REDIS_LATENCY_LIMIT_EXCEEDED = "REDIS_LATENCY_LIMIT_EXCEEDED"
    """The 25ms limit for Redis retrieval latency was exceeded."""


@dataclass
class SloInformation:
    """Class that represents SLO Information provided by Tecton when serving feature values.

    Refer to the official documentation
    `here <https://docs.tecton.ai/docs/beta/monitoring/production-slos#feature-service-metadata>`_ for more information.

    Attributes:
        slo_eligible (Optional[bool]): Whether the request was eligible for the latency SLO.
        server_time_seconds (Optional[float]): This includes the total time spent in the feature server, including
            online transforms and store latency.
        slo_server_time_seconds (Optional[float]): The server time minus any time spent waiting for online transforms to
            finish after all table transforms have finished. This is the indicator used for determining whether
            we are meeting the SLO.
        store_max_latency (Optional[float]): Max latency observed by the request from the store in seconds.
            Tecton fetches multiple feature views in parallel.
        store_response_size_bytes (Optional[int]): Total store response size in bytes.
        dynamodb_response_size_bytes (Optional[int]): The total DynamoDB response size processed to serve this request,
            in bytes. If this is greater than 2 MiB (i.e. 2097152), then the request is not SLO-Eligible.
        slo_ineligibility_reasons (Optional[List[SloIneligibilityReason]]): List of one or more reasons indicating why
            the feature was not SLO eligible. Only present if slo_eligible is False.
    """

    def __init__(self, slo_information: dict) -> None:
        """Initialize a SloInformation object.

        Args:
            slo_information (dict): The SLO information dictionary received from the server.

        """
        self.slo_eligible = slo_information.get("sloEligible")
        self.server_time_seconds = slo_information.get("serverTimeSeconds")
        self.slo_server_time_seconds = slo_information.get("sloServerTimeSeconds")
        self.store_max_latency = slo_information.get("storeMaxLatency")
        self.store_response_size_bytes = slo_information.get("storeResponseSizeBytes")
        self.dynamodb_response_size_bytes = slo_information.get("dynamoDBResponseSizeBytes")
        self.slo_ineligibility_reasons = (
            [SloIneligibilityReason(reason) for reason in slo_information["sloIneligibilityReasons"]]
            if "sloIneligibilityReasons" in slo_information
            else None
        )


@dataclass
class BatchSloInformation:
    """Class that represents the batch SLO Information provided by Tecton when serving feature values.

    Refer to the official documentation
    `here <https://docs.tecton.ai/docs/beta/monitoring/production-slos#feature-service-metadata>`_ for more information.

    Attributes:
        slo_eligible (Optional[bool]): Whether the request was eligible for the latency SLO.
        server_time_seconds (Optional[float]): This includes the total time spent in the feature server, including
            online transforms and store latency.
        slo_server_time_seconds (Optional[float]): The server time minus any time spent waiting for online transforms to
            finish after all table transforms have finished. This is the indicator used for determining whether
            we are meeting the SLO.
        store_max_latency (Optional[float]): Max latency observed by the request from the store in seconds.
            Tecton fetches multiple feature views in parallel.
        slo_ineligibility_reasons (Optional[List[SloIneligibilityReason]]): List of one or more reasons indicating why
            the feature was not SLO eligible. Only present if slo_eligible is False.
    """

    def __init__(self, batch_slo_information: dict) -> None:
        """Initializes a :class:`BatchSloInformation` object containing information on the batch request's SLO.

        Args:
           batch_slo_information (dict): The batch SLO information dictionary received from the server.

        """
        self.slo_eligible = batch_slo_information.get("sloEligible")
        self.server_time_seconds = batch_slo_information.get("serverTimeSeconds")
        self.slo_server_time_seconds = batch_slo_information.get("sloServerTimeSeconds")
        self.store_max_latency = batch_slo_information.get("storeMaxLatency")
        self.slo_ineligibility_reasons = (
            [SloIneligibilityReason(reason) for reason in batch_slo_information["sloIneligibilityReasons"]]
            if "sloIneligibilityReasons" in batch_slo_information
            else None
        )


class GetFeaturesResponse:
    """Response object for GetFeatures API call.

    Attributes:
        feature_values (Dict[str, FeatureValue]): Dictionary with feature names as keys and their corresponding feature
            values, one for each feature in the feature vector.
        slo_info (Optional[SloInformation]): :class:`SloInformation` object containing information on the feature
            vector's SLO, present only if the :class:`MetadataOption` `SLO_INFO` is requested in the request.
        request_latency (timedelta): The response time for GetFeatures API call (network latency + online store
            latency).
    """

    def __init__(
        self,
        http_response: Optional[HTTPResponse] = None,
        feature_values: Optional[Dict[str, FeatureValue]] = None,
        slo_info: Optional[SloInformation] = None,
        request_latency: timedelta = timedelta(seconds=0),
    ) -> None:
        """Initializes the object with data from the response.

        Args:
            http_response (Optional[HTTPResponse]): (Optional) The HTTP response object returned from the GetFeatures
                API call. Should be provided if the `feature_values` and `slo_info` parameters are not provided.
            feature_values (Optional[Dict[str, FeatureValue]]): (Optional) Dictionary with feature names as keys and
                their corresponding feature values, one for each feature in the feature vector.
                Should be provided if the `http_response` parameter is not provided.
            slo_info (Optional[SloInformation]): (Optional) :class:`SloInformation` object containing information on
                the feature vector's SLO, present only if the :class:`MetadataOption` `SLO_INFO` is requested in the
                request. Should be provided if the `http_response` parameter is not provided.
            request_latency (timedelta): (Optional) The response time for GetFeatures API call (network latency +
                online store latency). Should be provided when the `http_response` parameter is not provided.

        """
        if http_response:
            assert feature_values is None
            response = http_response.result
            feature_vector: list = response["result"]["features"]
            feature_metadata: List[dict] = response["metadata"]["features"]

            self.feature_values: Dict[str, FeatureValue] = {
                metadata["name"]: FeatureValue(
                    name=metadata["name"],
                    data_type=metadata["dataType"]["type"],
                    element_type=metadata["dataType"].get("elementType"),
                    effective_time=metadata.get("effectiveTime"),
                    feature_status=metadata.get("status"),
                    fields=metadata["dataType"].get("fields"),
                    feature_value=feature,
                )
                for feature, metadata in zip(feature_vector, feature_metadata)
            }
            self.slo_info: Optional[SloInformation] = (
                SloInformation(response["metadata"]["sloInfo"]) if "sloInfo" in response["metadata"] else None
            )
            self.request_latency = http_response.latency
        else:
            assert http_response is None
            self.feature_values = feature_values
            self.slo_info = slo_info
            self.request_latency = request_latency


class GetFeaturesMicroBatchResponse:
    """Class representing a response from the GetFeaturesBatch API call.

    Attributes:
        response_list (List[GetFeaturesResponse]): List of :class:`GetFeaturesResponse` objects, one for
            each feature vector in the batch.
        batch_slo_info (Optional[BatchSloInformation]): :class:`BatchSloInformation` object containing information on
            the batch's SLO, present only if the :class:`MetadataOption` `SLO_INFO` is requested in the request.
    """

    def __init__(self, http_response: HTTPResponse, micro_batch_size: int) -> None:
        """Initialize a :class:`GetFeaturesMicroBatchResponse` object.

        Args:
            http_response (HTTPResponse): The HTTP response object returned from the GetFeaturesBatch API call.
            micro_batch_size (int): Number of requests sent in a single batch request, determining how may feature
                vectors are present in the response returned from the server.

        """
        if micro_batch_size == 1:
            self.response_list = [GetFeaturesResponse(http_response=http_response)]
            self.batch_slo_info = None  # Not present since the request was made to /get-features endpoint
        elif 1 < micro_batch_size <= MAX_MICRO_BATCH_SIZE:
            response = http_response.result

            # A single batch response can contain multiple lists of feature vectors
            # Collect the lists of feature vectors present in the batch response
            feature_vectors_list = [result["features"] for result in response["result"]]

            # Collect the metadata and SLO information returned in the batch response
            features_metadata = response["metadata"]["features"]
            slo_info_list = response["metadata"].get("sloInfo")

            # For each feature vector returned in the batch response, create a GetFeaturesResponse object
            # Map the feature values in the feature vector to their corresponding metadata and SLO information
            # Return a list of GetFeaturesResponse objects, one for each feature vector in the batch
            self.response_list = [
                GetFeaturesResponse(
                    request_latency=http_response.latency,
                    feature_values={
                        metadata["name"]: FeatureValue(
                            name=metadata["name"],
                            data_type=metadata["dataType"]["type"],
                            element_type=metadata["dataType"].get("elementType"),
                            effective_time=metadata.get("effectiveTime"),
                            feature_status=metadata["status"][i] if "status" in metadata else None,
                            fields=metadata["dataType"].get("fields"),
                            feature_value=feature,
                        )
                        for feature, metadata in zip(feature_vector, features_metadata)
                    },
                    slo_info=SloInformation(slo_info_list[i]) if slo_info_list else None,
                )
                for i, feature_vector in enumerate(feature_vectors_list)
            ]

            # Get the batch SLO information from the batch response
            self.batch_slo_info = (
                BatchSloInformation(response["metadata"]["batchSloInfo"])
                if "batchSloInfo" in response["metadata"]
                else None
            )
        else:
            raise InvalidMicroBatchSizeError


class GetFeaturesBatchResponse:
    """A class that represents the response from the HTTP API when fetching batch features. The
    class provides methods to access the list of feature vectors returned, along with their metadata, if
    present.

    The list of :class:`GetFeaturesResponse` objects represents the list of responses, each of which encapsulates a
    feature vector and its metadata, in case of a successful request, or None if the request timed out.

    The batch SLO information is only present for batch requests to the /get-features-batch endpoint
    (i.e., `micro_batch_size` > 1)

    Attributes:
        batch_response_list (List[Optional[GetFeaturesResponse]]): List of :class:`GetFeaturesResponse` objects,
            one for each feature vector requested, or None if the request timed out.
        batch_slo_info (Optional[SloInformation]): :class:`SloInformation` object containing information on the
            batch request's SLO, present only for batch requests to the /get-features-batch endpoint and if the
            :class:`MetadataOption` `SLO_INFO` is requested in the request.
        request_latency (timedelta): The response time for GetFeaturesBatch call (network latency + online store
            latency).
    """

    def __init__(
        self,
        responses_list: List[Optional[HTTPResponse]],
        request_latency: timedelta,
        micro_batch_size: int,
    ) -> None:
        """Initializes the object with data from the response.

        Args:
            responses_list (List[Optional[HTTPResponse]]): List of JSON responses returned from the GetFeaturesBatch
                API call, or None if any requests timed out.
            request_latency (timedelta): The response time for GetFeaturesBatch API call (network latency +
                online store latency).
            micro_batch_size (int): The number of requests sent in a single batch request.

        """
        # Parse the list of responses to get a list of all responses as :class:`GetFeaturesMicroBatchResponse` objects
        # if the response exists (i.e. is returned from the batch call),
        # else store the None response as is (i.e. the request timed out and returned None).
        micro_batch_response_list = [
            GetFeaturesMicroBatchResponse(http_response=response, micro_batch_size=micro_batch_size)
            if response
            else response
            for response in responses_list
        ]
        self.batch_response_list = []
        # For each micro-batch response, get each response object from the list of responses if it exists
        for micro_batch_response in micro_batch_response_list:
            if micro_batch_response:
                self.batch_response_list += micro_batch_response.response_list
            else:
                self.batch_response_list.append(None)

        batch_slo_info_list = [
            response.batch_slo_info for response in micro_batch_response_list if response and response.batch_slo_info
        ]

        self.batch_slo_info = (
            self._compute_batch_slo_info(batch_slo_information_list=batch_slo_info_list)
            if batch_slo_info_list
            else None
        )

        self.request_latency = request_latency

    @staticmethod
    def _compute_batch_slo_info(batch_slo_information_list: List[BatchSloInformation]) -> BatchSloInformation:
        """Returns a :class:`BatchSloInformation` object containing information on the batch request's SLO.

        Args:
           batch_slo_information_list (List[BatchSloInformation]): List of :class:`BatchSloInformation` objects, one for
                each micro-batch response.

        Returns:
            BatchSloInformation: :class:`BatchSloInformation` object containing information on the batch
                request's SLO

        """
        # True if all the batch responses are SLO eligible, False otherwise
        is_slo_eligible_batch = all(
            slo_info.slo_eligible for slo_info in batch_slo_information_list if slo_info.slo_eligible is not None
        )

        # Get the list of all slo ineligibility reasons for all the batch responses returned from the server
        batch_slo_ineligibility_reasons = list(
            {
                reason
                for slo_info in batch_slo_information_list
                if slo_info.slo_ineligibility_reasons
                for reason in slo_info.slo_ineligibility_reasons
            }
        )

        # Get the max of the following fields for all the batch responses returned from the server
        max_slo_server_time_seconds = max(
            (
                slo_info.slo_server_time_seconds
                for slo_info in batch_slo_information_list
                if slo_info.slo_server_time_seconds
            ),
            default=None,
        )

        max_store_max_latency = max(
            (slo_info.store_max_latency for slo_info in batch_slo_information_list if slo_info.store_max_latency),
            default=None,
        )

        max_server_time_seconds = max(
            (slo_info.server_time_seconds for slo_info in batch_slo_information_list if slo_info.server_time_seconds),
            default=None,
        )

        return BatchSloInformation(
            {
                "sloEligible": is_slo_eligible_batch,
                "sloServerTimeSeconds": max_slo_server_time_seconds,
                "storeMaxLatency": max_store_max_latency,
                "serverTimeSeconds": max_server_time_seconds,
                "sloIneligibilityReasons": batch_slo_ineligibility_reasons,
            }
        )


def _parse_metadata_to_name_type_dict(response_dict: dict) -> Dict[str, NameAndType]:
    """Parse the returned metadata information to a list of :class:`NameAndType` objects.

    Args:
        response_dict (dict): The response dictionary returned from the server containing metadata information.

    Returns:
        Dict[str, NameAndType]: Dictionary of names mapping to :class:`NameAndType` objects.

    """
    return {
        key["name"]: NameAndType(
            name=key.get("name"),
            data_type=get_data_type(
                data_type=key["dataType"].get("type"),
                element_type=key["dataType"].get("elementType"),
                fields=key["dataType"].get("fields"),
            ),
        )
        for key in response_dict
    }


class FeatureServiceType(str, Enum):
    """Enum to represent the type of the feature service."""

    DEFAULT = "DEFAULT"
    """The feature service is a default feature service."""

    WILDCARD = "WILDCARD"
    """The feature service is a wildcard feature service."""


class GetFeatureServiceMetadataResponse:
    """Response object for GetFeatureServiceMetadata API call.

    Attributes:
        feature_service_type (FeatureServiceType): The type of the feature service.
        input_join_keys (Dict[str, NameAndType]): Dictionary of names mapping to :class:`NameAndType` objects
            representing the input join keys.
        input_request_context_keys (Dict[str, NameAndType]): Dictionary of names mapping to :class:`NameAndType` objects
            representing the input request context keys.
        feature_values (Dict[str, NameAndType]): Dictionary of names mapping to :class:`NameAndType` objects
            representing the feature values.
        output_join_keys (Dict[str, NameAndType]): Dictionary of names mapping to :class:`NameAndType` objects
            representing the output join keys.
    """

    def __init__(self, http_response: HTTPResponse) -> None:
        """Initializes the object with data from the response.

        Args:
            http_response (HTTPResponse): The HTTP response object returned from the GetFeatureServiceMetadata API call.

        """
        response = http_response.result
        self.feature_service_type = (
            FeatureServiceType(response["featureServiceType"]) if response.get("featureServiceType") else None
        )
        self.input_join_keys = _parse_metadata_to_name_type_dict(response.get("inputJoinKeys", {}))
        self.input_request_context_keys = _parse_metadata_to_name_type_dict(response.get("inputRequestContextKeys", {}))
        self.feature_values = _parse_metadata_to_name_type_dict(response.get("featureValues", {}))
        self.output_join_keys = _parse_metadata_to_name_type_dict(response.get("outputJoinKeys", {}))
