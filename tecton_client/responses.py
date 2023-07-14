from datetime import timedelta
from enum import Enum
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import DataType
from tecton_client.data_types import FloatType
from tecton_client.data_types import get_data_type
from tecton_client.data_types import IntType
from tecton_client.data_types import StringType
from tecton_client.data_types import StructType
from tecton_client.exceptions import TectonClientError
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

    def __init__(self, response: dict, request_latency: timedelta) -> None:
        """Initializes the object with data from the response.

        Args:
            response (dict): JSON response returned from the GetFeatures API call.
            request_latency (timedelta): The response time for GetFeatures API call (network latency + online store
                latency).

        """
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

        self.request_latency = request_latency
