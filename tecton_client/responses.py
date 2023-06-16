from datetime import datetime
from enum import Enum
from typing import Optional
from typing import Self
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


from abc import ABC
from typing import List
from typing import Optional
from typing import Self

from tecton_client.exceptions import MISSING_EXPECTED_METADATA
from tecton_client.exceptions import MissingResponseException
from tecton_client.exceptions import ResponseRelatedErrorMessage
from tecton_client.utils.data_types import FeatureValue
from tecton_client.utils.data_types import SloInformation

class Value:
    """Represents an object containing a feature value with a specific type."""

    def __init__(self: Self, data_type: DataType, feature_value: Union[str, None, list]) -> None:
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
    def value(self: Self) -> Union[int, float, str, bool, list, dict, None]:
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
        self: Self,
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
        self.effective_time = datetime.fromisoformat(effective_time) if effective_time else None
        self.data_type = get_data_type(data_type, element_type, fields)
        self.feature_value = Value(self.data_type, feature_value).value

class SloInformation:
    def __init__(self: Self, slo_information: dict):
        self.slo_eligible = slo_information["sloEligible"] if "sloEligible" in slo_information else None
        self.server_time_seconds = (
            slo_information["serverTimeSeconds"] if "serverTimeSeconds" in slo_information else None
        )
        self.slo_server_time_seconds = (
            slo_information["sloServerTimeSeconds"] if "sloServerTimeSeconds" in slo_information else None
        )
        self.dynamoDB_response_size_bytes = (
            slo_information["dynamodbResponseSizeBytes"] if "dynamodbResponseSizeBytes" in slo_information else None
        )
        self.store_max_latency = slo_information["storeMaxLatency"] if "storeMaxLatency" in slo_information else None
        self.store_response_size_bytes = (
            slo_information["storeResponseSizeBytes"] if "storeResponseSizeBytes" in slo_information else None
        )

    def to_dict(self: Self) -> dict:
        return {k: v for k, v in vars(self).items() if v is not None}


class AbstractTectonResponse(ABC):
    """Base class for Response objects from Tecton API calls."""

    @staticmethod
    def validate_response(feature_vector: list, feature_metadata: list) -> None:
        """Validates the response from the Tecton API call.

        Args:
            feature_vector (list): List of features returned.
            feature_metadata (list): List of metadata for each feature.

        Raises:
            MissingResponseException: If the feature vector is empty or if the metadata is missing name or data type.
        """
        if not feature_vector:
            raise MissingResponseException(ResponseRelatedErrorMessage.EMPTY_FEATURE_VECTOR)

        for metadata in feature_metadata:
            if "name" not in metadata:
                raise MissingResponseException(MISSING_EXPECTED_METADATA("name"))
            if "dataType" not in metadata or "type" not in metadata["dataType"]:
                raise MissingResponseException(MISSING_EXPECTED_METADATA("data type"))


class GetFeaturesResponse(AbstractTectonResponse):
    """Response object for GetFeatures API call.

    Attributes:
        feature_values (List[FeatureValue]): List of FeatureValue objects, one for each feature in the feature vector.
        slo_info (Optional[SloInformation]): SloInformation object containing information on the feature vector's SLO.
    """

    def __init__(self: Self, response: dict) -> None:
        """Initializes the object with data from the response.

        Args:
            response (dict): JSON response from the GetFeatures API call parsed to dict.
        """
        feature_vector: list = response["result"]["features"]
        feature_metadata: List[dict] = response["metadata"]["features"]
        self.feature_values: List[FeatureValue] = []

        self.validate_response(feature_vector, feature_metadata)

        for i in range(len(feature_vector)):
            element_type = (
                feature_metadata[i]["dataType"]["elementType"]
                if "elementType" in feature_metadata[i]["dataType"]
                else None
            )
            feature_status = feature_metadata[i]["status"] if "status" in feature_metadata[i] else None
            effective_time = feature_metadata[i]["effectiveTime"] if "effectiveTime" in feature_metadata[i] else None
            fields = feature_metadata[i]["dataType"]["fields"] if "fields" in feature_metadata[i]["dataType"] else None

            feature = FeatureValue(
                name=feature_metadata[i]["name"],
                value_type=feature_metadata[i]["dataType"]["type"],
                element_type=element_type,
                effective_time=effective_time,
                feature_status=feature_status,
                feature_value=feature_vector[i],
                fields=fields,
            )

            self.feature_values.append(feature)

            self.slo_info: Optional[SloInformation] = (
                SloInformation(response["metadata"]["sloInfo"]) if "sloInfo" in response["metadata"] else None
            )

    def get_feature_values_dict(self: Self) -> dict:
        """Returns a dictionary of feature values.

        Returns:
            Dictionary with feature names as keys and their corresponding values.
        """
        feature_values_dict = {
            f"{feature.feature_namespace}.{feature.feature_name}": feature.feature_value
            for feature in self.feature_values
        }

        return feature_values_dict
