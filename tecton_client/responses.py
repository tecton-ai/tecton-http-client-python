from datetime import datetime
from enum import Enum
from typing import Optional
from typing import Self
from typing import Union

from tecton_client.data_types import DataType
from tecton_client.data_types import parse_data_type
from tecton_client.exceptions import MissingResponseException
from tecton_client.exceptions import ResponseRelatedErrorMessage

class Value:
    """Represents an object containing a feature value with a specific type."""

    def __init__(self: Self, data_type: DataType, feature_value: Union[str, None, list]) -> None:
        """Set the value of the feature in the specified type.

        Args:
            data_type (DataType): The type of the feature value.
            feature_value (Union[str, None, list]): The value of the feature that needs to be converted to the specified
                type.

        Raises:
            MismatchedTypeException: If the feature value cannot be converted to the specified type.
            UnknownTypeException: If the specified type is not supported.
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
                raise MismatchedTypeException(feature_value, data_type.__str__())
        else:
            raise UnknownTypeException(data_type.__str__())

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
        value_type (DataType): The type of the feature value.
        feature_value (Value): The value of the feature.
        feature_namespace (str): The namespace that the feature belongs to.
        feature_name (str): The name of the feature.
        feature_status (str): The status of the feature.
        effective_time (datetime): The effective time of the feature value.
    """

    def __init__(
        self: Self,
        name: str,
        value_type: str,
        feature_value: Union[str, None, list],
        effective_time: Optional[str] = None,
        element_type: Optional[dict] = None,
        fields: Optional[list] = None,
        feature_status: Optional[str] = None,
    ) -> None:
        """Initialize a FeatureValue object.

        Args:
            name (str): The name of the feature.
            value_type (str): String that indicates the type of the feature value.
            feature_value (Union[str, None, list]): The value of the feature.
            effective_time (Optional[str]): The effective time of the feature value, sent as ISO-8601 format string.
            element_type (Optional[dict]): Dictionary that indicates the type of the elements in the array,
                present if value_type is ArrayType.
            fields (Optional[list]): List of the fields of the struct, if value_type is StructType.
            feature_status (Optional[str]): The status string of the feature value.

        Raises:
            MissingResponseException: If the name of the feature is not in the format of <namespace>.<feature_name>.
        """
        try:
            self.feature_namespace, self.feature_name = name.split(".")
        except ValueError:
            raise MissingResponseException(ResponseRelatedErrorMessage.MALFORMED_FEATURE_NAME)

        self.feature_status = FeatureStatus(feature_status) if feature_status else None
        self.effective_time = datetime.fromisoformat(effective_time) if effective_time else None
        self.value_type: DataType = parse_data_type(value_type, element_type, fields)
        self.feature_value: Value = Value(self.value_type, feature_value)
