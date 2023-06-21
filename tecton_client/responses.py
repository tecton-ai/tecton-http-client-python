from typing import Self
from typing import Union

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import DataType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.data_types import StringType
from tecton_client.data_types import StructType
from tecton_client.exceptions import MismatchedTypeException
from tecton_client.exceptions import UnknownTypeException


class Value:
    """
    Represents an object containing a feature value with a specific type.
    """

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
                field.name: Value(field._data_type, x[i]) for i, field in enumerate(data_type.fields)
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
    def value(self: Self) -> Union[int, float, str, bool, list, dict]:
        """Return the feature value of the feature in the specified type.

        Returns:
            Union[int, float, str, bool, list, dict]: The value of the feature in the specified type.

        """
        if isinstance(self._data_type, StructType):
            value_struct = self._value[self._data_type.__str__()]
            if value_struct is not None:
                return {field: value.value for field, value in value_struct.items()}

        elif isinstance(self._data_type, ArrayType):
            value_list = self._value[self._data_type.__str__()]
            if value_list is not None:
                return [value.value for value in value_list]
        else:
            return self._value[self._data_type.__str__()]
