import abc
from typing import List
from typing import Self
from typing import Union

from tecton_client.exceptions import ResponseRelatedErrorMessage
from tecton_client.exceptions import UnknownTypeException


class DataType(abc.ABC):
    """Base DataType class. All data types inherit from this class."""

    def __str__(self: Self) -> str:
        # Require __str__ implementation. Used in error messages.
        raise NotImplementedError


class IntType(DataType):
    """Class to represent datatype Integer."""

    def __str__(self: Self) -> str:
        return "Int"


class FloatType(DataType):
    """Class to represent datatype Float."""

    def __str__(self: Self) -> str:
        return "Float"


class StringType(DataType):
    """Class to represent datatype String."""

    def __str__(self: Self) -> str:
        return "String"


class BoolType(DataType):
    """Class to represent datatype Boolean."""

    def __str__(self: Self) -> str:
        return "Bool"


class ArrayType(DataType):
    """Class to represent datatype Array.
    An `ArrayType` object represents an array datatype that can contain elements of another `DataType`.

    Usage:
        element_type = FloatType()
        array_type = ArrayType(element_type)
        print(array_type)  # Output: Array(Float)

    Attributes:
        element_type (DataType): The datatype of the elements in the array.
    """

    def __init__(self: Self, element_type: DataType) -> None:
        self._element_type = element_type

    @property
    def element_type(self: Self) -> DataType:
        return self._element_type

    def __str__(self: Self) -> str:
        return f"Array({self._element_type})"


class StructField:
    """Class to represent a field in a StructType.

    A `StructField` object represents a field within a `StructType`, containing a name and a corresponding `DataType`.

    Attributes:
        name (str): Name of the field
        data_type (str): DataType of the field
    """

    def __init__(self: Self, name: str, data_type: DataType) -> None:
        self._name = name
        self._data_type = data_type

    @property
    def name(self: Self) -> str:
        return self._name

    @property
    def data_type(self: Self) -> DataType:
        return self._data_type

    def __str__(self: Self) -> str:
        return f"Field({self._name}, {self._data_type})"


class StructType(DataType):
    """Class to represent datatype Struct.

    A `StructType` object represents a struct datatype, consisting of multiple fields.

    Usage:
        field1 = StructField("name", StringType())
        field2 = StructField("age", IntType())
        struct_type = StructType([field1, field2])
        print(struct_type)  # Output: Struct(Field(name, String), Field(age, Int))

    Attributes:
        fields (List[StructField]): The list of StructField objects representing the fields in the struct.
    """

    def __init__(self: Self, fields: List[StructField]) -> None:
        self._fields = fields

    @property
    def fields(self: Self) -> List[StructField]:
        return self._fields

    def __str__(self: Self) -> str:
        fields_string = ", ".join(str(field) for field in self._fields)
        return f"Struct({fields_string})"


class Value:
    """
    Represents a value of a feature in a specific type.

    Attributes:
        value_type: The type of the feature value.
        value: A dictionary storing the value of the feature converted to the required type.
    """

    def __init__(self: Self, value_type: DataType, feature_value: Union[str, None, list]) -> None:
        """Get the value of the feature in the specified type.

        :param value_type: The type of the feature value.
        :param feature_value: The value of the feature that needs to be converted to specified type.
        """
        self.value_type = value_type
        self.value = {}

        if isinstance(value_type, IntType):
            self.value[value_type.__str__()] = None if feature_value is None else int(feature_value)
        elif isinstance(value_type, FloatType):
            self.value[value_type.__str__()] = None if feature_value is None else float(feature_value)
        elif isinstance(value_type, StringType):
            self.value[value_type.__str__()] = None if feature_value is None else feature_value
        elif isinstance(value_type, BoolType):
            self.value[value_type.__str__()] = None if feature_value is None else bool(feature_value)
        elif isinstance(value_type, ArrayType):
            self.value[value_type.__str__()] = [Value(value_type.element_type, value) for value in feature_value]
        elif isinstance(value_type, StructType):
            self.value[value_type.__str__()] = {field.name: Value(field.data_type, feature_value[i])
                                                for i, field in enumerate(value_type.fields)}
        else:
            raise UnknownTypeException(ResponseRelatedErrorMessage.UNKNOWN_DATA_TYPE % value_type)
