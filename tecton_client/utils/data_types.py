import abc
from typing import List
from typing import Self


class DataType(abc.ABC):
    """Base DataType class. All data types inherit from this class."""

    def __str__(self: Self) -> str:
        # Require __str__ implementation. Used in error messages.
        raise NotImplementedError


class IntType(DataType):
    """Class to represent data of type Integer."""

    def __str__(self: Self) -> str:
        return "Int"


class FloatType(DataType):
    """Class to represent data of type Float."""

    def __str__(self: Self) -> str:
        return "Float"


class StringType(DataType):
    """Class to represent data of type String."""

    def __str__(self: Self) -> str:
        return "String"


class BoolType(DataType):
    """Class to represent data of type Boolean."""

    def __str__(self: Self) -> str:
        return "Bool"


class ArrayType(DataType):
    """Class to represent data of type Array. Can contain elements of another DataType."""

    def __init__(self: Self, element_type: DataType) -> None:
        self._element_type = element_type

    @property
    def element_type(self: Self) -> DataType:
        return self._element_type

    def __str__(self: Self) -> str:
        return f"Array({self._element_type})"


class StructField:
    """Class to represent a field in a StructType.

    Attributes:
        name: Name of the field
        data_type: DataType of the field
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
    """Class to represent data of type Struct.

    Attributes:
        fields: List of StructField objects, one for each field in the struct.
    """

    def __init__(self: Self, fields: List[StructField]) -> None:
        self._fields = fields

    @property
    def fields(self: Self) -> List[StructField]:
        return self._fields

    def __str__(self: Self) -> str:
        fields_string = ", ".join(str(field) for field in self._fields)
        return f"Struct({fields_string})"
