import abc
from typing import List
from typing import Self


class DataType(abc.ABC):
    """
    Base DataType class
    """

    def __str__(self: Self) -> str:
        # Require __str__ implementation. Used in error messages.
        raise NotImplementedError


class IntType(DataType):
    def __str__(self: Self) -> str:
        return "Int"


class FloatType(DataType):
    def __str__(self: Self) -> str:
        return "Float"


class StringType(DataType):

    def __str__(self: Self) -> str:
        return "String"


class BoolType(DataType):

    def __str__(self: Self) -> str:
        return "Bool"


class ArrayType(DataType):
    def __init__(self: Self, element_type: DataType) -> None:
        self._element_type = element_type

    @property
    def element_type(self: Self) -> DataType:
        return self._element_type

    def __str__(self: Self) -> str:
        return f"Array({self._element_type})"


class StructField:
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
    def __init__(self: Self, fields: List[StructField]) -> None:
        self._fields = fields

    @property
    def fields(self: Self) -> List[StructField]:
        return self._fields

    def __str__(self: Self) -> str:
        fields_string = ", ".join(str(field) for field in self._fields)
        return f"Struct({fields_string})"
