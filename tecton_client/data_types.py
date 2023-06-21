import abc
from typing import List
from typing import Optional
from typing import Self

from tecton_client.exceptions import MISSING_EXPECTED_METADATA
from tecton_client.exceptions import MissingResponseException
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

    """

    def __init__(self: Self, element_type: DataType) -> None:
        """Initialize an ArrayType object.

        Args:
            element_type (DataType): The DataType of the elements in the array.
        """
        self._element_type = element_type

    @property
    def element_type(self: Self) -> DataType:
        """Return the DataType of the elements in the array."""
        return self._element_type

    def __str__(self: Self) -> str:
        return f"Array({self._element_type})"


class StructField:
    """Class to represent a field in a StructType.

    A `StructField` object represents a field within a `StructType`, containing a name and a corresponding `DataType`.
    """

    def __init__(self: Self, name: str, data_type: DataType) -> None:
        """Initialize a StructField object.

        Args:
            name (str): Name of the field
            data_type (DataType): DataType of the field
        """
        self._name = name
        self._data_type = data_type

    @property
    def name(self: Self) -> str:
        """Return the name of the field."""
        return self._name

    @property
    def data_type(self: Self) -> DataType:
        """Return the DataType of the field."""
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
    """

    def __init__(self: Self, fields: List[StructField]) -> None:
        """Initialize a StructType object.

        Args:
            fields (List[StructField]): The list of StructField objects representing the fields in the struct.
        """
        self._fields = fields

    @property
    def fields(self: Self) -> List[StructField]:
        """Return the list of StructField objects representing the fields in the struct."""
        return self._fields

    def __str__(self: Self) -> str:
        fields_string = ", ".join(str(field) for field in self._fields)
        return f"Struct({fields_string})"


def get_data_type(data_type: str, element_type: Optional[dict] = None, fields: Optional[list] = None) -> DataType:
    """Get the DataType of the feature value given a string representing the type in the response.

    Args:
        data_type (str): A string representing the type of the feature value, as returned in the response.
        element_type (Optional[dict]): A dict representing the type of the elements in the array,
            when the data_type is ArrayType.
        fields (Optional[list]): A list representing the fields of the struct, when the data_type is StructType.

    Returns:
        DataType: The parsed data type of the feature value.

    Raises:
        MissingResponseException: If some expected metadata is missing in the response.
        UnknownTypeException: If the data_type is unknown or unsupported.
    """
    data_type = data_type.lower()

    type_mapping = {
        "int64": IntType,
        "float64": FloatType,
        "float32": FloatType,
        "string": StringType,
        "boolean": BoolType,
        "array": lambda: ArrayType(
            get_data_type(
                data_type=element_type.get("type"),
                element_type=element_type.get("elementType"),
                fields=element_type.get("fields"),
            )
        ),
        "struct": lambda: StructType(
            [
                StructField(
                    field["name"],
                    get_data_type(
                        data_type=field["dataType"].get("type"),
                        element_type=field["dataType"].get("elementType"),
                        fields=field["dataType"].get("fields"),
                    ),
                )
                for field in fields
            ]
        ),
    }

    if data_type in type_mapping:
        try:
            return type_mapping[data_type]()
        except Exception:
            raise MissingResponseException(MISSING_EXPECTED_METADATA(f"for given data type {data_type}"))
    else:
        raise UnknownTypeException(data_type.__str__())
