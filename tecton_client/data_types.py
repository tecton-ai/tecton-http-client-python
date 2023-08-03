import abc
from dataclasses import dataclass
from typing import List
from typing import Optional

from tecton_client.exceptions import TectonClientError


class DataType(abc.ABC):
    """Base DataType class. All data types inherit from this class."""

    def __str__(self) -> str:
        # Require __str__ implementation. Used in error messages.
        raise NotImplementedError


class IntType(DataType):
    """Class to represent datatype Integer."""

    def __str__(self) -> str:
        return "Int"


class FloatType(DataType):
    """Class to represent datatype Float."""

    def __str__(self) -> str:
        return "Float"


class StringType(DataType):
    """Class to represent datatype String."""

    def __str__(self) -> str:
        return "String"


class BoolType(DataType):
    """Class to represent datatype Boolean."""

    def __str__(self) -> str:
        return "Bool"


class ArrayType(DataType):
    """Class to represent datatype Array.

    An :class:`ArrayType` object represents an array datatype that can contain elements of another :class:`DataType`.

    Examples:
        >>> element_type = FloatType()
        >>> array_type = ArrayType(element_type)
        >>> print(array_type)
        Array(Float)

    """

    def __init__(self, element_type: DataType) -> None:
        """Initialize an :class:`ArrayType` object.

        Args:
            element_type (DataType): The :class:`DataType` of the elements in the array.
        """
        self._element_type = element_type

    @property
    def element_type(self) -> DataType:
        """Return the :class:`DataType` of the elements in the array."""
        return self._element_type

    def __str__(self) -> str:
        return f"Array({self._element_type})"


class StructField:
    """Class to represent a field in a :class:`StructType`.

    A :class:`StructField` object represents a field within a :class:`StructType` object, containing a name and a
    corresponding :class:`DataType`.
    """

    def __init__(self, name: str, data_type: DataType) -> None:
        """Initialize a :class:`StructField` object.

        Args:
            name (str): Name of the field
            data_type (DataType): :class:`DataType` of the field
        """
        self._name = name
        self._data_type = data_type

    @property
    def name(self) -> str:
        """Return the name of the field."""
        return self._name

    @property
    def data_type(self) -> DataType:
        """Return the :class:`DataType` of the field."""
        return self._data_type

    def __str__(self) -> str:
        return f'Field("{self._name}", {self._data_type})'


class StructType(DataType):
    """Class to represent datatype Struct.

    A :class:`StructType` object represents a struct datatype, consisting of multiple fields.

    Examples:
        >>> field1 = StructField("name", StringType())
        >>> field2 = StructField("age", IntType())
        >>> struct_type = StructType([field1, field2])
        >>> print(struct_type)
        Struct(Field(name, String), Field(age, Int))
    """

    def __init__(self, fields: List[StructField]) -> None:
        """Initialize a :class:`StructType` object.

        Args:
            fields (List[StructField]): The list of :class:`StructField` objects representing the fields in the
                struct.
        """
        self._fields = fields

    @property
    def fields(self) -> List[StructField]:
        """Return the list of :class:`StructField` objects representing the fields in the struct."""
        return self._fields

    def __str__(self) -> str:
        fields_string = ", ".join(str(field) for field in self._fields)
        return f"Struct({fields_string})"


def get_data_type(data_type: str, element_type: Optional[dict] = None, fields: Optional[list] = None) -> DataType:
    """Get the :class:`DataType` of the feature value given a string representing the type in the response.

    Args:
        data_type (str): A string representing the type of the feature value, as returned in the response.
        element_type (Optional[dict]): A dict representing the type of the elements in the array,
            present when the data_type is :class:`ArrayType`.
        fields (Optional[list]): A list representing the fields of the struct, present when the data_type is
            :class:`StructType`

    Returns:
        DataType: The parsed :class:`DataType` of the feature value.

    Raises:
        TectonClientError: If an unexpected error occurred while processing the data types in the response.
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
            message = (
                f"Required metadata for the given data type {data_type} is missing from the response."
                f"If problem persists, please contact Tecton Support for assistance."
            )
            raise TectonClientError(message)
    else:
        message = (
            f"Received unknown data type {data_type} in the response."
            f"If problem persists, please contact Tecton Support for assistance."
        )
        raise TectonClientError(message)


@dataclass
class NameAndType:
    """Class to represent a name and data type pair."""

    name: str
    data_type: DataType
