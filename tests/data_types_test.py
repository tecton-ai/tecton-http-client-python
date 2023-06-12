from typing import Self

from tecton_client.utils.data_types import ArrayType
from tecton_client.utils.data_types import BoolType
from tecton_client.utils.data_types import FloatType
from tecton_client.utils.data_types import IntType
from tecton_client.utils.data_types import StringType
from tecton_client.utils.data_types import StructField
from tecton_client.utils.data_types import StructType
from tecton_client.utils.data_types import Value


class TestDataTypes:

    def test_str_value(self: Self) -> None:
        type_name = StringType()
        test_var = Value(type_name, "test_string")
        assert test_var.value[type_name.__str__()] == "test_string"
        assert len(test_var.value) == 1

    def test_int_value(self: Self) -> None:
        type_name = IntType()
        test_var = Value(type_name, 123)
        assert test_var.value[type_name.__str__()] == 123
        assert StringType().__str__() not in test_var.value
        assert len(test_var.value) == 1

    def test_float_value(self: Self) -> None:
        type_name = FloatType()
        test_var = Value(type_name, 123.456)
        assert test_var.value[type_name.__str__()] == 123.456
        assert len(test_var.value) == 1

    def test_bool_value(self: Self) -> None:
        type_name = BoolType()
        test_var = Value(type_name, True)
        assert test_var.value[type_name.__str__()] is True
        assert len(test_var.value) == 1

    def test_array_value(self: Self) -> None:
        type_name = ArrayType(StringType())
        test_var = Value(type_name, ["test_string1", "test_string2"])

        arraylist = test_var.value[type_name.__str__()]
        assert [item.value[item.value_type.__str__()] for item in arraylist] == ["test_string1", "test_string2"]
        assert len(test_var.value) == 1

    def test_struct_value(self: Self) -> None:
        type_name = StructType([StructField("field1", StringType()), StructField("field2", IntType())])
        test_var = Value(type_name, ["test_string", 123])

        datadict = test_var.value[type_name.__str__()]
        expected_dict = {"field1": "test_string", "field2": 123}
        assert {name: field.value[field.value_type.__str__()] for name, field in datadict.items()} == expected_dict
        assert len(test_var.value) == 1
