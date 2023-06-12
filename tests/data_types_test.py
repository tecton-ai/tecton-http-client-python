from typing import Self
from typing import Union

import pytest

from tecton_client.utils.data_types import ArrayType
from tecton_client.utils.data_types import BoolType
from tecton_client.utils.data_types import DataType
from tecton_client.utils.data_types import FloatType
from tecton_client.utils.data_types import IntType
from tecton_client.utils.data_types import StringType
from tecton_client.utils.data_types import StructField
from tecton_client.utils.data_types import StructType
from tecton_client.utils.data_types import Value


class TestDataTypes:

    @pytest.mark.parametrize("type_name,value", [(StringType(), "test_string"), (IntType(), 123),
                                                 (FloatType(), 123.45), (BoolType(), True)])
    def test_value(self: Self, type_name: DataType, value: Union[str, float, int, bool]) -> None:
        test_var = Value(type_name, value)
        assert test_var.value[str(type_name.__str__())] == value
        assert len(test_var.value) == 1

    def test_array_value(self: Self) -> None:
        type_name = ArrayType(StringType())
        test_var = Value(type_name, ["test_string1", "test_string2"])

        arraylist = test_var.value[str(type_name.__str__())]
        assert [item.value[type_name.element_type.__str__()] for item in arraylist] == ["test_string1", "test_string2"]
        assert len(test_var.value) == 1

    def test_struct_value(self: Self) -> None:
        type_name = StructType([StructField("field1", StringType()), StructField("field2", IntType())])
        test_var = Value(type_name, ["test_string", 123])

        datadict = test_var.value[type_name.__str__()]
        expected_dict = {"field1": "test_string", "field2": 123}

        field1 = datadict["field1"]
        field2 = datadict["field2"]

        actual_dict = {"field1": field1.value[type_name.fields[0].data_type.__str__()],
                       "field2": field2.value[type_name.fields[1].data_type.__str__()]}

        assert actual_dict == expected_dict
        assert len(test_var.value) == 1

    def test_nested_value(self: Self) -> None:
        nested_struct = StructType([StructField("field1", StringType()), StructField("field2", FloatType())])
        nested_array = ArrayType(BoolType())
        type_name = StructType([StructField("nested_struct", nested_struct), StructField("nested_array", nested_array),
                                StructField("normal", IntType())])

        actual_data = [["test_string", 123.45], [True, False], 123]
        test_var = Value(type_name, actual_data)

        datadict = test_var.value[type_name.__str__()]
        assert len(datadict) == 3

        struct_val = datadict["nested_struct"].value[type_name.fields[0].data_type.__str__()]
        assert len(struct_val) == 2

        nested_struct_f1 = struct_val["field1"]
        nested_struct_f2 = struct_val["field2"]
        nested_dict = {"field1": nested_struct_f1.value[nested_struct.fields[0].data_type.__str__()],
                       "field2": nested_struct_f2.value[nested_struct.fields[1].data_type.__str__()]}
        actual_nested_dict = {"field1": "test_string", "field2": 123.45}
        assert nested_dict == actual_nested_dict

        array_val = datadict["nested_array"].value[type_name.fields[1].data_type.__str__()]
        assert len(array_val) == 2
        assert [item.value[nested_array.element_type.__str__()] for item in array_val] == [True, False]

        assert datadict["normal"].value[type_name.fields[2].data_type.__str__()] == 123

    def test_none_values(self: Self) -> None:
        type_name = ArrayType(StringType())
        test_var = Value(type_name, ["test_string", None])

        arraylist = test_var.value[str(type_name.__str__())]
        assert [item.value[type_name.element_type.__str__()] for item in arraylist] == ["test_string", None]

        assert len(test_var.value) == 1
