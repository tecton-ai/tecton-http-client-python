from typing import List
from typing import Self
from typing import Union

import pytest

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import DataType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.data_types import StringType
from tecton_client.data_types import StructField
from tecton_client.data_types import StructType
from tecton_client.responses import Value


class TestDataTypes:
    array_type1 = ArrayType(ArrayType(IntType()))
    array_data1 = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    array_fields1 = [
        {
            "name": "array",
            "dataType": {"type": "array", "elementType": {"type": "array", "elementType": {"type": "int64"}}},
        }
    ]

    struct_type1 = StructType(
        [
            StructField(
                "nested_struct", StructType([StructField("field1", StringType()), StructField("field2", FloatType())])
            ),
            StructField("nested_array", ArrayType(BoolType())),
            StructField("normal", IntType()),
        ]
    )
    struct_data1 = [["test_string", 123.45], [True, False], 123]
    struct_fields1 = [
        {
            "name": "nested_struct",
            "dataType": {
                "type": "struct",
                "fields": [
                    {"name": "field1", "dataType": {"type": "string"}},
                    {"name": "field2", "dataType": {"type": "float64"}},
                ],
            },
        },
        {"name": "nested_array", "dataType": {"type": "array", "elementType": {"type": "boolean"}}},
        {"name": "normal", "dataType": {"type": "int64"}},
    ]

    struct_type2 = StructType([StructField("field1", ArrayType(IntType())), StructField("field2", IntType())])
    struct_data2 = [[1, 2, 3], 4]
    struct_fields2 = [
        {"name": "field1", "dataType": {"type": "array", "elementType": {"type": "int64"}}},
        {"name": "field2", "dataType": {"type": "int64"}},
    ]

    @pytest.mark.parametrize(
        "type_name,value", [(StringType(), "test_string"), (IntType(), 123), (FloatType(), 123.45), (BoolType(), True)]
    )
    def test_value(self: Self, type_name: DataType, value: Union[str, float, int, bool]) -> None:
        test_var = Value(type_name, value)
        assert test_var.value[str(type_name.__str__())] == value
        assert len(test_var.value) == 1

    @staticmethod
    def assert_array(test_var: Value, expected_list: list) -> None:
        arraylist = test_var.get_value()
        assert arraylist == expected_list
        assert len(arraylist) == len(expected_list)
        assert len(test_var.value) == 1

    @pytest.mark.parametrize("array_value", [(["test_string1", "test_string2"]), (["test_string", None])])
    def test_array_value(self: Self, array_value: List[str]) -> None:
        type_name = ArrayType(StringType())
        test_var = Value(type_name, array_value)

        self.assert_array(test_var, array_value)

    @staticmethod
    def assert_struct(test_var: Value, expected_dict: dict) -> None:
        assert len(test_var.value) == 1
        datadict = test_var.get_value()

        length = len(datadict)
        assert length == len(expected_dict)
        assert datadict == expected_dict

    def test_struct_value(self: Self) -> None:
        type_name = StructType([StructField("field1", StringType()), StructField("field2", IntType())])
        test_var = Value(type_name, ["test_string", 123])
        expected_dict = {"field1": "test_string", "field2": 123}

        self.assert_struct(test_var, expected_dict)

    @pytest.mark.parametrize("type_name,actual_data", [(struct_type1, struct_data1), (struct_type2, struct_data2)])
    def test_nested_struct(self: Self, type_name: StructType, actual_data: list) -> None:
        test_var = Value(type_name, actual_data)

        datadict = test_var.value[type_name.__str__()]
        assert len(datadict) == len(actual_data)

        for i in range(len(datadict)):
            key = type_name.fields[i].name
            datatype = type_name.fields[i].data_type

            if type(datatype) == StructType:
                result_dict = {field.name: actual_data[i][j] for j, field in enumerate(datatype.fields)}
                self.assert_struct(datadict[key], result_dict)

            elif type(datatype) == ArrayType:
                self.assert_array(datadict[key], actual_data[i])

            else:
                assert datadict[key].value[datatype.__str__()] == actual_data[i]

    @pytest.mark.parametrize("type_name,actual_data", [(array_type1, array_data1)])
    def test_nested_array(self: Self, type_name: ArrayType, actual_data: list) -> None:
        test_var = Value(type_name, actual_data)

        arraylist = test_var.value[str(type_name.__str__())]
        assert len(arraylist) == len(actual_data)

        for i in range(len(arraylist)):
            self.assert_array(arraylist[i], actual_data[i])
