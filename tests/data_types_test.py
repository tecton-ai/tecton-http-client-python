from typing import List
from typing import Union

import pytest

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import DataType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.data_types import NameAndType
from tecton_client.data_types import StringType
from tecton_client.data_types import StructField
from tecton_client.data_types import StructType
from tecton_client.exceptions import TectonClientError
from tecton_client.responses import FeatureValue
from tecton_client.responses import Value
from tests.test_utils import dict_equals


class TestDataTypes:
    array_type1 = ArrayType(ArrayType(IntType()))
    array_data1 = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    array_fields1 = [
        {
            "name": "array",
            "dataType": {"type": "array", "elementType": {"type": "array", "elementType": {"type": "int64"}}},
        }
    ]

    array_data2 = [[1, 2, 3], [4, 5, None], None]
    array_fields2 = [
        {
            "name": "array",
            "dataType": {"type": "array", "elementType": {"type": "array", "elementType": {"type": "int64"}}},
        }
    ]

    array_fields3 = [
        {
            "name": "array",
            "dataType": {"type": "array", "elementType": None},
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
    struct_data1 = [["test_string", 123.45], [True, False], None]
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
        "type_name,value",
        [
            (StringType(), "test_string"),
            (IntType(), 123),
            (FloatType(), 123.45),
            (BoolType(), True),
            (BoolType(), None),
            (StringType(), None),
            (IntType(), None),
            (FloatType(), None),
        ],
    )
    def test_value(self, type_name: DataType, value: Union[str, float, int, bool]) -> None:
        test_var = Value(type_name, value)
        assert test_var.value == value

    @staticmethod
    def assert_array(test_var: Value, expected_list: list) -> None:
        arraylist = test_var.value
        assert arraylist == expected_list
        assert len(arraylist) == len(expected_list)

    @pytest.mark.parametrize("array_value", [(["test_string1", "test_string2"]), (["test_string", None])])
    def test_array_value(self, array_value: List[str]) -> None:
        type_name = ArrayType(StringType())
        test_var = Value(type_name, array_value)
        self.assert_array(test_var, array_value)

    @staticmethod
    def assert_struct(test_var: Value, expected_dict: dict) -> None:
        datadict = test_var.value
        assert len(datadict) == len(expected_dict)
        assert dict_equals(datadict, expected_dict)

    def test_struct_value(self) -> None:
        type_name = StructType([StructField("field1", StringType()), StructField("field2", IntType())])
        test_var = Value(type_name, ["test_string", 123])
        expected_dict = {"field1": "test_string", "field2": 123}
        self.assert_struct(test_var, expected_dict)

    @pytest.mark.parametrize("type_name,actual_data", [(struct_type1, struct_data1), (struct_type2, struct_data2)])
    def test_nested_struct(self, type_name: StructType, actual_data: list) -> None:
        test_var = Value(type_name, actual_data)
        datadict = test_var.value
        for i, (field, data) in enumerate(zip(type_name.fields, actual_data)):
            key, datatype = field.name, field.data_type
            if type(datatype) == StructType:
                result_dict = {field.name: data[j] for j, field in enumerate(datatype.fields)}
                assert dict_equals(datadict[key], result_dict)
            else:
                assert datadict[key] == data

    @pytest.mark.parametrize("type_name,actual_data", [(array_type1, array_data1), (array_type1, array_data2)])
    def test_nested_array(self, type_name: ArrayType, actual_data: list) -> None:
        test_var = Value(type_name, actual_data)
        arraylist = test_var.value
        assert len(arraylist) == len(actual_data)
        assert arraylist == actual_data

    @pytest.mark.parametrize(
        "data_type,feature_value",
        [
            ("string", "test_feature_value"),
            ("int64", 123),
            ("float64", 123.45),
            ("float32", 123.45),
            ("boolean", True),
            ("int64", None),
            ("string", None),
        ],
    )
    def test_basic_feature_values(self, data_type: str, feature_value: str) -> None:
        feature = FeatureValue(name="test.test_feature", data_type=data_type, feature_value=feature_value)
        assert feature.feature_value == feature_value

    @pytest.mark.parametrize("data_type,feature_value", [("int64", "123")])
    def test_int_feature_value(self, data_type: str, feature_value: str) -> None:
        feature = FeatureValue(name="test.test_feature", data_type=data_type, feature_value=feature_value)
        assert feature.feature_value == int(feature_value)

    @pytest.mark.parametrize("feature_value, fields", [(struct_data1, struct_fields1), (struct_data2, struct_fields2)])
    def test_feature_value_with_structs(self, feature_value: list, fields: list) -> None:
        feature = FeatureValue(name="test.test_feature", data_type="struct", feature_value=feature_value, fields=fields)

        feature_val = feature.feature_value

        for i, (field, value) in enumerate(zip(feature.data_type.fields, feature_value)):
            key, datatype = field.name, field.data_type

            if isinstance(datatype, StructType):
                result_dict = {field.name: value[j] for j, field in enumerate(datatype.fields)}
                assert dict_equals(result_dict, feature_val[key])
            else:
                assert feature_val[key] == feature_value[i]

    @pytest.mark.parametrize("feature_value, fields", [(array_data1, array_fields1)])
    def test_feature_value_with_arrays(self, feature_value: list, fields: list) -> None:
        feature = FeatureValue(
            name="test.test_feature",
            data_type="array",
            feature_value=feature_value,
            element_type=fields[0]["dataType"]["elementType"],
        )

        feature_val = feature.feature_value
        assert feature_val == feature_value

    @pytest.mark.parametrize("feature_value, fields", [(array_data1, array_fields3)])
    def test_none_feature_value_type_with_arrays(self, feature_value: list, fields: list) -> None:
        with pytest.raises(TectonClientError):
            FeatureValue(
                name="test.test_feature",
                data_type="array",
                feature_value=feature_value,
                element_type=fields[0]["dataType"]["elementType"],
            )

    @pytest.mark.parametrize(
        "data_type",
        [
            BoolType(),
            IntType(),
            FloatType(),
            StringType(),
            ArrayType(IntType()),
            StructType([StructField("field1", StringType()), StructField("field2", IntType())]),
        ],
    )
    def test_name_and_type(self, data_type: DataType) -> None:
        name_and_type_var = NameAndType(name="test", data_type=data_type)
        assert name_and_type_var.name == "test"
        assert type(name_and_type_var.data_type) == type(data_type)
