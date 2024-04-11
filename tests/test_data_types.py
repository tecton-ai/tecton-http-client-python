import json
import pathlib
from unittest import TestCase

from tecton_client import GetFeaturesResponse

TEST_DATA_DIR = pathlib.Path(__file__).parent.joinpath("test_data")


class TestDataTypes(TestCase):
    def test_GetFeaturesResponse_from_dict(self):
        self.maxDiff = 10000
        file_1 = TEST_DATA_DIR.joinpath("sample_response.json")
        with open(file_1) as f:
            resp = json.load(f)
        resp = GetFeaturesResponse.from_response(resp)
        self.assertEquals(
            resp.result.features,
            [["0"], None, [55.5, 57.88, 58.96, 57.66, None, 55.98], ["0", "1", None, "3", "4", None]],
        )
        self.assertDictEqual(
            resp.metadata,
            {
                "features": [
                    {
                        "dataType": {"elementType": {"type": "int64"}, "type": "array"},
                        "name": "average_rain.rain_in_last_24_hrs",
                    },
                    {
                        "dataType": {"elementType": {"type": "string"}, "type": "array"},
                        "name": "average_rain.cloud_type",
                    },
                    {
                        "dataType": {"elementType": {"type": "float64"}, "type": "array"},
                        "name": "average_rain.average_temperate_6hrs",
                    },
                    {
                        "dataType": {"elementType": {"type": "int64"}, "type": "array"},
                        "name": "average_rain.fake_example",
                    },
                ]
            },
        )

    def test_GetFeaturesResponse_get_features_dict(self):
        self.maxDiff = 10000
        file_1 = TEST_DATA_DIR.joinpath("sample_response.json")
        with open(file_1) as f:
            resp = json.load(f)
        resp = GetFeaturesResponse.from_response(resp)
        self.assertDictEqual(
            resp.get_features_dict(),
            {
                "average_rain.average_temperate_6hrs": [55.5, 57.88, 58.96, 57.66, None, 55.98],
                "average_rain.cloud_type": None,
                "average_rain.rain_in_last_24_hrs": [0],
                "average_rain.fake_example": [0, 1, None, 3, 4, None],
            },
        )

    def test_GetFeaturesResponse_get_features_dict_nested(self):
        self.maxDiff = 10000
        file_1 = TEST_DATA_DIR.joinpath("nested_sample_response.json")
        with open(file_1) as f:
            resp = json.load(f)
        resp = GetFeaturesResponse.from_response(resp)
        self.assertDictEqual(
            resp.get_features_dict(),
            {
                "schema.map": {"this": 123},
                # Test null in nested array, null in top level array and empty array.
                "schema.two_dimensional_array": [[123, None], None, []],
                "schema.simple_struct": {"string_field": "fake-string", "int64_field": 12, "float64_field": 123},
                "schema.dist_km": 100,
            },
        )

    def test_get_feature_value_simple_types(self):
        self.assertEquals(GetFeaturesResponse._get_feature_value({"type": "string"}, "thing"), "thing")
        self.assertEquals(GetFeaturesResponse._get_feature_value({"type": "string"}, "thing"), "thing")
        self.assertEquals(GetFeaturesResponse._get_feature_value({"type": "int64"}, "1"), 1)
        self.assertEquals(GetFeaturesResponse._get_feature_value({"type": "float64"}, 1), 1)
        self.assertEquals(GetFeaturesResponse._get_feature_value({"type": "float64"}, "NaN"), "NaN")

    def test_get_feature_value_array(self):
        self.assertEquals(
            GetFeaturesResponse._get_feature_value({"type": "array", "elementType": {"type": "int64"}}, []), []
        )
        self.assertEquals(
            GetFeaturesResponse._get_feature_value({"type": "array", "elementType": {"type": "int64"}}, None), None
        )
        self.assertEquals(
            GetFeaturesResponse._get_feature_value(
                {"type": "array", "elementType": {"type": "int64"}}, ["1", None, "2"]
            ),
            [1, None, 2],
        )

    def test_get_feature_value_nested_array(self):
        self.assertEquals(
            GetFeaturesResponse._get_feature_value(
                {"type": "array", "elementType": {"type": "array", "elementType": {"type": "int64"}}},
                [["1", "2", None], None, []],
            ),
            [[1, 2, None], None, []],
        )

    def test_get_feature_value_map(self):
        self.assertEquals(
            GetFeaturesResponse._get_feature_value(
                {
                    "type": "map",
                    "keyType": {"type": "string"},
                    "valueType": {"type": "int64"},
                },
                {"key1": "1", "key2": None},
            ),
            {"key1": 1, "key2": None},
        )

    def test_get_feature_value_struct(self):
        self.assertEquals(
            GetFeaturesResponse._get_feature_value(
                {
                    "type": "struct",
                    "fields": [
                        {"name": "string_field", "dataType": {"type": "string"}},
                        {"name": "int64_field", "dataType": {"type": "int64"}},
                        {"name": "float64_field", "dataType": {"type": "float64"}},
                    ],
                },
                ["fake-string", "12", 123],
            ),
            {"float64_field": 123, "int64_field": 12, "string_field": "fake-string"},
        )
