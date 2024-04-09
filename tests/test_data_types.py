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
        self.assertEquals(resp.result.features, [["0"], None, [55.5, 57.88, 58.96, 57.66, None, 55.98]])
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
                "average_rain.rain_in_last_24_hrs": ["0"],
                "average_rain.fake_example": ['0', '1', None, '3', '4', '5'],
            },
        )
