import json

import pytest
from pytest_httpx import HTTPXMock
from typing_extensions import Self

from tecton_client.requests import GetFeatureRequestData
from tecton_client.requests import GetFeaturesRequest
from tecton_client.tecton_client import TectonClient

api_key = "1234"
url = "https://thisisaurl.ai"


class TestTectonClient:
    expected_response1 = {
        "test.output_struct1": None,
        "test.output_struct2": {"float64_field": 2.46, "string_field": "2.46"},
        "test.output_array": [1, 2, 3, None, 5],
        "test.output_string": "test",
        "test.output_int1": 24,
        "test.output_int2": 691,
    }

    tecton_client = TectonClient(url, api_key)
    join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
    request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

    get_feature_request_data = GetFeatureRequestData(join_key_map, request_context_map)
    get_features_request = GetFeaturesRequest(
        feature_service_name="fraud_detection_feature_service",
        request_data=get_feature_request_data,
        workspace_name="tecton-fundamentals-tutorial-live",
    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("file_name, expected_response", [("sample_response_mixed.json", expected_response1)])
    async def test_get_features(self: Self, httpx_mock: HTTPXMock, file_name: str, expected_response: dict) -> None:
        file_name = f"tests/test_data/{file_name}"
        with open(file_name) as json_file:
            json_response = json.load(json_file)
            httpx_mock.add_response(json=json_response)
            get_features_response = await self.tecton_client.get_features(self.get_features_request)

        assert {k: v.feature_value for k, v in get_features_response.feature_values.items()} == expected_response
        await self.tecton_client.close()
