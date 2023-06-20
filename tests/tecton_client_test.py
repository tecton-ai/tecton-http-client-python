from typing import Self

import pytest
from pytest_httpx import HTTPXMock

from tecton_client.requests import GetFeatureRequestData
from tecton_client.requests import GetFeaturesRequest
from tecton_client.tecton_client import TectonClient

api_key = "1234"
url = "https://thisisaurl.ai"


class TestTectonClient:
    @pytest.mark.asyncio
    async def test_get_features(self: Self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            json={
                "result": {"features": ["1", 11292.571748310578]},
                "metadata": {
                    "features": [
                        {
                            "name": "transaction_amount_is_high.transaction_amount_is_high",
                            "dataType": {"type": "int64"},
                        },
                        {"name": "transaction_distance_from_home.dist_km", "dataType": {"type": "float64"}},
                    ]
                },
            }
        )
        tecton_client = TectonClient(url, api_key)
        join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
        request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

        get_feature_request_data = GetFeatureRequestData(join_key_map, request_context_map)
        get_features_request = GetFeaturesRequest(
            feature_service_name="fraud_detection_feature_service",
            request_data=get_feature_request_data,
            workspace_name="tecton-fundamentals-tutorial-live",
        )
        get_features_response = await tecton_client.get_features(get_features_request)
        expected_response = {
            "transaction_amount_is_high.transaction_amount_is_high": 1,
            "transaction_distance_from_home.dist_km": 11292.571748310578,
        }
        assert get_features_response.get_feature_values_dict() == expected_response
        await tecton_client.close()
