import json

import pytest
from pytest_httpx import HTTPXMock

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
    expected_response2 = {
        "transaction_amount.is_present": True,
        "merchant_fraud_rate.is_fraud_mean_90d_1d": None,
        "user_transaction_counts.transaction_id_last_3_1d_1d": 669,
        "user_transaction_amount_metrics.amt_mean_1d_10m": 842.8599999999999,
        "user_transaction_counts.test": [
            "892054b9598370dce846bb6e4b5805a1",
            "cc9f13814a736160984bc9896222e4d9",
            "43a9799c961de6ebb22c122d8c7eb340",
        ],
    }

    join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
    request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

    get_feature_request_data = GetFeatureRequestData(join_key_map, request_context_map)
    get_features_request = GetFeaturesRequest(
        feature_service_name="fraud_detection_feature_service",
        request_data=get_feature_request_data,
        workspace_name="tecton-fundamentals-tutorial-live",
    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "file_name, expected_response",
        [("sample_response_mixed.json", expected_response1), ("sample_response_metadata.json", expected_response2)],
    )
    async def test_get_features(self, httpx_mock: HTTPXMock, file_name: str, expected_response: dict) -> None:
        tecton_client = TectonClient(url, api_key)

        with open(f"tests/test_data/{file_name}") as json_file:
            httpx_mock.add_response(json=json.load(json_file))
            get_features_response = await tecton_client.get_features(self.get_features_request)

        assert {k: v.feature_value for k, v in get_features_response.feature_values.items()} == expected_response
        await tecton_client.close()
