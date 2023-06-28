import json
from typing import Final

import pytest
from pytest_httpx import HTTPXMock

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.requests import GetFeatureRequestData
from tecton_client.requests import GetFeaturesRequest
from tecton_client.requests import MetadataOptions
from tecton_client.responses import FeatureStatus
from tecton_client.tecton_client import TectonClient
from tests.test_utils import dict_equals


class TestTectonClient:
    api_key: Final[str] = "1234"
    url: Final[str] = "https://thisisaurl.ai"

    TEST_DATA_REL_PATH: Final[str] = "tests/test_data/"

    expected_response_mixed = {
        "test.output_struct1": None,
        "test.output_struct2": {"float64_field": 2.46, "string_field": "2.46"},
        "test.output_array": [1, 2, 3, None, 5],
        "test.output_string": "test",
        "test.output_int1": 24,
        "test.output_int2": 691,
    }
    expected_response_metadata = {
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
    expected_response_long = {
        "test.output_struct1": None,
        "test.output_struct2": {"string_field": "2.46", "float64_field": 2.46},
        "test.output_array1": [1, 2, 3, None, 5],
        "test.output_string1": "test",
        "test.output_int1": 24,
        "test.output_int2": 691,
        "test.output_string2": "test2",
        "test.output_struct3": {"bool_field": False, "float64_field": 123.123},
        "test.output_int3": None,
        "test.output_array2": [1.23, 2.34, 3.45],
        "test.output_string3": "test3",
        "test.output_int4": 78989,
        "test.output_array3": [1, 2, 3],
        "test.output_struct5": {"boolean_field": None},
        "test.output_float1": 456.78,
        "test.output_int5": 999,
        "test.output_bool1": False,
        "test.output_array4": [1.23, 2.34, 3.45],
        "test.output_string4": "test4",
        "test.output_struct6": {"float64_field": 1.23, "int64_field": 2, "string_field": "test_string"},
    }

    expected_metadata = [
        (BoolType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
        (FloatType, FeatureStatus.MISSING_DATA, "2023-05-03T00:00:00"),
        (IntType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
        (FloatType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
        (ArrayType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
    ]
    expected_slo_info = {
        "dynamodb_response_size_bytes": None,
        "server_time_seconds": 0.016889888,
        "slo_eligible": True,
        "slo_ineligibility_reasons": None,
        "slo_server_time_seconds": 0.015835683,
        "store_max_latency": 0.02687345,
        "store_response_size_bytes": 23722,
    }

    join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
    request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

    test_request_normal = GetFeaturesRequest(
        feature_service_name="test_feature_service",
        request_data=GetFeatureRequestData(join_key_map, request_context_map),
        workspace_name="test-workspace",
    )
    test_request_metadata = GetFeaturesRequest(
        feature_service_name="test_feature_service",
        request_data=GetFeatureRequestData(join_key_map, request_context_map),
        workspace_name="test-workspace",
        metadata_options={MetadataOptions.SLO_INFO, MetadataOptions.FEATURE_STATUS, MetadataOptions.EFFECTIVE_TIME},
    )

    @pytest.mark.parametrize(
        "file_name, expected_response",
        [
            ("sample_response_mixed.json", expected_response_mixed),
            ("sample_response_long.json", expected_response_long),
        ],
    )
    def test_get_features(self, httpx_mock: HTTPXMock, file_name: str, expected_response: dict) -> None:
        tecton_client = TectonClient(TestTectonClient.url, TestTectonClient.api_key)

        with open(f"{TestTectonClient.TEST_DATA_REL_PATH}{file_name}") as json_file:
            httpx_mock.add_response(json=json.load(json_file))
            response = tecton_client.get_features(self.test_request_normal)

        assert dict_equals({k: v.feature_value for k, v in response.feature_values.items()}, expected_response)
        tecton_client.close()

    @pytest.mark.parametrize("metadata_path, expected_metadata", [("sample_response_metadata.json", expected_metadata)])
    def test_get_features_metadata(self, httpx_mock: HTTPXMock, metadata_path: str, expected_metadata: list) -> None:
        tecton_client = TectonClient(TestTectonClient.url, TestTectonClient.api_key)
        with open(f"{TestTectonClient.TEST_DATA_REL_PATH}{metadata_path}") as json_file:
            httpx_mock.add_response(json=json.load(json_file))
            response = tecton_client.get_features(self.test_request_metadata)

        assert response.slo_info is not None
        assert dict_equals(vars(response.slo_info), self.expected_slo_info)

        for feature, metadata in zip(response.feature_values.values(), expected_metadata):
            assert isinstance(feature.data_type, metadata[0])
            assert feature.feature_status == metadata[1]
            assert feature.effective_time.isoformat(timespec="seconds") == metadata[2]

        assert dict_equals(
            {k: v.feature_value for k, v in response.feature_values.items()}, self.expected_response_metadata
        )
        tecton_client.close()
