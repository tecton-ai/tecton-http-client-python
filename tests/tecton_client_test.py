import json
import os
from datetime import timedelta
from typing import Final
from urllib.parse import urljoin

import aiohttp
import pytest
from aioresponses import aioresponses

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.exceptions import BadRequestError
from tecton_client.exceptions import ForbiddenError
from tecton_client.exceptions import GatewayTimeoutError
from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import NotFoundError
from tecton_client.exceptions import ResourceExhaustedError
from tecton_client.exceptions import ServiceUnavailableError
from tecton_client.exceptions import TectonServerException
from tecton_client.exceptions import UnauthorizedError
from tecton_client.requests import GetFeaturesBatchRequest
from tecton_client.requests import GetFeatureServiceMetadataRequest
from tecton_client.requests import GetFeaturesRequest
from tecton_client.requests import GetFeaturesRequestData
from tecton_client.requests import MetadataOptions
from tecton_client.responses import FeatureServiceType
from tecton_client.responses import FeatureStatus
from tecton_client.tecton_client import TectonClient
from tecton_client.tecton_client import TectonClientOptions
from tests.test_utils import dict_equals


@pytest.fixture
def mocked() -> aioresponses:
    with aioresponses() as mock:
        yield mock


class TestTectonClient:
    api_key: Final[str] = "1234"
    url: Final[str] = "https://thisisaurl.ai"

    final_url: Final[str] = urljoin(url, "api/v1/feature-service/get-features")
    batch_url: Final[str] = urljoin(url, "api/v1/feature-service/get-features-batch")
    metadata_url: Final[str] = urljoin(url, "api/v1/feature-service/metadata")

    TEST_DATA_ROOT: Final[str] = "tests/test_data/"
    TEST_DATA_REL_PATH_SINGLE: Final[str] = os.path.join(TEST_DATA_ROOT, "single/")
    TEST_DATA_REL_PATH_BATCH: Final[str] = os.path.join(TEST_DATA_ROOT, "batch/")

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
        request_data=GetFeaturesRequestData(join_key_map, request_context_map),
        workspace_name="test-workspace",
    )
    test_request_metadata = GetFeaturesRequest(
        feature_service_name="test_feature_service",
        request_data=GetFeaturesRequestData(join_key_map, request_context_map),
        workspace_name="test-workspace",
        metadata_options={MetadataOptions.SLO_INFO, MetadataOptions.FEATURE_STATUS, MetadataOptions.EFFECTIVE_TIME},
    )
    test_request_batch = GetFeaturesBatchRequest(
        feature_service_name="test_feature_service",
        request_data_list=[GetFeaturesRequestData(join_key_map, request_context_map)] * 10,
        workspace_name="test-workspace",
        metadata_options={MetadataOptions.SLO_INFO, MetadataOptions.FEATURE_STATUS, MetadataOptions.EFFECTIVE_TIME},
        micro_batch_size=5,
    )
    test_request_fs_metadata = GetFeatureServiceMetadataRequest(
        feature_service_name="test_feature_service",
        workspace_name="test-workspace",
    )

    tecton_client = TectonClient(url=url, api_key=api_key)

    @pytest.mark.parametrize(
        "file_name, expected_response",
        [
            ("sample_response_mixed.json", expected_response_mixed),
            ("sample_response_long.json", expected_response_long),
        ],
    )
    def test_get_features(self, mocked: aioresponses, file_name: str, expected_response: dict) -> None:
        TectonClient(TestTectonClient.url, TestTectonClient.api_key)

        with open(os.path.join(TestTectonClient.TEST_DATA_REL_PATH_SINGLE, file_name)) as json_file:
            mocked.post(url=self.final_url, payload=json.load(json_file))
            response = self.tecton_client.get_features(self.test_request_normal)

        assert dict_equals({k: v.feature_value for k, v in response.feature_values.items()}, expected_response)

    @pytest.mark.parametrize("metadata_path, expected_metadata", [("sample_response_metadata.json", expected_metadata)])
    def test_get_features_metadata(self, mocked: aioresponses, metadata_path: str, expected_metadata: list) -> None:
        TectonClient(TestTectonClient.url, TestTectonClient.api_key)
        with open(os.path.join(TestTectonClient.TEST_DATA_REL_PATH_SINGLE, metadata_path)) as json_file:
            mocked.post(url=self.final_url, payload=json.load(json_file))
            response = self.tecton_client.get_features(self.test_request_metadata)

        assert response.slo_info is not None
        assert dict_equals(vars(response.slo_info), self.expected_slo_info)

        for feature, metadata in zip(response.feature_values.values(), expected_metadata):
            assert isinstance(feature.data_type, metadata[0])
            assert feature.feature_status == metadata[1]
            assert feature.effective_time.isoformat(timespec="seconds") == metadata[2]

        assert dict_equals(
            {k: v.feature_value for k, v in response.feature_values.items()}, self.expected_response_metadata
        )

    @pytest.mark.parametrize(
        "exception, status_code, error_json",
        [
            (
                BadRequestError,
                400,
                {
                    "error": "Missing required join key: `user_id`",
                    "message": "Missing required join key: `user_id`",
                    "code": 3,
                },
            ),
            (
                UnauthorizedError,
                401,
                {
                    "error": "invalid 'Tecton-key' authorization header. Note that newly created credentials may "
                    "take up to 60 seconds to be usable.",
                    "message": "invalid 'Tecton-key' authorization header. Note that newly created credentials may "
                    "take up to 60 seconds to be usable.",
                    "code": 16,
                },
            ),
            (
                ForbiddenError,
                403,
                {
                    "error": "Not Authorized. Note that access control changes may take up to 60 seconds to apply.",
                    "message": "Not Authorized. Note that access control changes may take up to 60 seconds to apply.",
                    "code": 7,
                },
            ),
            (
                NotFoundError,
                404,
                {
                    "error": "Unable to query FeatureService `fs` for workspace `ws`. Newly created feature "
                    "services may take up to 60 seconds to query. Also, ensure that the workspace "
                    "is a live workspace.",
                    "message": "Unable to query FeatureService `fs` for workspace `ws`. Newly created feature "
                    "services may take up to 60 seconds to query. Also, ensure that the workspace "
                    "is a live workspace.",
                    "code": 5,
                },
            ),
            (
                ResourceExhaustedError,
                429,
                {
                    "error": "GetFeatures exceeded the concurrent request limit, please retry later",
                    "message": "GetFeatures exceeded the concurrent request limit, please retry later",
                    "code": 8,
                },
            ),
            (
                ServiceUnavailableError,
                503,
                {
                    "error": "503 Service Temporarily Unavailable",
                    "message": "503 Service Temporarily Unavailable",
                    "code": 14,
                },
            ),
            (
                GatewayTimeoutError,
                504,
                {"error": "Timed out", "message": "Timed out", "code": 4},
            ),
        ],
    )
    def test_get_features_error_response(
        self, mocked: aioresponses, exception: TectonServerException, status_code: int, error_json: dict
    ) -> None:
        with pytest.raises(exception):
            mocked.post(
                url=self.final_url,
                status=status_code,
                payload=error_json,
            )
            self.tecton_client.get_features(self.test_request_normal)

    def test_error_response_no_duplicate_codes(self) -> None:
        response_codes = set()
        for exception in TectonServerException.__subclasses__():
            assert exception.STATUS_CODE not in response_codes
            response_codes.add(exception.STATUS_CODE)

    client1 = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(connect=5, total=10), connector=aiohttp.TCPConnector(limit=10)
    )
    client2 = aiohttp.ClientSession()

    @pytest.mark.parametrize(
        "client",
        [client1, client2, None],
    )
    def test_custom_client_with_options(self, mocked: aioresponses, client: aiohttp.ClientSession) -> None:
        if client is None:
            client_options = TectonClientOptions(
                connect_timeout=timedelta(seconds=10),
                read_timeout=timedelta(seconds=15),
                keepalive_expiry=timedelta(seconds=500),
            )
            local_tecton_client = TectonClient(
                TestTectonClient.url, TestTectonClient.api_key, client_options=client_options
            )
            assert client_options.connect_timeout.seconds == 10
            assert client_options.read_timeout.seconds == 15
            assert client_options.keepalive_expiry.seconds == 500
            assert client_options.max_connections == 10
        else:
            local_tecton_client = TectonClient(TestTectonClient.url, TestTectonClient.api_key, client=client)

        with open(os.path.join(TestTectonClient.TEST_DATA_REL_PATH_SINGLE, "sample_response_mixed.json")) as json_file:
            mocked.post(url=self.final_url, payload=json.load(json_file))
            response = local_tecton_client.get_features(self.test_request_normal)

        assert dict_equals(
            {k: v.feature_value for k, v in response.feature_values.items()}, self.expected_response_mixed
        )
        local_tecton_client.close()

    def test_no_api_key(self) -> None:
        # Testing that no API key is provided as a parameter and the environment variable `TECTON_API_KEY` is not set
        with pytest.raises(InvalidParameterError):
            TectonClient(url="https://thisisaurl.ai")

    def test_api_key_env_var(self) -> None:
        os.environ["TECTON_API_KEY"] = "SOME_API_KEY"
        client = TectonClient(url="https://thisisaurl.ai")
        assert not client.is_closed
        client.close()

    @pytest.mark.asyncio
    async def test_client_and_client_options(self) -> None:
        client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=2))
        with pytest.raises(InvalidParameterError):
            TectonClient(
                url="https://thisisaurl.ai", client_options=TectonClientOptions(max_connections=1), client=client
            )
        await client.close()

    @pytest.mark.parametrize(
        "file_name, feature_vector_len, feature_name, order_of_responses",
        [
            (
                "sample_batch_response_long_slo.json",
                14,
                "user_distinct_merchant_transaction_count_30d.distinct_merchant_transaction_count_30d",
                [672, 668, 697, 690, 688, 685, 676, 672, 668, 697, 690, 688, 685, 676],
            ),
        ],
    )
    def test_get_features_batch(
        self, mocked: aioresponses, file_name: str, feature_vector_len: int, feature_name: str, order_of_responses: list
    ) -> None:
        with open(f"{TestTectonClient.TEST_DATA_REL_PATH_BATCH}{file_name}") as json_file:
            mocked.post(url=self.batch_url, payload=json.load(json_file), repeat=True)
            batch_response = self.tecton_client.get_features_batch(self.test_request_batch)

            # Test that the number of feature values in each response corresponds to the individual
            # feature vector length
            assert all(
                len(response.feature_values) == feature_vector_len for response in batch_response.batch_response_list
            )

            # Test that the order of responses is retained by comparing a random feature within each response
            assert all(
                response.feature_values[feature_name].feature_value == order_of_responses.pop(0)
                for response in batch_response.batch_response_list
            )

    @pytest.mark.parametrize(
        "file_name, feature_service_type",
        [
            ("sample_metadata_response.json", FeatureServiceType.DEFAULT),
            ("sample_metadata_response_long.json", FeatureServiceType.DEFAULT),
        ],
    )
    def test_get_metadata(self, mocked: aioresponses, file_name: str, feature_service_type: FeatureServiceType) -> None:
        with open(os.path.join(TestTectonClient.TEST_DATA_ROOT, file_name)) as json_file:
            json_request = json.load(json_file)
            mocked.post(url=self.metadata_url, payload=json_request)
            response = self.tecton_client.get_feature_service_metadata(self.test_request_fs_metadata)

            print(json_request)

            assert response is not None
            assert response.feature_service_type == feature_service_type
            assert len(response.input_join_keys) == len(json_request.get("inputJoinKeys", []))
            assert len(response.input_request_context_keys) == len(json_request.get("inputRequestContextKeys", []))
            assert len(response.feature_values) == len(json_request.get("featureValues", []))
            assert len(response.output_join_keys) == len(json_request.get("outputJoinKeys", []))

    def pytest_sessionfinish(self) -> None:
        self.tecton_client.close()
