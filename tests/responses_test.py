import json
import os
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import List

import pytest

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.data_types import StringType
from tecton_client.data_types import StructType
from tecton_client.responses import FeatureServiceType
from tecton_client.responses import FeatureStatus
from tecton_client.responses import GetFeaturesBatchResponse
from tecton_client.responses import GetFeatureServiceMetadataResponse
from tecton_client.responses import GetFeaturesResponse
from tecton_client.responses import HTTPResponse
from tecton_client.responses import SloIneligibilityReason
from tecton_client.utils import parse_string_to_isotime
from tests.test_utils import dict_equals


class TestResponse:
    TEST_DATA_ROOT: Final[str] = "tests/test_data/"
    TEST_DATA_REL_PATH_SINGLE: Final[str] = os.path.join(TEST_DATA_ROOT, "single/")
    TEST_DATA_REL_PATH_BATCH: Final[str] = os.path.join(TEST_DATA_ROOT, "batch/")

    def assert_answers(self, expected_answer: list, get_features_response: GetFeaturesResponse) -> None:
        assert len(get_features_response.feature_values) == len(expected_answer)

        # For StructType data, the feature_values are stored as a dictionary of field.name: field.value pairs.
        # While testing, we retrieve only the values from the dictionary and compare them to the expected response.
        # For other data types, we can directly compare the stored feature_value to the expected response.
        actual_answer = [
            [value for value in feature.feature_value.values()]
            if isinstance(feature.data_type, StructType) and feature.feature_value is not None
            else feature.feature_value
            for key, feature in get_features_response.feature_values.items()
        ]
        assert dict_equals(actual_answer, expected_answer)

    @pytest.mark.parametrize(
        "file_name, expected_answer",
        [
            ("sample_response.json", [0, False, None, "nimbostratus", 55.5]),
            ("sample_response_null.json", [True, None, None, None, 669]),
            ("sample_response_struct.json", [["2.46", 2.46]]),
            ("sample_response_list.json", [[0], None, [55.5, 57.88, 58.96, 57.66, None, 55.98]]),
            ("sample_response_mixed.json", [None, ["2.46", 2.46], [1, 2, 3, None, 5], "test", 24, 691]),
        ],
    )
    def test_json_responses(self, file_name: str, expected_answer: list) -> None:
        with open(os.path.join(TestResponse.TEST_DATA_REL_PATH_SINGLE, file_name)) as json_file:
            http_response = HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
            get_features_response = GetFeaturesResponse(http_response=http_response)

            assert get_features_response.slo_info is None
            assert get_features_response.request_latency == timedelta(milliseconds=10)
            self.assert_answers(expected_answer, get_features_response)

    def test_slo_response(self) -> None:
        actual_slo_info = {
            "dynamodb_response_size_bytes": None,
            "server_time_seconds": 0.049082851,
            "slo_eligible": True,
            "slo_ineligibility_reasons": None,
            "slo_server_time_seconds": 0.039343822,
            "store_max_latency": None,
            "store_response_size_bytes": 204,
        }

        with open(os.path.join(TestResponse.TEST_DATA_REL_PATH_SINGLE, "sample_response_slo.json")) as json_file:
            http_response = HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
            get_features_response = GetFeaturesResponse(http_response=http_response)

            assert get_features_response.slo_info is not None
            assert get_features_response.request_latency == timedelta(milliseconds=10)
            assert dict_equals(vars(get_features_response.slo_info), actual_slo_info)

    @pytest.mark.parametrize(
        "filename, expected_answers, expected_metadata",
        [
            (
                "sample_response_metadata.json",
                [
                    True,
                    None,
                    669,
                    842.8599999999999,
                    [
                        "892054b9598370dce846bb6e4b5805a1",
                        "cc9f13814a736160984bc9896222e4d9",
                        "43a9799c961de6ebb22c122d8c7eb340",
                    ],
                ],
                [
                    (BoolType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
                    (FloatType, FeatureStatus.MISSING_DATA, "2023-05-03T00:00:00"),
                    (IntType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
                    (FloatType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
                    (ArrayType, FeatureStatus.PRESENT, "2023-05-03T00:00:00"),
                ],
            )
        ],
    )
    def test_metadata_response(self, filename: str, expected_answers: list, expected_metadata: List[tuple]) -> None:
        with open(os.path.join(TestResponse.TEST_DATA_REL_PATH_SINGLE, filename)) as json_file:
            http_response = HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
            get_features_response = GetFeaturesResponse(http_response=http_response)

            assert get_features_response is not None
            assert get_features_response.slo_info is not None
            assert get_features_response.request_latency == timedelta(milliseconds=10)

            for feature, metadata in zip(get_features_response.feature_values.values(), expected_metadata):
                assert isinstance(feature.data_type, metadata[0])
                assert feature.feature_status == metadata[1]
                assert feature.effective_time.isoformat(timespec="seconds") == metadata[2]

            self.assert_answers(expected_answers, get_features_response)

    @pytest.mark.parametrize(
        "effective_time", ["2023-05-03T00:00:00Z", "2023-05-03T00:00:00.000000Z", "2023-05-03T00:00:00.000Z"]
    )
    def test_time_parsing(self, effective_time: str) -> None:
        assert isinstance(parse_string_to_isotime(effective_time), datetime)

    @pytest.mark.parametrize(
        "file_names_list, expected_answers_list",
        [
            (
                [
                    "sample_response.json",
                    "sample_response_null.json",
                    "sample_response_struct.json",
                    "sample_response_list.json",
                    "sample_response_mixed.json",
                ],
                [
                    [0, False, None, "nimbostratus", 55.5],
                    [True, None, None, None, 669],
                    [["2.46", 2.46]],
                    [[0], None, [55.5, 57.88, 58.96, 57.66, None, 55.98]],
                    [None, ["2.46", 2.46], [1, 2, 3, None, 5], "test", 24, 691],
                ],
            ),
            (["sample_response.json"] * 4, [[0, False, None, "nimbostratus", 55.5]] * 4),
        ],
    )
    def test_batch_responses_micro_batch_1(self, file_names_list: list, expected_answers_list: list) -> None:
        http_responses_list = []
        for file_name in file_names_list:
            with open(os.path.join(TestResponse.TEST_DATA_REL_PATH_SINGLE, file_name)) as json_file:
                http_responses_list.append(
                    HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
                )

        batch_response = GetFeaturesBatchResponse(
            responses_list=http_responses_list, request_latency=timedelta(milliseconds=10), micro_batch_size=1
        )

        assert batch_response.batch_slo_info is None
        assert batch_response.request_latency == timedelta(milliseconds=10)
        for response, expected_answer in zip(batch_response.batch_response_list, expected_answers_list):
            self.assert_answers(expected_answer, response)

    def test_batch_responses_with_timeout_micro_batch_1(self) -> None:
        http_responses_list = [
            HTTPResponse(
                result={
                    "result": {"features": [True]},
                    "metadata": {
                        "features": [
                            {
                                "name": "transaction_amount_is_high.transaction_amount_is_high",
                                "dataType": {"type": "boolean"},
                            }
                        ]
                    },
                },
                latency=timedelta(milliseconds=10),
            ),
            None,
            HTTPResponse(
                result={
                    "result": {"features": [True]},
                    "metadata": {
                        "features": [
                            {
                                "name": "transaction_amount_is_high.transaction_amount_is_high",
                                "dataType": {"type": "boolean"},
                            }
                        ]
                    },
                },
                latency=timedelta(milliseconds=10),
            ),
            None,
        ]
        batch_response = GetFeaturesBatchResponse(
            responses_list=http_responses_list, request_latency=timedelta(milliseconds=10), micro_batch_size=1
        )
        assert batch_response.batch_slo_info is None
        assert batch_response.request_latency == timedelta(milliseconds=10)
        for i in range(2):
            assert batch_response.batch_response_list[2 * i + 1] is None

    @pytest.mark.parametrize(
        "file_names_list, micro_batch_size, number_of_responses, feature_vector_len, feature_name, order_of_responses",
        [
            (
                ["sample_batch_response.json"],
                5,
                7,
                14,
                "merchant_fraud_rate.is_fraud_mean_30d_1d",
                [
                    0.001596593932943,
                    0.01410105757931845,
                    0.0,
                    0.0030410542321338,
                    0.0,
                    0.0,
                    0.007851934941110488,
                ],
            ),
            (
                ["sample_batch_response.json"] * 5,
                5,
                35,
                14,
                "merchant_fraud_rate.is_fraud_mean_30d_1d",
                [0.001596593932943, 0.01410105757931845, 0.0, 0.0030410542321338, 0.0, 0.0, 0.007851934941110488] * 5,
            ),
        ],
    )
    def test_batch_responses_without_slo_info(
        self,
        file_names_list: list,
        micro_batch_size: int,
        number_of_responses: int,
        feature_vector_len: int,
        feature_name: str,
        order_of_responses: list,
    ) -> None:
        http_responses_list = []
        for file_name in file_names_list:
            with open(os.path.join(TestResponse.TEST_DATA_REL_PATH_BATCH, file_name)) as json_file:
                http_responses_list.append(
                    HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
                )
        batch_response = GetFeaturesBatchResponse(
            responses_list=http_responses_list,
            request_latency=timedelta(milliseconds=10),
            micro_batch_size=micro_batch_size,
        )

        # Test that SLO information is none for the batch and for each response in the list
        assert batch_response.batch_slo_info is None
        assert all(response.slo_info is None for response in batch_response.batch_response_list)

        assert batch_response.request_latency == timedelta(milliseconds=10)

        # Test that the number of responses in the batch is equal to the number of feature vectors in the response
        assert len(batch_response.batch_response_list) == number_of_responses
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
        "file_names_list, micro_batch_size, slo_information, number_of_responses, "
        "feature_vector_len, feature_name, order_of_responses",
        [
            (
                [
                    "sample_batch_response_slo.json",
                    "sample_batch_response_long_slo.json",
                ],
                5,
                {
                    "slo_server_time_seconds": 0.077513756,
                    "server_time_seconds": 0.099455727,
                    "slo_eligible": False,
                    "slo_ineligibility_reasons": [SloIneligibilityReason.DYNAMODB_RESPONSE_SIZE_LIMIT_EXCEEDED],
                },
                12,
                14,
                "user_distinct_merchant_transaction_count_30d.distinct_merchant_transaction_count_30d",
                [693, 671, 692, 669, None, 672, 668, 697, 690, 688, 685, 676],
            ),
            (
                [
                    "sample_batch_response_long_slo.json",
                ],
                2,
                {
                    "slo_server_time_seconds": 0.077513756,
                    "server_time_seconds": 0.057755123,
                    "slo_eligible": False,
                    "slo_ineligibility_reasons": [SloIneligibilityReason.DYNAMODB_RESPONSE_SIZE_LIMIT_EXCEEDED],
                },
                7,
                14,
                "user_distinct_merchant_transaction_count_30d.distinct_merchant_transaction_count_30d",
                [672, 668, 697, 690, 688, 685, 676],
            ),
            (
                ["sample_batch_response_slo.json", "sample_batch_response_long_slo.json"] * 2,
                3,
                {
                    "slo_server_time_seconds": 0.077513756,
                    "server_time_seconds": 0.099455727,
                    "slo_eligible": False,
                    "slo_ineligibility_reasons": [SloIneligibilityReason.DYNAMODB_RESPONSE_SIZE_LIMIT_EXCEEDED],
                },
                24,
                14,
                "user_distinct_merchant_transaction_count_30d.distinct_merchant_transaction_count_30d",
                [693, 671, 692, 669, None, 672, 668, 697, 690, 688, 685, 676] * 2,
            ),
            (
                [
                    "sample_batch_response_slo.json",
                ],
                5,
                {
                    "slo_server_time_seconds": 0.048292505,
                    "server_time_seconds": 0.099455727,
                    "slo_eligible": True,
                    "slo_ineligibility_reasons": [],
                },
                5,
                14,
                "user_distinct_merchant_transaction_count_30d.distinct_merchant_transaction_count_30d",
                [693, 671, 692, 669, None],
            ),
        ],
    )
    def test_multiple_batch_response_with_slo_info(
        self,
        file_names_list: list,
        micro_batch_size: int,
        slo_information: dict,
        number_of_responses: int,
        feature_vector_len: int,
        feature_name: str,
        order_of_responses: list,
    ) -> None:
        http_responses_list = []
        for file_name in file_names_list:
            with open(os.path.join(TestResponse.TEST_DATA_REL_PATH_BATCH, file_name)) as json_file:
                http_responses_list.append(
                    HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
                )

        batch_response = GetFeaturesBatchResponse(
            responses_list=http_responses_list,
            request_latency=timedelta(milliseconds=10),
            micro_batch_size=micro_batch_size,
        )

        # Test that batch SLO information is as expected
        assert batch_response.batch_slo_info is not None
        assert batch_response.batch_slo_info.slo_server_time_seconds == slo_information.get("slo_server_time_seconds")
        assert batch_response.batch_slo_info.server_time_seconds == slo_information.get("server_time_seconds")
        assert batch_response.batch_slo_info.slo_eligible is slo_information.get("slo_eligible")
        assert len(batch_response.batch_slo_info.slo_ineligibility_reasons) == len(
            slo_information.get("slo_ineligibility_reasons")
        )
        assert batch_response.batch_slo_info.slo_ineligibility_reasons == slo_information.get(
            "slo_ineligibility_reasons"
        )

        # Test that each response has SLO information
        assert all(response.slo_info is not None for response in batch_response.batch_response_list)

        assert batch_response.request_latency == timedelta(milliseconds=10)

        # Test that the number of responses in the batch is equal to the number of feature vectors in the response
        assert len(batch_response.batch_response_list) == number_of_responses
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

    def assert_name_and_type_match(self, input_dict: dict, response_dict: dict) -> None:
        assert len(input_dict) == len(response_dict)
        for key, value in input_dict.items():
            assert key in response_dict
            assert isinstance(response_dict[key].data_type, value)

    @pytest.mark.parametrize(
        "file_name, feature_service_type, input_join_keys, input_rc_keys, feature_values, output_join_keys",
        [
            (
                "sample_metadata_response.json",
                FeatureServiceType.DEFAULT,
                {"longitude": FloatType, "latitude": FloatType},
                {},
                {
                    "average_rain.average_temperate_6hrs": ArrayType,
                    "average_rain.precipitation_higher_than_average": BoolType,
                },
                {},
            ),
            (
                "sample_metadata_response_long.json",
                FeatureServiceType.DEFAULT,
                {"user_id": StringType, "merchant": StringType},
                {"amt": FloatType},
                {
                    "transaction_amount_is_higher_than_average.transaction_amount_is_higher_than_average": BoolType,
                    "merchant_fraud_rate.is_fraud_mean_1d_1d": FloatType,
                    "merchant_fraud_rate.is_fraud_mean_30d_1d": FloatType,
                    "merchant_fraud_rate.is_fraud_mean_90d_1d": FloatType,
                    "user_distinct_merchant_transaction_count_30d.distinct_merchant_transaction_count_30d": IntType,
                    "user_transaction_amount_metrics.amt_mean_1d_10m": FloatType,
                    "user_transaction_amount_metrics.amt_mean_1h_10m": FloatType,
                    "user_transaction_amount_metrics.amt_mean_3d_10m": FloatType,
                    "user_transaction_amount_metrics.amt_sum_1d_10m": FloatType,
                    "user_transaction_amount_metrics.amt_sum_1h_10m": FloatType,
                    "user_transaction_amount_metrics.amt_sum_3d_10m": FloatType,
                    "user_transaction_counts.transaction_id_last_3_1d_1d": ArrayType,
                },
                {},
            ),
        ],
    )
    def test_feature_service_metadata_response(
        self,
        file_name: str,
        feature_service_type: FeatureServiceType,
        input_join_keys: dict,
        input_rc_keys: dict,
        feature_values: dict,
        output_join_keys: dict,
    ) -> None:
        with open(os.path.join(TestResponse.TEST_DATA_ROOT, file_name)) as json_file:
            json_dict = json.load(json_file)
            http_response = HTTPResponse(result=json_dict, latency=timedelta(milliseconds=10))
            response = GetFeatureServiceMetadataResponse(http_response=http_response)

            assert response.feature_service_type == feature_service_type
            self.assert_name_and_type_match(input_join_keys, response.input_join_keys)
            self.assert_name_and_type_match(input_rc_keys, response.input_request_context_keys)
            self.assert_name_and_type_match(feature_values, response.feature_values)
            self.assert_name_and_type_match(output_join_keys, response.output_join_keys)
