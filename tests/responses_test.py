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
from tecton_client.data_types import StructType
from tecton_client.responses import FeatureStatus
from tecton_client.responses import GetFeaturesBatchResponse
from tecton_client.responses import GetFeaturesResponse
from tecton_client.responses import HTTPResponse
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

    @pytest.mark.parametrize(
        "file_name, expected_answers, micro_batch_size",
        [
            (
                "sample_batch_response.json",
                [
                    [
                        False,
                        0.01818181818181818,
                        0.0015965939329430547,
                        0.0016059957173447537,
                        672,
                        63.27903409090909,
                        96.72319999999999,
                        62.30119104335397,
                        44548.44,
                        2418.08,
                        130770.19999999998,
                        682,
                        20522,
                        61328,
                    ],
                    [
                        True,
                        0,
                        0.01410105757931845,
                        0.01611963488055933,
                        668,
                        54.19960377358488,
                        48.155,
                        58.52166878980895,
                        28725.789999999986,
                        866.79,
                        91879.02000000005,
                        512,
                        15355,
                        45987,
                    ],
                    [
                        True,
                        0,
                        0,
                        0,
                        693,
                        68.6643213499633,
                        42.13561403508772,
                        63.56664792176038,
                        93589.46999999997,
                        2401.73,
                        259987.58999999994,
                        1300,
                        40626,
                        121947,
                    ],
                    [
                        False,
                        0,
                        0.0030410542321338066,
                        0.0036076275554028517,
                        693,
                        51.22098890942699,
                        33.63320754716981,
                        56.38687715991207,
                        55421.11000000001,
                        1782.56,
                        179479.4300000001,
                        1030,
                        30385,
                        91675,
                    ],
                    [
                        True,
                        0,
                        0,
                        0,
                        693,
                        71.65225285171101,
                        43.10842105263158,
                        68.77729103404191,
                        150756.33999999997,
                        3276.24,
                        430339.5100000002,
                        2028,
                        60634,
                        182805,
                    ],
                    [
                        False,
                        0,
                        0,
                        0,
                        693,
                        67.6827083333333,
                        53.572978723404255,
                        68.38951367781152,
                        87716.78999999995,
                        2517.93,
                        270001.7999999999,
                        1245,
                        38824,
                        116290,
                    ],
                    [
                        False,
                        0,
                        0.007851934941110488,
                        0.0066630650099045565,
                        676,
                        82.75716713881016,
                        60.33,
                        89.99427063339739,
                        29213.279999999988,
                        844.62,
                        93774.03000000009,
                        339,
                        10279,
                        30528,
                    ],
                ],
                10,
            )
        ],
    )
    def test_batch_responses(self, file_name: str, expected_answers: list, micro_batch_size: int) -> None:
        with open(f"{TestResponse.TEST_DATA_REL_PATH_BATCH}{file_name}") as json_file:
            http_response = HTTPResponse(result=json.load(json_file), latency=timedelta(milliseconds=10))
            batch_response = GetFeaturesBatchResponse(
                responses_list=[http_response],
                request_latency=timedelta(milliseconds=10),
                micro_batch_size=micro_batch_size,
            )

            assert batch_response.batch_slo_info is None
            assert batch_response.request_latency == timedelta(milliseconds=10)
            for response, answer in zip(batch_response.batch_response_list, expected_answers):
                self.assert_answers(answer, response)
