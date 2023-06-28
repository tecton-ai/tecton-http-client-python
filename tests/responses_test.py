import json
from datetime import datetime
from typing import List

import pytest
from typing_extensions import Self

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.data_types import StructType
from tecton_client.responses import FeatureStatus
from tecton_client.responses import GetFeaturesResponse
from tecton_client.utils import parse_time
from tests.test_utils import dict_equals


class TestResponse:
    def assert_answers(self: Self, expected_answer: list, get_features_response: GetFeaturesResponse) -> None:
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
    def test_json_responses(self: Self, file_name: str, expected_answer: list) -> None:
        with open(f"tests/test_data/{file_name}") as json_file:
            get_features_response = GetFeaturesResponse(json.load(json_file))

            assert get_features_response.slo_info is None
            self.assert_answers(expected_answer, get_features_response)

    def test_slo_response(self: Self) -> None:
        actual_slo_info = {
            "dynamodb_response_size_bytes": None,
            "server_time_seconds": 0.049082851,
            "slo_eligible": True,
            "slo_ineligibility_reasons": None,
            "slo_server_time_seconds": 0.039343822,
            "store_max_latency": None,
            "store_response_size_bytes": 204,
        }

        with open("tests/test_data/sample_response_slo.json") as json_file:
            get_features_response = GetFeaturesResponse(json.load(json_file))

            assert get_features_response.slo_info is not None
            assert dict_equals(vars(get_features_response.slo_info), actual_slo_info)

    @pytest.mark.parametrize(
        "filename, expected_answers, expected_metadata",
        [
            (
                "tests/test_data/sample_response_metadata.json",
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
    def test_metadata_response(
        self: Self, filename: str, expected_answers: list, expected_metadata: List[tuple]
    ) -> None:
        with open(filename) as json_file:
            get_features_response = GetFeaturesResponse(json.load(json_file))

            assert get_features_response is not None
            assert get_features_response.slo_info is not None

            for feature, metadata in zip(get_features_response.feature_values.values(), expected_metadata):
                assert isinstance(feature.data_type, metadata[0])
                assert feature.feature_status == metadata[1]
                assert feature.effective_time.isoformat(timespec="seconds") == metadata[2]

            self.assert_answers(expected_answers, get_features_response)

    @pytest.mark.parametrize(
        "effective_time", ["2023-05-03T00:00:00Z", "2023-05-03T00:00:00.000000Z", "2023-05-03T00:00:00.000Z"]
    )
    def test_time_parsing(self: Self, effective_time: str) -> None:
        assert isinstance(parse_time(effective_time), datetime)
