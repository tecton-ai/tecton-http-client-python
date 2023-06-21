import json
from typing import List
from typing import Self

import pytest

from tecton_client.data_types import ArrayType
from tecton_client.data_types import BoolType
from tecton_client.data_types import FloatType
from tecton_client.data_types import IntType
from tecton_client.data_types import StructType
from tecton_client.responses import FeatureStatus
from tecton_client.responses import GetFeaturesResponse


class TestResponse:
    def assert_answers(
        self: Self, expected_answer: list, get_features_response: GetFeaturesResponse, feature_values_map: dict
    ) -> None:
        assert len(feature_values_map) == len(expected_answer)

        actual_answer = []

        for i, key in enumerate(feature_values_map):
            feature_type = get_features_response.feature_values[i].value_type
            if isinstance(feature_type, StructType):
                feature_answer = (
                    [value for key, value in feature_values_map[key].items()]
                    if feature_values_map[key] is not None
                    else None
                )
            else:
                feature_answer = feature_values_map[key]
            actual_answer.append(feature_answer)

        assert actual_answer == expected_answer

    @pytest.mark.parametrize(
        "file_name, expected_answer",
        [
            ("resources/sample_response.json", [0, False, None, "nimbostratus", 55.5]),
            ("resources/sample_response_null.json", [True, None, None, None, 669]),
            ("resources/sample_response_struct.json", [["2.46", 2.46]]),
            ("resources/sample_response_list.json", [[0], None, [55.5, 57.88, 58.96, 57.66, None, 55.98]]),
            ("resources/sample_response_mixed.json", [None, [1, 2, 3, None, 5], "test"]),
        ],
    )
    def test_json_responses(self: Self, file_name: str, expected_answer: list) -> None:
        with open(file_name) as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response.slo_info is None
            feature_values_map = get_features_response.get_feature_values_dict()
            self.assert_answers(expected_answer, get_features_response, feature_values_map)

    def test_slo_response(self: Self) -> None:
        actual_slo_info = {
            "slo_eligible": True,
            "slo_server_time_seconds": 0.039343822,
            "dynamoDB_response_size_bytes": 204,
            "server_time_seconds": 0.049082851,
        }

        with open("resources/sample_response_slo.json") as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response.slo_info is not None
            assert get_features_response.slo_info.to_dict() == actual_slo_info

    @pytest.mark.parametrize(
        "filename, expected_answers, expected_metadata",
        [
            (
                "resources/sample_response_metadata.json",
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
                    (BoolType, FeatureStatus.PRESENT, 0),
                    (FloatType, FeatureStatus.MISSING_DATA, "2023-05-03T00:00:00+00:00"),
                    (IntType, FeatureStatus.PRESENT, "2023-05-04T00:00:00+00:00"),
                    (FloatType, FeatureStatus.PRESENT, "2023-05-04T15:50:00+00:00"),
                    (ArrayType, FeatureStatus.PRESENT, "2023-05-03T00:00:00+00:00"),
                ],
            )
        ],
    )
    def test_metadata_response(
        self: Self, filename: str, expected_answers: list, expected_metadata: List[tuple]
    ) -> None:
        with open(filename) as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response is not None
            for i, feature in enumerate(get_features_response.feature_values):
                assert isinstance(feature.value_type, expected_metadata[i][0])
                assert feature.feature_status == expected_metadata[i][1]
                if feature.effective_time:
                    assert feature.effective_time.isoformat(timespec="seconds") == expected_metadata[i][2]

            assert get_features_response.slo_info is not None

            feature_values_map = get_features_response.get_feature_values_dict()
            self.assert_answers(expected_answers, get_features_response, feature_values_map)
