import json
from typing import List
from typing import Self

import pytest

from tecton_client.responses import GetFeaturesResponse
from tecton_client.utils.data_types import ArrayType
from tecton_client.utils.data_types import BoolType
from tecton_client.utils.data_types import FeatureStatus
from tecton_client.utils.data_types import FloatType
from tecton_client.utils.data_types import IntType
from tecton_client.utils.data_types import StructType


class TestResponse:
    def assert_answers(
        self: Self, expected_answer: list, get_features_response: GetFeaturesResponse, feature_values_map: dict
    ) -> None:
        assert len(feature_values_map) == len(expected_answer)

        actual_answer = []

        for i, key in enumerate(feature_values_map):
            feature_type = get_features_response.feature_values[i].value_type
            if isinstance(feature_type, StructType):
                feature_answer = []
                struct_val = feature_values_map[key].value[feature_type.__str__()]
                if struct_val is None:
                    feature_answer = None
                else:
                    for j, value in enumerate(struct_val.items()):
                        feature_answer.append(value[1].value[feature_type.fields[j].data_type.__str__()])

            elif isinstance(feature_type, ArrayType):
                feature_answer = []
                array_val = feature_values_map[key].value[feature_type.__str__()]
                if array_val is None:
                    feature_answer = None
                else:
                    for item in array_val:
                        feature_answer.append(item.value[feature_type.element_type.__str__()])
            else:
                feature_answer = feature_values_map[key].value[feature_type.__str__()]

            actual_answer.append(feature_answer)

        assert actual_answer == expected_answer

    @pytest.mark.parametrize(
        "file_name, expected_answer",
        [
            ("resources/sample_response.json", [0, False, None, "nimbostratus", 55.5]),
            ("resources/sample_response_null.json", [True, None, None, None, 669]),
        ],
    )
    def test_simple_responses(self: Self, file_name: str, expected_answer: list) -> None:
        with open(file_name) as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response.slo_info is None
            feature_values_map = get_features_response.get_feature_values_dict()

<<<<<<< HEAD
            assert len(feature_values_map) == len(expected_answer)

            actual_answer = []

            for i, key in enumerate(feature_values_map):
                feature_type = get_features_response.feature_values[i].value_type
                if isinstance(feature_type, StructType):
                    feature_answer = []
                    struct_val = feature_values_map[key].value[feature_type.__str__()]
                    if struct_val is None:
                        feature_answer = None
                    else:
                        for j, value in enumerate(struct_val.items()):
                            feature_answer.append(value[1].value[feature_type.fields[j].data_type.__str__()])
                elif isinstance(feature_type, ArrayType):
                    feature_answer = []
                    array_val = feature_values_map[key].value[feature_type.__str__()]
                    if array_val is None:
                        feature_answer = None
                    else:
                        for item in array_val:
                            feature_answer.append(item.value[feature_type.element_type.__str__()])
                else:
                    feature_answer = feature_values_map[key].value[feature_type.__str__()]

                actual_answer.append(feature_answer)

            assert actual_answer == expected_answer
=======
            self.assert_answers(expected_answer, get_features_response, feature_values_map)
>>>>>>> 470464f (Adding response_test files)

    def test_arr_response(self: Self) -> None:
        expected_float_list = [55.5, 57.88, 58.96, 57.66, None, 55.98]
        expected_string_list = [None]
        expected_int_list = [0]
        expected_answers = [expected_int_list, expected_string_list, expected_float_list]

        with open("resources/sample_response_list.json") as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response is not None
            feature_values_map = get_features_response.get_feature_values_dict()
            assert len(feature_values_map) == len(expected_answers)

            actual_answers = []

            for i, key in enumerate(feature_values_map):
                feature_type = get_features_response.feature_values[i].value_type
                mini_ans_list = []
                for item in feature_values_map[key].value[feature_type.__str__()]:
                    mini_ans_list.append(item.value[feature_type.element_type.__str__()])
                actual_answers.append(mini_ans_list)

            assert actual_answers == expected_answers

    def test_slo_response(self: Self) -> None:
        actual_slo_info = {
            "sloEligible": True,
            "sloServerTimeSeconds": 0.039343822,
            "dynamodbResponseSizeBytes": 204,
            "serverTimeSeconds": 0.049082851,
        }

        with open("resources/sample_response_slo.json") as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response.slo_info is not None
            assert get_features_response.slo_info.to_dict() == actual_slo_info

<<<<<<< HEAD
    def test_struct_response(self: Self) -> None:
        expected_answer = [["2.46", 2.46]]

        with open("resources/sample_response_struct.json") as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response is not None
            feature_values_map = get_features_response.get_feature_values_dict()
            assert len(feature_values_map) == len(expected_answer)

            actual_answer = []

            for i, key in enumerate(feature_values_map):
                feature_type = get_features_response.feature_values[i].value_type
                if isinstance(feature_type, StructType):
                    feature_answer = []
                    for j, value in enumerate(feature_values_map[key].value[feature_type.__str__()].items()):
                        feature_answer.append(value[1].value[feature_type.fields[j].data_type.__str__()])
                elif isinstance(feature_type, ArrayType):
                    feature_answer = []
                    for item in feature_values_map[key].value[feature_type.__str__()]:
                        feature_answer.append(item.value[feature_type.element_type.__str__()])
                else:
                    feature_answer = feature_values_map[key].value[feature_type.__str__()]

                actual_answer.append(feature_answer)

            assert actual_answer == expected_answer

    def test_mixed_response(self: Self) -> None:
        expected_struct = ["2.46", 2.46]
        expected_array = [1, 2, 3, 4, 5]
        expected_string = "test"

        with open("resources/sample_response_mixed.json") as json_file:
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response is not None
            assert len(get_features_response.feature_values) == 3
            assert isinstance(get_features_response.feature_values[0].value_type, StructType)
            assert isinstance(get_features_response.feature_values[1].value_type, ArrayType)
            assert isinstance(get_features_response.feature_values[2].value_type, StringType)

            feature_values_map = get_features_response.get_feature_values_dict()
            assert len(feature_values_map) == 3

            assert feature_values_map["test.output_struct"] == expected_struct
            assert feature_values_map["test.output_array"] == expected_array
            assert feature_values_map["test.output_string"] == expected_string

    def test_metadata_response(self: Self) -> None:
        with open("resources/sample_response_metadata.json") as json_file:
=======
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
>>>>>>> 470464f (Adding response_test files)
            json_response = json.load(json_file)
            get_features_response = GetFeaturesResponse(json_response)

            assert get_features_response is not None

<<<<<<< HEAD
            feature1 = get_features_response.feature_values[0]
            assert isinstance(feature1.value_type, BoolType)
            assert feature1.feature_status == "PRESENT"

            feature2 = get_features_response.feature_values[1]
            assert isinstance(feature2.value_type, FloatType)
            assert feature2.feature_status == "MISSING_DATA"
            assert feature2.feature_value is None

            feature3 = get_features_response.feature_values[2]
            assert isinstance(feature3.value_type, IntType)
            assert feature3.feature_status == "PRESENT"
            assert feature3.effective_time.isoformat(timespec="seconds") == "2023-05-04T00:00:00+00:00"

            feature4 = get_features_response.feature_values[3]
            assert isinstance(feature4.value_type, FloatType)
            assert feature4.feature_status == "PRESENT"
            assert feature4.effective_time.isoformat(timespec="seconds") == "2023-05-04T15:50:00+00:00"

            feature5 = get_features_response.feature_values[4]
            assert isinstance(feature5.value_type, ArrayType)
            assert feature5.feature_status == "PRESENT"
            assert feature5.effective_time.isoformat(timespec="seconds") == "2023-05-03T00:00:00+00:00"
=======
            for i, feature in enumerate(get_features_response.feature_values):
                assert isinstance(feature.value_type, expected_metadata[i][0])
                assert feature.feature_status == expected_metadata[i][1]
                if feature.effective_time:
                    assert feature.effective_time.isoformat(timespec="seconds") == expected_metadata[i][2]
>>>>>>> 470464f (Adding response_test files)

            assert get_features_response.slo_info is not None

            feature_values_map = get_features_response.get_feature_values_dict()
            self.assert_answers(expected_answers, get_features_response, feature_values_map)
