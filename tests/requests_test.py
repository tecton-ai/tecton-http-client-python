import json
import os
from typing import List
from typing import Union

import pytest

from tecton_client.exceptions import InvalidMicroBatchSizeError
from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import UnsupportedTypeError
from tecton_client.requests import GetFeaturesBatchRequest
from tecton_client.requests import GetFeatureServiceMetadataRequest
from tecton_client.requests import GetFeaturesMicroBatchRequest
from tecton_client.requests import GetFeaturesRequest
from tecton_client.requests import GetFeaturesRequestData
from tecton_client.requests import MetadataOptions
from tests.test_utils import dict_equals


class TestRequests:
    TEST_WORKSPACE_NAME = "test_workspace_name"
    TEST_FEATURE_SERVICE_NAME = "test_feature_service_name"

    TEST_DATA_PATH = "tests/test_data"

    default_request_data = GetFeaturesRequestData(join_key_map={"test_key": "test_value"})

    @pytest.mark.parametrize("key", ["", None])
    def test_error_join_key(self, key: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequestData(join_key_map={key: "test_value"})

    @pytest.mark.parametrize("key", ["", None])
    def test_error_request_context_key(self, key: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequestData(request_context_map={key: "test_value"})

    @pytest.mark.parametrize("key", [123, 123.45])
    def test_unsupported_request_context_key(self, key: str) -> None:
        with pytest.raises(UnsupportedTypeError):
            GetFeaturesRequestData(request_context_map={key: "test_value"})

    def test_empty_join_value(self) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequestData(join_key_map={"test_key": ""})

    def test_unsupported_join_value(self) -> None:
        with pytest.raises(UnsupportedTypeError):
            GetFeaturesRequestData(join_key_map={"test_key": 123.45})

    def test_none_join_value(self) -> None:
        request_data = GetFeaturesRequestData(join_key_map={"test_key": None})
        assert request_data.join_key_map["test_key"] is None

    def test_none_request_context_value(self) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequestData(request_context_map={"test_key": None})

    def test_empty_request_context_value(self) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequestData(request_context_map={"test_key": ""})

    @pytest.mark.parametrize("value", [False, {"test_key": "test_value"}])
    def test_unsupported_request_context_value(self, value: Union[bool, dict]) -> None:
        with pytest.raises(UnsupportedTypeError):
            GetFeaturesRequestData(request_context_map={"test_key": value})

    def test_mixed_type_join_key_value(self) -> None:
        request_data = GetFeaturesRequestData(
            join_key_map={"test_string_key": "test_string_value", "test_long_key": 1234}
        )
        join_key_map = request_data.join_key_map

        assert join_key_map is not None
        assert len(join_key_map) == 2
        assert join_key_map.get("test_string_key") == "test_string_value"
        assert join_key_map.get("test_long_key") == "1234"

    def test_mixed_type_request_context_key_value(self) -> None:
        request_data = GetFeaturesRequestData(
            request_context_map={
                "test_string_key": "test_string_value",
                "test_long_key": 1234,
                "test_float_key": 123.45,
            }
        )

        request_context_map = request_data.request_context_map

        assert request_context_map is not None
        assert len(request_context_map) == 3
        assert request_context_map.get("test_string_key") == "test_string_value"
        assert request_context_map.get("test_long_key") == "1234"
        assert request_context_map.get("test_float_key") == 123.45

    def test_join_key_and_request_context(self) -> None:
        join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
        request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

        request_data = GetFeaturesRequestData(join_key_map, request_context_map)

        assert request_data.join_key_map is not None
        assert request_data.request_context_map is not None

        assert len(request_data.join_key_map) == 2
        assert len(request_data.request_context_map) == 2

        assert dict_equals(join_key_map, request_data.join_key_map)
        assert dict_equals(request_context_map, request_data.request_context_map)

    def test_empty_maps(self) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequestData()

    @pytest.mark.parametrize("workspace", ["", None])
    def test_error_workspace_name(self, workspace: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequest(workspace, self.TEST_FEATURE_SERVICE_NAME, self.default_request_data)

    @pytest.mark.parametrize("feature_service", ["", None])
    def test_error_feature_service_name(self, feature_service: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesRequest(self.TEST_WORKSPACE_NAME, feature_service, self.default_request_data)

    def test_simple_request_with_none_join_key(self) -> None:
        local_request_data = GetFeaturesRequestData(join_key_map={"test_key": "test_value", "test_none_key": None})

        get_features_request = GetFeaturesRequest(
            self.TEST_WORKSPACE_NAME,
            self.TEST_FEATURE_SERVICE_NAME,
            local_request_data,
            {MetadataOptions.NAME, MetadataOptions.DATA_TYPE},
        )

        assert get_features_request.ENDPOINT == GetFeaturesRequest.ENDPOINT
        assert get_features_request.workspace_name == self.TEST_WORKSPACE_NAME
        assert get_features_request.feature_service_name == self.TEST_FEATURE_SERVICE_NAME

        assert get_features_request.request_data.request_context_map is None

        join_key_map = get_features_request.request_data.join_key_map
        assert len(join_key_map) == 2
        assert join_key_map.get("test_key") == "test_value"
        assert join_key_map.get("test_none_key") is None

        expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {"include_data_types": True, "include_names": True},
            "join_key_map": {"test_key": "test_value", "test_none_key": None},
        }

        expected_json_request = {"params": expected_response}
        json_request = get_features_request.to_json()

        assert dict_equals(json_request, expected_json_request)

    def test_request_with_request_context_map(self) -> None:
        local_request_data = GetFeaturesRequestData(
            join_key_map={"test_key": "test_value"},
            request_context_map={"test_key_1": 123.45, "test_key_2": "test_val"},
        )

        get_features_request = GetFeaturesRequest(
            self.TEST_WORKSPACE_NAME,
            self.TEST_FEATURE_SERVICE_NAME,
            local_request_data,
            {MetadataOptions.NAME, MetadataOptions.DATA_TYPE},
        )

        assert get_features_request.ENDPOINT == GetFeaturesRequest.ENDPOINT
        assert get_features_request.workspace_name == self.TEST_WORKSPACE_NAME
        assert get_features_request.feature_service_name == self.TEST_FEATURE_SERVICE_NAME

        request_context_map = get_features_request.request_data.request_context_map

        assert len(request_context_map) == 2
        assert request_context_map.get("test_key_1") == 123.45
        assert request_context_map.get("test_key_2") == "test_val"

        json_request = get_features_request.to_json()

        expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {"include_data_types": True, "include_names": True},
            "join_key_map": {"test_key": "test_value"},
            "request_context_map": {"test_key_1": 123.45, "test_key_2": "test_val"},
        }

        expected_json_request = {"params": expected_response}

        assert dict_equals(json_request, expected_json_request)

    def test_all_metadata_options(self) -> None:
        get_features_request = GetFeaturesRequest(
            self.TEST_WORKSPACE_NAME,
            self.TEST_FEATURE_SERVICE_NAME,
            self.default_request_data,
            {
                MetadataOptions.NAME,
                MetadataOptions.SLO_INFO,
                MetadataOptions.EFFECTIVE_TIME,
                MetadataOptions.FEATURE_STATUS,
            },
        )

        assert len(get_features_request.metadata_options) == 5
        metadata_options = get_features_request.metadata_options
        expected_metadata_options = {
            MetadataOptions.NAME,
            MetadataOptions.DATA_TYPE,
            MetadataOptions.EFFECTIVE_TIME,
            MetadataOptions.FEATURE_STATUS,
            MetadataOptions.SLO_INFO,
        }

        assert dict_equals(expected_metadata_options, metadata_options)

        expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {
                "include_data_types": True,
                "include_effective_times": True,
                "include_names": True,
                "include_serving_status": True,
                "include_slo_info": True,
            },
            "join_key_map": {"test_key": "test_value"},
        }

        expected_json = {"params": expected_response}
        actual_json = get_features_request.to_json()

        assert dict_equals(actual_json, expected_json)

    def test_custom_metadata_options(self) -> None:
        get_features_request = GetFeaturesRequest(
            self.TEST_WORKSPACE_NAME,
            self.TEST_FEATURE_SERVICE_NAME,
            self.default_request_data,
            {MetadataOptions.NAME, MetadataOptions.SLO_INFO},
        )

        assert len(get_features_request.metadata_options) == 3

        metadata_options = get_features_request.metadata_options
        expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE, MetadataOptions.SLO_INFO}

        assert expected_metadata_options == metadata_options

        expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {"include_data_types": True, "include_names": True, "include_slo_info": True},
            "join_key_map": {"test_key": "test_value"},
        }

        expected_json = {"params": expected_response}
        actual_json = get_features_request.to_json()

        assert dict_equals(actual_json, expected_json)

    def test_default_metadata_options(self) -> None:
        get_features_request = GetFeaturesRequest(
            self.TEST_WORKSPACE_NAME, self.TEST_FEATURE_SERVICE_NAME, self.default_request_data
        )
        assert len(get_features_request.metadata_options) == 2

        metadata_options = get_features_request.metadata_options
        expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}

        assert expected_metadata_options == metadata_options

        expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {"include_data_types": True, "include_names": True},
            "join_key_map": {"test_key": "test_value"},
        }

        expected_json = {"params": expected_response}
        actual_json = get_features_request.to_json()

        assert dict_equals(actual_json, expected_json)

    @pytest.mark.parametrize("workspace", ["", None])
    def test_error_workspace_name_batch(self, workspace: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesBatchRequest(workspace, self.TEST_FEATURE_SERVICE_NAME, [self.default_request_data])

    @pytest.mark.parametrize("feature_service", ["", None])
    def test_error_feature_service_name_batch(self, feature_service: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesBatchRequest(self.TEST_WORKSPACE_NAME, feature_service, [self.default_request_data])

    def test_batch_request_with_none_join_keys(self) -> None:
        local_request_data = GetFeaturesRequestData(join_key_map={"test_key": "test_value", "test_none_key": None})

        request = GetFeaturesBatchRequest(
            workspace_name=self.TEST_WORKSPACE_NAME,
            feature_service_name=self.TEST_FEATURE_SERVICE_NAME,
            request_data_list=[local_request_data],
            metadata_options={MetadataOptions.NAME, MetadataOptions.DATA_TYPE},
            micro_batch_size=1,
        )

        assert request.workspace_name == self.TEST_WORKSPACE_NAME
        assert request.feature_service_name == self.TEST_FEATURE_SERVICE_NAME

        for get_features_request in request.request_list:
            assert get_features_request.ENDPOINT == GetFeaturesRequest.ENDPOINT
            assert get_features_request.request_data.request_context_map is None
            assert len(get_features_request.request_data.join_key_map) == 2
            assert get_features_request.request_data.join_key_map.get("test_key") == "test_value"
            assert get_features_request.request_data.join_key_map.get("test_none_key") is None

        expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {"include_data_types": True, "include_names": True},
            "join_key_map": {"test_key": "test_value", "test_none_key": None},
        }

        expected_json_request = {"params": expected_response}
        json_requests = request.to_json_list()

        for json_request in json_requests:
            assert dict_equals(json_request, expected_json_request)

    request_list = [
        GetFeaturesRequestData(join_key_map={"test_key": "test_value"}),
        GetFeaturesRequestData(request_context_map={"test_key": "test_value"}),
        GetFeaturesRequestData(join_key_map={"test_key": "test_value"}, request_context_map={"test_key": "test_value"}),
    ]

    @pytest.mark.parametrize(
        "request_list, micro_batch_size, expected_json_file_name",
        [
            (request_list, 1, "batch_expected_request_1"),
            (request_list, 2, "batch_expected_request_2"),
            (request_list, 3, "batch_expected_request_3"),
            (request_list, 4, "batch_expected_request_3"),
            (request_list * 7, 3, "batch_expected_request_4"),
        ],
    )
    def test_batch_requests(
        self, request_list: List[GetFeaturesRequestData], micro_batch_size: int, expected_json_file_name: str
    ) -> None:
        with open(os.path.join(self.TEST_DATA_PATH, "batch", f"{expected_json_file_name}.json"), "r") as f:
            expected_json_list = json.load(f)

        get_features_request_batch = GetFeaturesBatchRequest(
            workspace_name=self.TEST_WORKSPACE_NAME,
            feature_service_name=self.TEST_FEATURE_SERVICE_NAME,
            request_data_list=request_list,
            metadata_options={MetadataOptions.NAME, MetadataOptions.SLO_INFO},
            micro_batch_size=micro_batch_size,
        )

        if micro_batch_size != 1:
            assert get_features_request_batch.request_list[0].ENDPOINT == GetFeaturesMicroBatchRequest.ENDPOINT

        for actual_json, expected_json in zip(get_features_request_batch.to_json_list(), expected_json_list):
            assert dict_equals(actual_json, expected_json)

    @pytest.mark.parametrize("request_list", [[], [None]])
    def test_error_batch_request_list(self, request_list: List[GetFeaturesRequestData]) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeaturesBatchRequest(
                workspace_name=self.TEST_WORKSPACE_NAME,
                feature_service_name=self.TEST_FEATURE_SERVICE_NAME,
                request_data_list=request_list,
                metadata_options={MetadataOptions.NAME, MetadataOptions.DATA_TYPE},
                micro_batch_size=1,
            )

    @pytest.mark.parametrize("micro_batch_size", [-1, 0, 11])
    def test_error_micro_batch_size(self, micro_batch_size: int) -> None:
        with pytest.raises(InvalidMicroBatchSizeError):
            GetFeaturesBatchRequest(
                workspace_name=self.TEST_WORKSPACE_NAME,
                feature_service_name=self.TEST_FEATURE_SERVICE_NAME,
                request_data_list=self.request_list,
                metadata_options={MetadataOptions.NAME, MetadataOptions.DATA_TYPE},
                micro_batch_size=micro_batch_size,
            )

    @pytest.mark.parametrize(
        "workspace_name, expected_response",
        [
            (
                TEST_WORKSPACE_NAME,
                {
                    "feature_service_name": "test_feature_service_name",
                    "workspace_name": "test_workspace_name",
                },
            )
        ],
    )
    def test_metadata_request(self, workspace_name: str, expected_response: dict) -> None:
        request = GetFeatureServiceMetadataRequest(
            feature_service_name=self.TEST_FEATURE_SERVICE_NAME, workspace_name=workspace_name
        )

        assert request.feature_service_name == self.TEST_FEATURE_SERVICE_NAME
        assert request.ENDPOINT == GetFeatureServiceMetadataRequest.ENDPOINT

        assert request.to_json() == {"params": expected_response}

    @pytest.mark.parametrize(
        "feature_service, workspace_name", [("test", ""), ("test", None), (None, "test"), ("", "test")]
    )
    def test_error_parameters_metadata(self, feature_service: str, workspace_name: str) -> None:
        with pytest.raises(InvalidParameterError):
            GetFeatureServiceMetadataRequest(workspace_name=workspace_name, feature_service_name=feature_service)
