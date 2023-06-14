import json
from typing import Self, Set
from typing import Union

import pytest

from tecton_client.exceptions import InvalidParameterException
from tecton_client.exceptions import UnsupportedTypeException
from tecton_client.requests import GetFeatureRequestData
from tecton_client.requests import GetFeaturesRequest
from tecton_client.requests import MetadataOptions


class TestRequests:
    TEST_WORKSPACE_NAME = "test_workspace_name"
    TEST_FEATURE_SERVICE_NAME = "test_feature_service_name"

    default_get_feature_request_data = GetFeatureRequestData(join_key_map={"test_key": "test_value"})

    sample_join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
    sample_request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

    join_key_map_with_none = {"test_key": "test_value", "test_none_key": None}

    default_meta_opts_json = {"include_data_types": True, "include_names": True}

    custom_metadata_options = {MetadataOptions.NAME, MetadataOptions.SLO_INFO}
    custom_meta_opts_json = {"include_data_types": True, "include_names": True, "include_slo_info": True}

    all_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE,
                            MetadataOptions.EFFECTIVE_TIME, MetadataOptions.FEATURE_STATUS, MetadataOptions.SLO_INFO}
    all_meta_opts_json = {"include_data_types": True, "include_effective_times": True,
                          "include_names": True, "include_serving_status": True, "include_slo_info": True}
    
    def setup_method(self: Self) -> None:

        self.expected_response = {
            "feature_service_name": "test_feature_service_name",
            "workspace_name": "test_workspace_name",
            "metadata_options": {"include_data_types": True, "include_names": True},
            "join_key_map": {"test_key": "test_value"}
        }

    @pytest.mark.parametrize("key", ["", None])
    def test_error_join_key(self: Self, key: str) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeatureRequestData(join_key_map={key: "test_value"})

    @pytest.mark.parametrize("key", ["", None])
    def test_error_request_context_key(self: Self, key: str) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeatureRequestData(request_context_map={key: "test_value"})

    @pytest.mark.parametrize("key", [123, 123.45])
    def test_unsupported_request_context_key(self: Self, key: str) -> None:
        with pytest.raises(UnsupportedTypeException):
            GetFeatureRequestData(request_context_map={key: "test_value"})

    def test_empty_join_value(self: Self) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeatureRequestData(join_key_map={"test_key": ""})

    def test_unsupported_join_value(self: Self) -> None:
        with pytest.raises(UnsupportedTypeException):
            GetFeatureRequestData(join_key_map={"test_key": 123.45})

    def test_none_join_value(self: Self) -> None:
        get_feature_request_data = GetFeatureRequestData(join_key_map={"test_key": None})
        assert get_feature_request_data.join_key_map["test_key"] is None

    @pytest.mark.parametrize("value", [None, ""])
    def test_error_request_context_value(self: Self, value: Union[None, str]) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeatureRequestData(request_context_map={"test_key": value})

    @pytest.mark.parametrize("value", [False, {"test_key": "test_value"}])
    def test_unsupported_request_context_value(self: Self, value: Union[bool, dict]) -> None:
        with pytest.raises(UnsupportedTypeException):
            GetFeatureRequestData(request_context_map={"test_key": value})

    @pytest.mark.parametrize("join_key_map, request_context_map", [({"test_string_key": "test_string_value",
                                                                     "test_long_key": 1234}, {}),
                                                                   ({}, {"test_string_key": "test_string_value",
                                                                         "test_long_key": 1234,
                                                                         "test_float_key": 123.45}),
                                                                   (sample_join_key_map, sample_request_context_map)])
    def test_both_maps(self: Self, join_key_map: dict, request_context_map: dict) -> None:
        get_feature_request_data = GetFeatureRequestData(join_key_map=join_key_map,
                                                         request_context_map=request_context_map)

        if join_key_map:
            actual_join_key_map = get_feature_request_data.join_key_map
            assert actual_join_key_map is not None
            assert actual_join_key_map == join_key_map
        if request_context_map:
            actual_request_context_map = get_feature_request_data.request_context_map
            assert actual_request_context_map is not None
            assert actual_request_context_map == request_context_map

    @pytest.mark.parametrize("workspace", ["", None])
    def test_error_workspace_name(self: Self, workspace: str) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeaturesRequest(workspace, self.TEST_FEATURE_SERVICE_NAME, self.default_get_feature_request_data)

    @pytest.mark.parametrize("feature_service", ["", None])
    def test_error_feature_service_name(self: Self, feature_service: str) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeaturesRequest(self.TEST_WORKSPACE_NAME, feature_service, self.default_get_feature_request_data)

    def test_empty_maps(self: Self) -> None:
        with pytest.raises(InvalidParameterException):
            GetFeatureRequestData()

    @pytest.mark.parametrize("join_key_map, request_context_map", [(sample_join_key_map, sample_request_context_map),
                                                                   (join_key_map_with_none, {})])
    def test_request_with_different_maps(self: Self, join_key_map: dict, request_context_map: dict) -> None:
        local_get_feature_request_data = GetFeatureRequestData(join_key_map=join_key_map,
                                                               request_context_map=request_context_map)

        get_features_request = GetFeaturesRequest(self.TEST_WORKSPACE_NAME, self.TEST_FEATURE_SERVICE_NAME,
                                                  local_get_feature_request_data, MetadataOptions.defaults())

        assert get_features_request.endpoint == GetFeaturesRequest.ENDPOINT
        assert get_features_request.workspace_name == self.TEST_WORKSPACE_NAME
        assert get_features_request.feature_service_name == self.TEST_FEATURE_SERVICE_NAME

        expected_response = dict(self.expected_response)

        if join_key_map:
            assert get_features_request.request_data.join_key_map == join_key_map
            expected_response["join_key_map"] = join_key_map
        else:
            assert get_features_request.request_data.join_key_map is None

        if request_context_map:
            assert get_features_request.request_data.request_context_map == request_context_map
            expected_response["request_context_map"] = request_context_map
        else:
            assert get_features_request.request_data.request_context_map is None

        expected_json_request = json.dumps({"params": expected_response})
        json_request = get_features_request.to_json_string()

        assert json_request == expected_json_request

    @pytest.mark.parametrize("metadata_options,json_meta_opts", [(MetadataOptions.defaults(), default_meta_opts_json),
                                                                 (custom_metadata_options, custom_meta_opts_json),
                                                                 (all_metadata_options, all_meta_opts_json)])
    def test_different_metadata_options(self: Self, metadata_options: Set["MetadataOptions"],
                                        json_meta_opts: dict) -> None:
        get_features_request = GetFeaturesRequest(self.TEST_WORKSPACE_NAME, self.TEST_FEATURE_SERVICE_NAME,
                                                  self.default_get_feature_request_data,
                                                  metadata_options)

        actual_metadata_options = get_features_request.metadata_options

        assert len(actual_metadata_options) == len(json_meta_opts) if json_meta_opts else len(metadata_options)
        assert actual_metadata_options == metadata_options | MetadataOptions.defaults()

        expected_response = dict(self.expected_response)
        expected_response["metadata_options"] = json_meta_opts

        expected_json = json.dumps({"params": expected_response})
        actual_json = get_features_request.to_json_string()

        assert actual_json == expected_json
