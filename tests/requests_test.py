import json
from types import NoneType
from typing import Union

import pytest

from tecton_client.exceptions import InvalidParameterException
from tecton_client.exceptions import UnsupportedTypeException
from tecton_client.requests import GetFeatureRequestData
from tecton_client.requests import GetFeaturesRequest
from tecton_client.requests import MetadataOptions

TEST_WORKSPACE_NAME = "test_workspace_name"
TEST_FEATURE_SERVICE_NAME = "test_feature_service_name"

default_get_feature_request_data = GetFeatureRequestData(join_key_map={"test_key": "test_value"})


@pytest.mark.parametrize("key", ["", None])
def test_error_join_key(key: str) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeatureRequestData(join_key_map={key: "test_value"})


@pytest.mark.parametrize("key", ["", None])
def test_error_request_context_key(key: str) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeatureRequestData(request_context_map={key: "test_value"})


def test_empty_join_value() -> None:
    with pytest.raises(InvalidParameterException):
        GetFeatureRequestData(join_key_map={"test_key": ""})


def test_unsupported_join_value() -> None:
    with pytest.raises(UnsupportedTypeException):
        GetFeatureRequestData(join_key_map={"test_key": 123.45})


def test_none_join_value() -> None:
    get_feature_request_data = GetFeatureRequestData(join_key_map={"test_key": None})
    assert get_feature_request_data.join_key_map["test_key"] is None


@pytest.mark.parametrize("value", ["", None])
def test_error_request_context_value(value: Union[str, NoneType]) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeatureRequestData(request_context_map={"test_key": value})


@pytest.mark.parametrize("value", [False, {"test_key": "test_value"}])
def test_unsupported_request_context_value(value: Union[bool, dict]) -> None:
    with pytest.raises(UnsupportedTypeException):
        GetFeatureRequestData(request_context_map={"test_key": value})


def test_mixed_type_join_key_value() -> None:
    get_feature_request_data = GetFeatureRequestData(join_key_map={"test_string_key": "test_string_value",
                                                                   "test_long_key": 1234})
    join_key_map = get_feature_request_data.join_key_map

    assert join_key_map is not None
    assert len(join_key_map) == 2
    assert join_key_map.get("test_string_key") == "test_string_value"
    assert join_key_map.get("test_long_key") == "1234"


def test_mixed_type_request_context_key_value() -> None:
    get_feature_request_data = GetFeatureRequestData(request_context_map={"test_string_key": "test_string_value",
                                                                          "test_long_key": 1234,
                                                                          "test_float_key": 123.45})

    request_context_map = get_feature_request_data.request_context_map

    assert request_context_map is not None
    assert len(request_context_map) == 3
    assert request_context_map.get("test_string_key") == "test_string_value"
    assert request_context_map.get("test_long_key") == "1234"
    assert request_context_map.get("test_float_key") == 123.45


def test_join_key_and_request_context() -> None:
    join_key_map = {"test_join_key_1": "test_join_value_1", "test_join_key_2": "test_join_value_2"}
    request_context_map = {"test_request_context_1": 1234, "test_request_context_2": "test_string_value"}

    get_feature_request_data = GetFeatureRequestData(join_key_map, request_context_map)

    assert get_feature_request_data.join_key_map is not None
    assert get_feature_request_data.request_context_map is not None

    assert len(get_feature_request_data.join_key_map) == 2
    assert len(get_feature_request_data.request_context_map) == 2

    assert join_key_map == get_feature_request_data.join_key_map
    assert request_context_map == get_feature_request_data.request_context_map


@pytest.mark.parametrize("workspace", ["", None])
def test_error_workspace_name(workspace: str) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeaturesRequest(workspace, TEST_FEATURE_SERVICE_NAME, default_get_feature_request_data)


@pytest.mark.parametrize("feature_service", ["", None])
def test_error_feature_service_name(feature_service: str) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeaturesRequest(TEST_WORKSPACE_NAME, feature_service, default_get_feature_request_data)


def test_empty_maps() -> None:
    with pytest.raises(InvalidParameterException):
        GetFeatureRequestData()


def test_simple_request_with_none_join_key() -> None:
    local_get_feature_request_data = GetFeatureRequestData(join_key_map={"test_key": "test_value",
                                                                         "test_none_key": None})

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME,
                                              local_get_feature_request_data, MetadataOptions.defaults())

    assert get_features_request.endpoint == GetFeaturesRequest.ENDPOINT
    assert get_features_request.workspace_name == TEST_WORKSPACE_NAME
    assert get_features_request.feature_service_name == TEST_FEATURE_SERVICE_NAME

    assert get_features_request.request_data.request_context_map is None

    join_key_map = get_features_request.request_data.join_key_map
    assert len(join_key_map) == 2
    assert join_key_map.get("test_key") == "test_value"
    assert join_key_map.get("test_none_key") is None

    expected_response = {"feature_service_name": "test_feature_service_name",
                         "workspace_name": "test_workspace_name",
                         "metadata_options": {"include_data_types": True, "include_names": True},
                         "join_key_map": {"test_key": "test_value", "test_none_key": None}
                         }

    expected_json_request = json.dumps({"params": expected_response})
    json_request = get_features_request.to_json_string()

    assert json_request == expected_json_request


def test_request_with_request_context_map() -> None:
    local_get_feature_request_data = GetFeatureRequestData(join_key_map={"test_key": "test_value"},
                                                           request_context_map={"test_key_1": 123.45,
                                                                                "test_key_2": "test_val"})

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME,
                                              local_get_feature_request_data, MetadataOptions.defaults())

    assert get_features_request.endpoint == GetFeaturesRequest.ENDPOINT
    assert get_features_request.workspace_name == TEST_WORKSPACE_NAME
    assert get_features_request.feature_service_name == TEST_FEATURE_SERVICE_NAME

    request_context_map = get_features_request.request_data.request_context_map

    assert len(request_context_map) == 2
    assert request_context_map.get("test_key_1") == 123.45
    assert request_context_map.get("test_key_2") == "test_val"

    json_request = get_features_request.to_json_string()

    expected_response = {"feature_service_name": "test_feature_service_name",
                         "workspace_name": "test_workspace_name",
                         "metadata_options": {"include_data_types": True, "include_names": True},
                         "join_key_map": {"test_key": "test_value"},
                         "request_context_map": {"test_key_1": 123.45, "test_key_2": "test_val"}
                         }

    expected_json_request = json.dumps({"params": expected_response})

    assert json_request == expected_json_request


def test_all_metadata_options() -> None:
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME,
                                              default_get_feature_request_data,
                                              {MetadataOptions.NAME, MetadataOptions.SLO_INFO,
                                               MetadataOptions.EFFECTIVE_TIME, MetadataOptions.FEATURE_STATUS})

    assert len(get_features_request.metadata_options) == 5
    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE, MetadataOptions.EFFECTIVE_TIME,
                                 MetadataOptions.FEATURE_STATUS, MetadataOptions.SLO_INFO}

    assert expected_metadata_options == metadata_options

    expected_response = {"feature_service_name": "test_feature_service_name",
                         "workspace_name": "test_workspace_name",
                         "metadata_options": {"include_data_types": True, "include_effective_times": True,
                                              "include_names": True, "include_serving_status": True,
                                              "include_slo_info": True},
                         "join_key_map": {"test_key": "test_value"}}

    expected_json = json.dumps({"params": expected_response})
    actual_json = get_features_request.to_json_string()

    assert actual_json == expected_json


def test_custom_metadata_options() -> None:
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME,
                                              default_get_feature_request_data,
                                              {MetadataOptions.NAME, MetadataOptions.SLO_INFO})

    assert len(get_features_request.metadata_options) == 3

    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE, MetadataOptions.SLO_INFO}

    assert expected_metadata_options == metadata_options

    expected_response = {"feature_service_name": "test_feature_service_name",
                         "workspace_name": "test_workspace_name",
                         "metadata_options": {"include_data_types": True, "include_names": True,
                                              "include_slo_info": True},
                         "join_key_map": {"test_key": "test_value"}}

    expected_json = json.dumps({"params": expected_response})
    actual_json = get_features_request.to_json_string()

    assert actual_json == expected_json


def test_default_metadata_options() -> None:
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME,
                                              default_get_feature_request_data)
    assert len(get_features_request.metadata_options) == 2

    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}

    assert expected_metadata_options == metadata_options

    expected_response = {"feature_service_name": "test_feature_service_name",
                         "workspace_name": "test_workspace_name",
                         "metadata_options": {"include_data_types": True, "include_names": True},
                         "join_key_map": {"test_key": "test_value"}}

    expected_json = json.dumps({"params": expected_response})
    actual_json = get_features_request.to_json_string()

    assert actual_json == expected_json
