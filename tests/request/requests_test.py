import json

import pytest

from tecton_client.exceptions.exceptions import InvalidParameterException
from tecton_client.request.requests import GetFeaturesRequest
from tecton_client.request.requests_data import (
    GetFeatureRequestData, MetadataOptions, GetFeaturesRequestJSON
)

TEST_WORKSPACE_NAME = "test_workspace_name"
TEST_FEATURE_SERVICE_NAME = "test_feature_service_name"

default_get_feature_request_data = \
    GetFeatureRequestData(join_key_map={"test_key": "test_value"})


@pytest.mark.parametrize("workspace", ["", None])
def test_error_workspace_name(workspace: str) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeaturesRequest(workspace,
                           TEST_FEATURE_SERVICE_NAME,
                           default_get_feature_request_data,
                           MetadataOptions.defaults())


@pytest.mark.parametrize("feature_service", ["", None])
def test_error_feature_service_name(feature_service: str) -> None:
    with pytest.raises(InvalidParameterException):
        GetFeaturesRequest(TEST_WORKSPACE_NAME,
                           feature_service,
                           default_get_feature_request_data,
                           MetadataOptions.defaults())


def test_empty_maps() -> None:
    empty_get_feature_request_data = GetFeatureRequestData()
    with pytest.raises(InvalidParameterException):
        GetFeaturesRequest(TEST_WORKSPACE_NAME,
                           TEST_FEATURE_SERVICE_NAME,
                           empty_get_feature_request_data,
                           MetadataOptions.defaults())


def test_simple_request_with_none_join_key() -> None:
    local_get_feature_request_data = GetFeatureRequestData(
        join_key_map={"test_key": "test_value", "test_none_key": None})

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              local_get_feature_request_data,
                                              MetadataOptions.defaults())

    assert get_features_request.endpoint == GetFeaturesRequest.ENDPOINT
    assert get_features_request.workspace_name == TEST_WORKSPACE_NAME
    assert get_features_request.feature_service_name == \
           TEST_FEATURE_SERVICE_NAME
    get_features_request_data = get_features_request.request_data
    assert len(get_features_request_data.request_context_map) == 0

    join_key_map = get_features_request.request_data.join_key_map
    assert len(join_key_map) == 2
    assert join_key_map.get("test_key") == "test_value"
    assert join_key_map.get("test_none_key") is None

    expected_response = GetFeaturesRequestJSON(
        feature_service_name="test_feature_service_name",
        join_key_map={"test_key": "test_value",
                      "test_none_key": None},
        metadata_options={"include_data_types": True,
                          "include_names": True,
                          },
        request_context_map={},
        workspace_name="test_workspace_name"
    ).to_dict

    expected_json_request = json.dumps({"params": expected_response})
    json_request = get_features_request.request_to_json

    assert json_request == expected_json_request


def test_request_with_request_context_map() -> None:
    local_get_feature_request_data = \
        GetFeatureRequestData(join_key_map={"test_key": "test_value"},
                              request_context_map={"test_key_1": 123.45,
                                                   "test_key_2": "test_val"})

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              local_get_feature_request_data,
                                              MetadataOptions.defaults())

    assert get_features_request.endpoint == GetFeaturesRequest.ENDPOINT
    assert get_features_request.workspace_name == \
           TEST_WORKSPACE_NAME
    assert get_features_request.feature_service_name == \
           TEST_FEATURE_SERVICE_NAME

    request_context_map = get_features_request. \
        request_data.request_context_map

    assert len(request_context_map) == 2
    assert request_context_map.get("test_key_1") == 123.45
    assert request_context_map.get("test_key_2") == "test_val"

    json_request = get_features_request.request_to_json

    expected_response = GetFeaturesRequestJSON(
        feature_service_name="test_feature_service_name",
        join_key_map={"test_key": "test_value"},
        metadata_options={"include_data_types": True,
                          "include_names": True,
                          },
        request_context_map={"test_key_1": 123.45,
                             "test_key_2": "test_val"},
        workspace_name="test_workspace_name"
    ).to_dict

    expected_json_request = json.dumps({"params": expected_response})

    assert json_request == expected_json_request


def test_all_metadata_options() -> None:
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              default_get_feature_request_data,
                                              {MetadataOptions.NAME,
                                               MetadataOptions.SLO_INFO,
                                               MetadataOptions.DATA_TYPE,
                                               MetadataOptions.EFFECTIVE_TIME,
                                               MetadataOptions.FEATURE_STATUS})

    assert len(get_features_request.metadata_options) == 5
    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME,
                                 MetadataOptions.DATA_TYPE,
                                 MetadataOptions.EFFECTIVE_TIME,
                                 MetadataOptions.FEATURE_STATUS,
                                 MetadataOptions.SLO_INFO}

    assert expected_metadata_options == metadata_options


def test_custom_metadata_options() -> None:
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              default_get_feature_request_data,
                                              MetadataOptions.defaults())
    assert len(get_features_request.metadata_options) == 2

    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME,
                                 MetadataOptions.DATA_TYPE}

    assert expected_metadata_options == metadata_options


def test_default_metadata_options() -> None:
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              default_get_feature_request_data,
                                              None)
    assert len(get_features_request.metadata_options) == 2

    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME,
                                 MetadataOptions.DATA_TYPE}

    assert expected_metadata_options == metadata_options


def test_json_with_custom_metadata_options() -> None:
    local_get_feature_request_data = \
        GetFeatureRequestData(join_key_map={"test_key": "test_value"},
                              request_context_map={"test_key": 123.45})

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              local_get_feature_request_data,
                                              {MetadataOptions.NAME,
                                               MetadataOptions.SLO_INFO})

    assert len(get_features_request.metadata_options) == 3

    expected_response = GetFeaturesRequestJSON(
        feature_service_name="test_feature_service_name",
        join_key_map={"test_key": "test_value"},
        metadata_options={"include_data_types": True,
                          "include_names": True,
                          "include_slo_info": True
                          },
        request_context_map={"test_key": 123.45},
        workspace_name="test_workspace_name"
    ).to_dict

    expected_json = json.dumps({"params": expected_response})
    actual_json = get_features_request.request_to_json

    assert actual_json == expected_json


def test_json_with_all_metadata_options() -> None:
    local_get_feature_request_data = \
        GetFeatureRequestData(join_key_map={"test_key": "test_value"},
                              request_context_map={"test_key": 123.45})

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              local_get_feature_request_data,
                                              {MetadataOptions.NAME,
                                               MetadataOptions.DATA_TYPE,
                                               MetadataOptions.SLO_INFO,
                                               MetadataOptions.FEATURE_STATUS,
                                               MetadataOptions.EFFECTIVE_TIME})

    assert len(get_features_request.metadata_options) == 5

    expected_response = GetFeaturesRequestJSON(
        feature_service_name="test_feature_service_name",
        join_key_map={"test_key": "test_value"},
        metadata_options={"include_data_types": True,
                          "include_effective_times": True,
                          "include_names": True,
                          "include_serving_status": True,
                          "include_slo_info": True
                          },
        request_context_map={"test_key": 123.45},
        workspace_name="test_workspace_name"
    ).to_dict

    expected_json = json.dumps({"params": expected_response})
    actual_json = get_features_request.request_to_json

    assert actual_json == expected_json
