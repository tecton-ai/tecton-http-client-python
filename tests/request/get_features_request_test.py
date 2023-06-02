import pytest

from tecton_client.exceptions.exceptions import TectonClientException
from tecton_client.model.metadata_option import MetadataOptions
from tecton_client.request.get_feature_request_data import \
    GetFeatureRequestData
from tecton_client.request.get_features_request import GetFeaturesRequest
from tecton_client.request.request_constants import DEFAULT_METADATA_OPTIONS, ALL_METADATA_OPTIONS

TEST_WORKSPACE_NAME = "test_workspace_name"
TEST_FEATURE_SERVICE_NAME = "test_feature_service_name"
ENDPOINT = "/api/v1/feature-service/get-features"

default_metadata_options = DEFAULT_METADATA_OPTIONS

default_get_feature_request_data = GetFeatureRequestData(
    join_key_map={"test_key": "test_value"})


@pytest.mark.parametrize("workspace", ["", None])
def test_error_workspace_name(workspace: str) -> None:
    with pytest.raises(TectonClientException):
        GetFeaturesRequest(workspace, TEST_FEATURE_SERVICE_NAME,
                           default_metadata_options,
                           default_get_feature_request_data)


@pytest.mark.parametrize("feature_service", ["", None])
def test_error_feature_service_name(feature_service: str) -> None:
    with pytest.raises(TectonClientException):
        GetFeaturesRequest(TEST_WORKSPACE_NAME, feature_service,
                           default_metadata_options,
                           default_get_feature_request_data)


def test_empty_maps() -> None:
    empty_get_feature_request_data = GetFeatureRequestData()
    with pytest.raises(TectonClientException):
        GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME,
                           default_metadata_options,
                           empty_get_feature_request_data)


def test_simple_request_with_none_join_key() -> None:
    local_get_feature_request_data = GetFeatureRequestData(
    join_key_map={"test_key": "test_value"})

    local_get_feature_request_data.add_join_key("test_none_key", None)
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              default_metadata_options,
                                              local_get_feature_request_data)

    assert get_features_request.endpoint == ENDPOINT
    assert get_features_request.workspace_name == TEST_WORKSPACE_NAME
    assert get_features_request.feature_service_name == \
           TEST_FEATURE_SERVICE_NAME
    get_features_request_data = get_features_request.get_feature_request_data
    assert get_features_request_data.is_empty_request_context_map() is True

    join_key_map = get_features_request.get_feature_request_data.join_key_map
    assert len(join_key_map) == 2
    assert join_key_map.get("test_key") == "test_value"
    assert join_key_map.get("test_none_key") is None

    json_request = get_features_request.request_to_json()
    expected_json_request = '{"params": {"feature_service_name": "test_feature_service_name", ' \
                              '"join_key_map": {"test_key": "test_value", "test_none_key": null}, ' \
                              '"metadata_options": {"include_data_types": true, "include_names": true}, ' \
                              '"request_context_map": {}, "workspace_name": "test_workspace_name"}}'

    assert json_request == expected_json_request


def test_request_with_request_context_map() -> None:
    local_get_feature_request_data = GetFeatureRequestData(
    join_key_map={"test_key": "test_value"})

    local_get_feature_request_data.add_request_context("test_key_1", 123.45)
    local_get_feature_request_data.add_request_context("test_key_2",
                                                         "test_val")

    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME,
                                              TEST_FEATURE_SERVICE_NAME,
                                              default_metadata_options,
                                              local_get_feature_request_data)

    assert get_features_request.endpoint == ENDPOINT
    assert get_features_request.workspace_name == \
           TEST_WORKSPACE_NAME
    assert get_features_request.feature_service_name == \
           TEST_FEATURE_SERVICE_NAME

    request_context_map = get_features_request. \
        get_feature_request_data.request_context_map

    assert len(request_context_map) == 2
    assert request_context_map.get("test_key_1") == "123.45"
    assert request_context_map.get("test_key_2") == "test_val"

    json_request = get_features_request.request_to_json()

    expected_json_request = '{"params": {"feature_service_name": "test_feature_service_name", ' \
                              '"join_key_map": {"test_key": "test_value"}, ' \
                              '"metadata_options": {"include_data_types": true, "include_names": true}, ' \
                              '"request_context_map": {"test_key_1": "123.45", "test_key_2": "test_val"}, ' \
                              '"workspace_name": "test_workspace_name"}}'

    assert json_request == expected_json_request


def test_all_metadata_options():
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME, {MetadataOptions.ALL},
                                              default_get_feature_request_data)

    assert len(get_features_request.metadata_options) == 5
    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE,
                                 MetadataOptions.EFFECTIVE_TIME, MetadataOptions.FEATURE_STATUS,
                                 MetadataOptions.SLO_INFO}

    assert expected_metadata_options == metadata_options


def test_custom_metadata_options():
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME, DEFAULT_METADATA_OPTIONS, default_get_feature_request_data)
    assert len(get_features_request.metadata_options) == 2

    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}

    assert expected_metadata_options == metadata_options


def test_default_metadata_options():
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME, None, default_get_feature_request_data)
    assert len(get_features_request.metadata_options) == 2

    metadata_options = get_features_request.metadata_options
    expected_metadata_options = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}

    assert expected_metadata_options == metadata_options


def test_json_with_custom_metadata_options():
    local_get_feature_request_data = GetFeatureRequestData(
        join_key_map={"test_key": "test_value"})

    local_get_feature_request_data.add_request_context("test_key", 123.45)
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME, {MetadataOptions.NAME, MetadataOptions.SLO_INFO}, local_get_feature_request_data)

    assert len(get_features_request.metadata_options) == 3

    expected_json = '{"params": {"feature_service_name": "test_feature_service_name", ' \
                    '"join_key_map": {"test_key": "test_value"}, "metadata_options": ' \
                    '{"include_data_types": true, "include_names": true, "include_slo_info": true}, ' \
                    '"request_context_map": {"test_key": "123.45"}, "workspace_name": ' \
                    '"test_workspace_name"}}'
    actual_json = get_features_request.request_to_json()
    assert actual_json == expected_json

def test_json_with_all_metadata_options():
    default_get_feature_request_data.add_request_context("test_key", 123.45)
    get_features_request = GetFeaturesRequest(TEST_WORKSPACE_NAME, TEST_FEATURE_SERVICE_NAME, {MetadataOptions.ALL}, default_get_feature_request_data)

    assert len(get_features_request.metadata_options) == 5

    expected_json = '{"params": {"feature_service_name": "test_feature_service_name", ' \
                    '"join_key_map": {"test_key": "test_value"}, "metadata_options": ' \
                    '{"include_data_types": true, "include_effective_times": true, ' \
                    '"include_names": true, "include_serving_status": true, ' \
                    '"include_slo_info": true}, "' \
                    'request_context_map": {"test_key": "123.45"}, ' \
                    '"workspace_name": "test_workspace_name"}}'
    actual_json = get_features_request.request_to_json()
    print(actual_json)

    assert actual_json == expected_json
