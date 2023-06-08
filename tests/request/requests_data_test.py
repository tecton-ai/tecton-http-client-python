import pytest

from tecton_client.exceptions.exceptions import TectonInvalidParameterException
from tecton_client.request.requests_data import GetFeatureRequestData


@pytest.mark.parametrize("key", ["", None])
def test_error_join_key(key: str) -> None:
    with pytest.raises(TectonInvalidParameterException):
        default_get_feature_request_data.add_join_key(key, "testValue")


@pytest.mark.parametrize("key", ["", None])
def test_error_request_context_key(key: str) -> None:
    with pytest.raises(TectonInvalidParameterException):
        default_get_feature_request_data.add_request_context(key, "test_value")


def test_empty_join_value() -> None:
    with pytest.raises(TectonInvalidParameterException):
        default_get_feature_request_data.add_join_key("test_key", "")


def test_none_join_value() -> None:
<<<<<<< HEAD
    default_get_feature_request_data.add_join_key("test_key", None)
    assert default_get_feature_request_data.join_key_map["test_key"] is None
=======
    get_feature_request_data = \
        GetFeatureRequestData(join_key_map={"test_key": None})
    assert get_feature_request_data.join_key_map["test_key"] is None
>>>>>>> cd43997 (Fixing conflicts 3)


@pytest.mark.parametrize("value", ["", None])
def test_error_request_context_value(value: object) -> None:
    with pytest.raises(TectonInvalidParameterException):
        default_get_feature_request_data.add_request_context("test_key", value)


def test_mixed_type_join_key_value() -> None:
    get_feature_request_data = GetFeatureRequestData()
    get_feature_request_data.add_join_key("test_string_key",
                                          "test_string_value")
    get_feature_request_data.add_join_key("test_long_key", 1234)
    join_key_map = get_feature_request_data.join_key_map

    assert join_key_map is not None
    assert len(join_key_map) == 2
    assert join_key_map.get("test_string_key") == "test_string_value"
    assert join_key_map.get("test_long_key") == "1234"


def test_mixed_type_request_context_key_value() -> None:
    get_feature_request_data = GetFeatureRequestData()
    get_feature_request_data.add_request_context("test_string_key",
                                                 "test_string_value")
    get_feature_request_data.add_request_context("test_long_key", 1234)
    get_feature_request_data.add_request_context("test_float_key", 123.45)

    request_context_map = get_feature_request_data.request_context_map

    assert request_context_map is not None
    assert len(request_context_map) == 3
    assert request_context_map.get("test_string_key") == "test_string_value"
    assert request_context_map.get("test_long_key") == "1234"
    assert request_context_map.get("test_float_key") == "123.45"


def test_join_key_and_request_context() -> None:
    join_key_map = {"test_join_key_1": "test_join_value_1",
                    "test_join_key_2": "test_join_value_2"}
    request_context_map = {"test_request_context_1": 1234,
                           "test_request_context_2": "test_string_value"}

    default_get_feature_request_data.add_join_key_map(join_key_map)
    default_get_feature_request_data.add_request_context_map(
        request_context_map)

    assert default_get_feature_request_data.join_key_map is not None
    assert default_get_feature_request_data.request_context_map is not None

    assert len(default_get_feature_request_data.join_key_map) == 2
    assert len(default_get_feature_request_data.request_context_map) == 2

    for k, v in default_get_feature_request_data.join_key_map.items():
        assert join_key_map.get(k) == \
               default_get_feature_request_data.join_key_map.get(k)

    for k, v in default_get_feature_request_data.join_key_map.items():
        assert request_context_map.get(k) == \
               default_get_feature_request_data.request_context_map.get(k)
