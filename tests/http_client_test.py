from pytest_httpx import HTTPXMock

from tecton_client.exceptions import (
    TectonServerException,
    InvalidParameterException
)
from tecton_client.tecton_client import TectonClient
from tecton_client.http_client import TectonHttpClient
import pytest

url = "https://thisisaurl.ai"
api_key = "abcd1234"


@pytest.mark.asyncio
async def test_http_client() -> None:
    http_client = TectonHttpClient(url, api_key)
    assert http_client.is_closed is False
    await http_client.close()


@pytest.mark.asyncio
async def test_perform_http_request_success(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        json={'result': {'features': ['1', 11292.571748310578,
                                      'other', 35.6336, -99.2427,
                                      None, '5', '25']}})

    http_client = TectonHttpClient(url, api_key)

    endpoint = "api/v1/feature-service/get-features"
    params = {
        "feature_service_name": "fraud_detection_feature_service",
        "join_key_map": {"user_id": "user_205125746682"},
        "request_context_map": {"merch_long": 35.0, "amt": 500.0,
                                "merch_lat": 30.0},
        "workspace_name": "tecton-fundamentals-tutorial-live",
        "metadata_options": None}
    request = {"params": params}

    response = await http_client.execute_request(
        endpoint, request)

    assert type({}) == type(response)
    await http_client.close()


@pytest.mark.asyncio
async def test_perform_http_request_failure(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=401, json={
        "error": "invalid 'Tecton-key' authorization header. "
                 "Newly created credentials may take up to "
                 "60 seconds to be usable.",
        "message": "invalid 'Tecton-key' authorization header. "
                   "Newly created credentials may take up to "
                   "60 seconds to be usable.",
        "code": 16
    })

    http_client = TectonHttpClient(url, api_key)

    endpoint = "api/v1/feature-service/get-features"
    params = {
        "feature_service_name": "fraud_detection_feature_service",
        "join_key_map": {"user_id": "user_205125746682"},
        "request_context_map": {"merch_long": 35.0, "amt": 500.0,
                                "merch_lat": 30.0},
        "workspace_name": "tecton-fundamentals-tutorial-live",
        "metadata_options": None}
    request = {"params": params}

    try:
        await http_client.execute_request(
            endpoint, request)

    except Exception as e:
        assert type(e) == TectonServerException

    await http_client.close()


@pytest.mark.parametrize("url", ["", None, "###", "somesite"])
def test_invalid_url(url: object) -> None:
    with pytest.raises(InvalidParameterException):
        TectonClient(url, "1234")


@pytest.mark.parametrize("key", ["", None])
def test_empty_or_none_key(key: object) -> None:
    with pytest.raises(InvalidParameterException):
        TectonClient(url, key)


def test_invalid_api_key(httpx_mock: HTTPXMock) -> None:
    expected_message = "401 Unauthorized: invalid 'Tecton-key' " \
                       "authorization header. Newly created credentials " \
                       "may take up to 60 seconds to be usable."

    try:
        TectonClient(url, api_key)
    except TectonServerException as e:
        assert e == expected_message
