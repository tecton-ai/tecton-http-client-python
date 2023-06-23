import pytest
from pytest_httpx import HTTPXMock

from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidURLError
from tecton_client.exceptions import TectonServerException
from tecton_client.http_client import TectonHttpClient
from tecton_client.tecton_client import TectonClientOptions


class TestHttpClient:
    URL = "https://thisisaurl.ai"
    API_KEY = "abcd1234"
    client_options = TectonClientOptions()

    @pytest.mark.asyncio
    async def test_http_client(self) -> None:
        http_client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            self.client_options.connect_timeout,
            self.client_options.read_timeout,
            self.client_options.pool_timeout,
            self.client_options.keepalive_expiry,
            self.client_options.max_connections,
        )
        assert not http_client.is_closed
        await http_client.close()

    @pytest.mark.asyncio
    async def test_perform_http_request_success(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            json={"result": {"features": ["1", 11292.571748310578, "other", 35.6336, -99.2427, None, "5", "25"]}}
        )

        http_client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            self.client_options.connect_timeout,
            self.client_options.read_timeout,
            self.client_options.pool_timeout,
            self.client_options.keepalive_expiry,
            self.client_options.max_connections,
        )

        endpoint = "api/v1/feature-service/get-features"
        params = {
            "feature_service_name": "fraud_detection_feature_service",
            "join_key_map": {"user_id": "user_205125746682"},
            "request_context_map": {"merch_long": 35.0, "amt": 500.0, "merch_lat": 30.0},
            "workspace_name": "tecton-fundamentals-tutorial-live",
            "metadata_options": None,
        }
        request = {"params": params}

        response = await http_client.execute_request(endpoint, request)

        assert type({}) == type(response)
        await http_client.close()

    @pytest.mark.asyncio
    async def test_perform_http_request_failure(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            status_code=401,
            json={
                "error": "invalid 'Tecton-key' authorization header. Newly created credentials "
                "may take up to 60 seconds to be usable.",
                "message": "invalid 'Tecton-key' authorization header. Newly created credentials "
                "may take up to 60 seconds to be usable.",
                "code": 16,
            },
        )

        http_client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            self.client_options.connect_timeout,
            self.client_options.read_timeout,
            self.client_options.pool_timeout,
            self.client_options.keepalive_expiry,
            self.client_options.max_connections,
        )

        endpoint = "api/v1/feature-service/get-features"
        params = {
            "feature_service_name": "fraud_detection_feature_service",
            "join_key_map": {"user_id": "user_205125746682"},
            "request_context_map": {"merch_long": 35.0, "amt": 500.0, "merch_lat": 30.0},
            "workspace_name": "tecton-fundamentals-tutorial-live",
            "metadata_options": None,
        }
        request = {"params": params}

        try:
            await http_client.execute_request(endpoint, request)

        except Exception as e:
            # Confirm that a child error of :class:`TectonServerException` is raised
            assert isinstance(e, TectonServerException)

        await http_client.close()

    @pytest.mark.parametrize("url", ["", None, "###", "somesite"])
    def test_invalid_url(self, url: object) -> None:
        with pytest.raises(InvalidURLError):
            TectonHttpClient(
                url,
                "1234",
                self.client_options.connect_timeout,
                self.client_options.read_timeout,
                self.client_options.pool_timeout,
                self.client_options.keepalive_expiry,
                self.client_options.max_connections,
            )

    @pytest.mark.parametrize("key", ["", None])
    def test_empty_or_none_key(self, key: object) -> None:
        with pytest.raises(InvalidParameterError):
            TectonHttpClient(
                self.URL,
                key,
                self.client_options.connect_timeout,
                self.client_options.read_timeout,
                self.client_options.pool_timeout,
                self.client_options.keepalive_expiry,
                self.client_options.max_connections,
            )

    def test_invalid_api_key(self, httpx_mock: HTTPXMock) -> None:
        expected_message = (
            "401 Unauthorized: invalid 'Tecton-key' authorization header. "
            "Newly created credentials may take up to 60 seconds to be usable."
        )

        try:
            TectonHttpClient(
                self.URL,
                self.API_KEY,
                self.client_options.connect_timeout,
                self.client_options.read_timeout,
                self.client_options.pool_timeout,
                self.client_options.keepalive_expiry,
                self.client_options.max_connections,
            )
        except TectonServerException as e:
            assert e == expected_message
