from typing import Final
from urllib.parse import urljoin

import pytest
from aioresponses import aioresponses

from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidURLError
from tecton_client.exceptions import TectonServerException
from tecton_client.exceptions import UnauthorizedError
from tecton_client.http_client import TectonHttpClient
from tecton_client.tecton_client import TectonClientOptions


@pytest.fixture
def mocked() -> aioresponses:
    with aioresponses() as mock:
        yield mock


class TestHttpClient:
    URL: Final[str] = "https://thisisaurl.ai"
    API_KEY: Final[str] = "abcd1234"
    client_options = TectonClientOptions()

    endpoint: Final[str] = "api/v1/feature-service/get-features"
    full_url: Final[str] = urljoin(URL, endpoint)
    params = {
        "feature_service_name": "fraud_detection_feature_service",
        "join_key_map": {"user_id": "user_205125746682"},
        "request_context_map": {"merch_long": 35.0, "amt": 500.0, "merch_lat": 30.0},
        "workspace_name": "tecton-fundamentals-tutorial-live",
        "metadata_options": None,
    }
    request = {"params": params}

    @pytest.mark.asyncio
    async def test_http_client(self) -> None:
        http_client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            client_options=self.client_options,
        )
        assert not http_client.is_closed
        await http_client.close()

    @pytest.mark.asyncio
    async def test_perform_http_request_success(self, mocked: aioresponses) -> None:
        mocked.post(
            url=self.full_url,
            payload={"result": {"features": ["1", 11292.571748310578, "other", 35.6336, -99.2427, None, "5", "25"]}},
        )

        http_client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            client_options=self.client_options,
        )

        response = await http_client.execute_request(self.endpoint, self.request)

        assert type({}) == type(response)
        await http_client.close()

    @pytest.mark.asyncio
    async def test_perform_http_request_failure(self, mocked: aioresponses) -> None:
        mocked.post(
            url=self.full_url,
            status=401,
            payload={
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
            client_options=self.client_options,
        )

        try:
            await http_client.execute_request(self.endpoint, self.request)

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
                client_options=self.client_options,
            )

    @pytest.mark.parametrize("key", ["", None])
    def test_empty_or_none_key(self, key: object) -> None:
        with pytest.raises(InvalidParameterError):
            TectonHttpClient(
                self.URL,
                key,
                client_options=self.client_options,
            )

    @pytest.mark.asyncio
    async def test_invalid_api_key(self, mocked: aioresponses) -> None:
        mocked.post(
            url=self.full_url,
            status=401,
            reason="Unauthorized: invalid 'Tecton-key' authorization header. "
            "Newly created credentials may take up to 60 seconds to be usable.",
            payload={
                "error": "invalid 'Tecton-key' authorization header. Note that newly created credentials may "
                "take up to 60 seconds to be usable.",
                "message": "invalid 'Tecton-key' authorization header. Note that newly created credentials may "
                "take up to 60 seconds to be usable.",
                "code": 16,
            },
        )

        client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            client_options=self.client_options,
        )
        try:
            await client.execute_request(self.endpoint, self.request)
        except TectonServerException as e:
            assert isinstance(e, UnauthorizedError)
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_default_client_options(self) -> None:
        client = TectonHttpClient(
            self.URL,
            self.API_KEY,
            client_options=self.client_options,
        )
        assert client._client.timeout.connect == 2
        assert client._client.timeout.total == 2

        await client.close()
