import asyncio
from datetime import timedelta
from typing import Final
from typing import Union
from urllib.parse import urljoin

import pytest
from aioresponses import aioresponses
from aioresponses.core import CallbackResult

from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidURLError
from tecton_client.exceptions import TectonServerException
from tecton_client.exceptions import UnauthorizedError
from tecton_client.http_client import HTTPRequest
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
    http_client = TectonHttpClient(
        url=URL,
        api_key=API_KEY,
        client_options=client_options,
    )

    def test_http_client(self) -> None:
        assert not self.http_client.is_closed

    @pytest.mark.asyncio
    async def test_perform_http_request_success(self, mocked: aioresponses) -> None:
        mocked.post(
            url=self.full_url,
            payload={"result": {"features": ["1", 11292.571748310578, "other", 35.6336, -99.2427, None, "5", "25"]}},
        )
        http_request = HTTPRequest(endpoint=self.endpoint, request_body=self.request)
        response = await self.http_client.execute_request(request=http_request)
        assert isinstance(response.result, dict)

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
        try:
            http_request = HTTPRequest(endpoint=self.endpoint, request_body=self.request)
            await self.http_client.execute_request(request=http_request)
        except Exception as e:
            # Confirm that a child error of :class:`TectonServerException` is raised
            assert isinstance(e, TectonServerException)

    @pytest.mark.parametrize("url", ["", None, "###", "somesite"])
    def test_invalid_url(self, url: object) -> None:
        with pytest.raises(InvalidURLError):
            TectonHttpClient(
                url=url,
                api_key="1234",
                client_options=self.client_options,
            )

    @pytest.mark.parametrize("key", ["", None])
    def test_empty_or_none_key(self, key: object) -> None:
        with pytest.raises(InvalidParameterError):
            TectonHttpClient(
                url=self.URL,
                api_key=key,
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
        try:
            http_request = HTTPRequest(endpoint=self.endpoint, request_body=self.request)
            await self.http_client.execute_request(request=http_request)
        except TectonServerException as e:
            assert isinstance(e, UnauthorizedError)

    @pytest.mark.asyncio
    async def test_default_client_options(self) -> None:
        assert self.http_client._client.timeout.connect == 2
        assert self.http_client._client.timeout.total == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("number_of_requests", [10, 50, 100, 500])
    async def test_parallel_requests(self, mocked: aioresponses, number_of_requests: int) -> None:
        mocked.post(
            url=self.full_url,
            payload={"result": {"features": ["1", 11292.571748310578, "other", 35.6336, -99.2427, None, "5", "25"]}},
            repeat=True,
        )

        requests_list = [self.request] * number_of_requests
        responses_list, latency = await self.http_client.execute_parallel_requests(
            self.endpoint, requests_list, timedelta(seconds=1)
        )

        assert len(responses_list) == len(requests_list)
        assert all(isinstance(response.result, dict) for response in responses_list)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("number_of_requests", [10, 100])
    async def test_parallel_requests_smaller_timeout(self, mocked: aioresponses, number_of_requests: int) -> None:
        async def delayed_callback(request_url: str, **kwargs: Union[str, bool, dict]) -> dict:
            # This is the function that sends a mock response when the client sends a request
            # Here, `**kwargs` represents information such as the request data, headers etc. needed to parse a request
            await asyncio.sleep(1)
            return {"result": {"features": ["1", 11292.571748310578, "other", 35.6336, -99.2427, None, "5", "25"]}}

        mocked.post(
            url=self.full_url,
            callback=delayed_callback,
            repeat=True,
        )

        requests_list = [self.request] * number_of_requests
        responses_list, latency = await self.http_client.execute_parallel_requests(
            self.endpoint, requests_list, timedelta(milliseconds=1)
        )

        assert len(responses_list) == len(requests_list)
        # No request should complete in this timeout, resulting in all returned responses being empty
        assert all(response is None for response in responses_list)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("number_of_requests", [2])
    async def test_order_of_parallel_requests(self, mocked: aioresponses, number_of_requests: int) -> None:
        params2 = {
            "feature_service_name": "fraud_detection_feature_service",
            "join_key_map": {"user_id": "user_205125746682"},
            "request_context_map": {"merch_long": 35.0, "amt": 500.0, "merch_lat": 30.0},
            "workspace_name": "tecton-fundamentals-tutorial-live",
            "metadata_options": {"include_names": True, "include_data_types": True},
        }
        request2 = {"params": params2}
        requests_list = [self.request, request2] * number_of_requests

        for i in range(1, len(requests_list) + 1):
            mocked.post(
                url=self.full_url,
                payload={
                    "result": {"features": [str(i), 11292.571748310578, "other", 35.6336, -99.2427, None, "5", "25"]}
                },
            )

        responses_list, latency = await self.http_client.execute_parallel_requests(
            self.endpoint, requests_list, timedelta(seconds=1)
        )

        assert len(responses_list) == len(requests_list)
        assert all(response is not None for response in responses_list)

        for response in responses_list:
            if response:
                assert isinstance(response.result, dict)
                # Testing out order of responses by checking the value stored in the first feature of the features list
                assert response.result["result"]["features"][0] == str(responses_list.index(response) + 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("number_of_requests", [10, 50])
    async def test_parallel_requests_partial(self, mocked: aioresponses, number_of_requests: int) -> None:
        async def conditional_callback(request_url: str, **kwargs: Union[str, bool, dict]) -> dict:
            # This is the function that sends a mock response when the client sends a request
            # Here, `**kwargs` represents information such as the request data, headers etc. needed to parse a request
            if kwargs["json"]["params"]["request_context_map"]["amt"] % 2 == 0:
                await asyncio.sleep(2)
            return CallbackResult(
                status=200,
                payload={
                    "result": {
                        "features": [
                            "1",
                            kwargs["json"]["params"]["request_context_map"]["amt"],
                            "other",
                            35.6336,
                            -99.2427,
                            None,
                            "5",
                            "25",
                        ]
                    }
                },
            )

        mocked.post(
            url=self.full_url,
            callback=conditional_callback,
            repeat=True,
        )

        requests_list = []
        for i in range(number_of_requests):
            requests_list.append({"params": {"request_context_map": {"merch_long": 35.0, "amt": i, "merch_lat": 30.0}}})
        responses_list, latency = await self.http_client.execute_parallel_requests(
            self.endpoint, requests_list, timedelta(seconds=1)
        )
        assert len(responses_list) == len(requests_list)
        assert latency < timedelta(seconds=1.5)
        assert all(isinstance(response.result, dict) for response in responses_list if response)
        for i in range(number_of_requests):
            if i % 2 == 0:
                assert responses_list[i] is None
            else:
                assert responses_list[i].result == {
                    "result": {"features": ["1", i, "other", 35.6336, -99.2427, None, "5", "25"]}
                }

    @pytest.mark.asyncio
    async def pytest_sessionfinish(self) -> None:
        await self.http_client.close()
