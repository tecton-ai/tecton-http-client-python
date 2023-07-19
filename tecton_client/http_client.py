import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional
from urllib.parse import urljoin
from urllib.parse import urlparse

import aiohttp

from tecton_client.client_options import TectonClientOptions
from tecton_client.exceptions import INVALID_SERVER_RESPONSE
from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.exceptions import InvalidURLError
from tecton_client.exceptions import SERVER_ERRORS
from tecton_client.exceptions import TectonClientError
from tecton_client.exceptions import TectonServerException

API_PREFIX = "Tecton-key"


def _get_default_client(client_options: TectonClientOptions) -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            connect=client_options.connect_timeout.seconds, total=client_options.read_timeout.seconds
        ),
        connector=aiohttp.TCPConnector(
            limit=client_options.max_connections, keepalive_timeout=client_options.keepalive_expiry.seconds
        ),
    )


@dataclass
class HTTPResponse:
    """Represents an HTTP response object to capture the result of making an HTTP request.

    Attributes:
        exception (Optional[Exception]): The server exception if one occurred while making the HTTP request, else None.
        result (Optional[dict]): The result of the HTTP request, if the request was successful, else None.
        latency (Optional[timedelta]): The latency of the HTTP request, if the request was successful, else None.

    """

    exception: Optional[Exception] = None
    result: Optional[dict] = None
    latency: Optional[timedelta] = None


class TectonHttpClient:
    """Basic HTTP Client to send and receive requests to a given URL."""

    class headers(Enum):
        """Enum class for HTTP headers."""

        AUTHORIZATION = "Authorization"
        ACCEPT = "Accept"
        CONTENT_TYPE = "Content-Type"

    def __init__(
        self,
        url: str,
        api_key: str,
        client_options: TectonClientOptions,
        client: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The URL to ping.
            api_key (str): The API Key required as part of header authorization.
            client_options (TectonClientOptions): The configurations for the HTTP Client initialized by
                :class:`TectonHttpClient`.
            client (Optional[aiohttp.ClientSession]): (Optional) The HTTP Asynchronous Client.
                Users can initialize their own HTTP client and pass it in, otherwise the :class:`TectonHttpClient`
                object will initialize its own HTTP client.

        """
        self._url = self._validate_url(url)
        self._api_key = self._validate_key(api_key)

        self._auth = {self.headers.AUTHORIZATION.value: f"{API_PREFIX} {self._api_key}"}
        self._client: aiohttp.ClientSession = client or _get_default_client(client_options)
        self._is_client_closed: bool = False

    async def close(self) -> None:
        """Close the HTTP Asynchronous Client."""
        await self._client.close()
        self._is_client_closed = True

    @property
    def is_closed(self) -> bool:
        """Checks if the client is closed.

        Returns:
            bool: True if the client is closed, False otherwise.

        """
        return self._is_client_closed

    async def execute_request(self, endpoint: str, request_body: dict) -> HTTPResponse:
        """Performs an HTTP request to a specified endpoint using the client.

        This method sends an HTTP POST request to the specified endpoint, attaching the provided request body data.

        Args:
            endpoint (str): The HTTP endpoint to attach to the URL and query.
            request_body (dict): The request data to be passed, in JSON format.

        Returns:
            HTTPResponse: An :class:`HTTPResponse` object containing the result and the latency of the HTTP request.

        Raises:
            TectonServerException: If the server returns an error response, different errors based on the
                error response are raised.
            TectonClientError: If the client encounters an error while making the request.

        """
        url = urljoin(self._url, endpoint)

        try:
            start_time = time.time()
            async with self._client.post(url, json=request_body, headers=self._auth) as response:
                json_response = await response.json()
            end_time = time.time()
            request_latency = timedelta(seconds=(end_time - start_time))

            if response.status == 200:
                return HTTPResponse(result=json_response, latency=request_latency)
            else:
                message = INVALID_SERVER_RESPONSE(response.status, response.reason, json_response["message"])
                error_class = SERVER_ERRORS.get(response.status, TectonServerException)
                raise error_class(message)
        except aiohttp.ClientError as e:
            raise TectonClientError from e

    @staticmethod
    def _validate_url(url: Optional[str]) -> str:
        """Validate that a given URL string is a valid URL.

        Args:
            url (Optional[str]): The URL string to validate.

        Returns:
            str: The validated URL string.

        Raises:
            InvalidURLError: If the URL is invalid or empty.

        """
        if not url or not urlparse(url).netloc:
            raise InvalidURLError(InvalidParameterMessage.URL.value)

        return url

    @staticmethod
    def _validate_key(api_key: Optional[str]) -> str:
        """Validate that a given API key string is valid.

        Args:
            api_key (Optional[str]): The API key string to validate.

        Returns:
            str: The validated API key string.

        Raises:
            InvalidParameterError: If the API key is empty.

        """
        if not api_key:
            raise InvalidParameterError(InvalidParameterMessage.KEY.value)

        return api_key
