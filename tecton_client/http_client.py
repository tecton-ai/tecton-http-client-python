import json
from enum import Enum
from typing import Optional
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from httpx_auth import HeaderApiKey

from tecton_client.exceptions import INVALID_SERVER_RESPONSE
from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.exceptions import InvalidURLError
from tecton_client.exceptions import SERVER_ERRORS
from tecton_client.exceptions import TectonServerException


API_PREFIX = "Tecton-key"


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
        connect_timeout: float,
        read_timeout: float,
        keepalive_expiry: Optional[int],
        max_connections: Optional[int],
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The URL to ping.
            api_key (str): The API Key required as part of header authorization.
            connect_timeout (float): The maximum amount of time to wait until a socket connection to the requested host
                is established. If the HTTP client is unable to connect within this time frame,
                a ConnectTimeout exception is raised.
            read_timeout (float): The maximum duration to wait for a chunk of data to be received
                (for example, a chunk of the response body). If the HTTP client is unable to receive data within this
                time frame, a ReadTimeout exception is raised.
            keepalive_expiry (Optional[int]): The time limit on idle keep-alive connections in seconds,
                or None for no limits.
            max_connections (Optional[int]): (Optional) maximum number of allowable connections, or None for no limits.
            client (Optional[httpx.AsyncClient]): (Optional) The HTTPX Asynchronous Client.
                Users can initialize their own HTTPX client and pass it in, otherwise the TectonHttpClient object
                will initialize its own HTTPX client.

        """
        self._url = self._validate_url(url)
        self._api_key = self._validate_key(api_key)

        self._auth = HeaderApiKey(header_name=self.headers.AUTHORIZATION.value, api_key=f"{API_PREFIX} {self._api_key}")
        self._client: httpx.AsyncClient = client or httpx.AsyncClient(
            timeout=httpx.Timeout(5, connect=connect_timeout, read=read_timeout),
            limits=httpx.Limits(keepalive_expiry=keepalive_expiry, max_connections=max_connections),
        )
        self._is_client_closed: bool = False

    async def close(self) -> None:
        """Close the HTTPX Asynchronous Client."""
        await self._client.aclose()
        self._is_client_closed = True

    @property
    def is_closed(self) -> bool:
        """Checks if the client is closed.

        Returns:
            bool: True if the client is closed, False otherwise.

        """
        return self._is_client_closed

    async def execute_request(self, endpoint: str, request_body: dict) -> dict:
        """Performs an HTTP request to a specified endpoint using the client.

        This method sends an HTTP POST request to the specified endpoint, attaching the provided request body data.

        Args:
            endpoint (str): The HTTP endpoint to attach to the URL and query.
            request_body (dict): The request data to be passed, in JSON format.

        Returns:
            dict: The response in JSON format.

        Raises:
            TectonServerException: If the server returns an error response, different errors based on the
                error response are raised.

        """
        url = urljoin(self._url, endpoint)

        # HTTPX requires the data provided to the request to be a string and not a dictionary.
        # The request dictionary is therefore converted to a JSON formatted string and passed in.
        # For more information, please check the HTTPX documentation `here <https://www.python-httpx.org/quickstart/>`_.
        response = await self._client.post(url, data=json.dumps(request_body), auth=self._auth)

        if response.status_code == 200:
            return response.json()
        else:
            message = INVALID_SERVER_RESPONSE(response.status_code, response.reason_phrase, response.json()["message"])

            try:
                raise SERVER_ERRORS.get(response.status_code)(message)
            except TypeError:
                raise TectonServerException(message)

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
