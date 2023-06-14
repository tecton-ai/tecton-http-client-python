from enum import Enum
from typing import Optional
from typing import Self
from urllib.parse import urljoin
from urllib.parse import urlparse

import httpx
from httpx_auth import HeaderApiKey

from tecton_client.exceptions import INVALID_SERVER_RESPONSE
from tecton_client.exceptions import InvalidParameterException
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.exceptions import InvalidURLException
from tecton_client.exceptions import TectonServerException


API_PREFIX = "Tecton-key"


class TectonHttpClient:
    """Basic HTTP Client to send and receive requests to a given URL."""

    class headers(Enum):
        AUTHORIZATION = 'Authorization'
        ACCEPT = 'Accept'
        CONTENT_TYPE = 'Content-Type'

    def __init__(self: Self, url: str, api_key: str, client: Optional[httpx.AsyncClient] = None) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The URL to ping.
            api_key (str): The API Key required as part of header authorization.
            client (Optional[httpx.AsyncClient]): (Optional) The HTTPX Asynchronous Client.

        """
        self.url = self.validate_url(url)
        self.api_key = self.validate_key(api_key)

        self.auth = HeaderApiKey(header_name=self.headers.AUTHORIZATION.value,
                                 api_key=f"{API_PREFIX} {self.api_key}")

        self.client: httpx.AsyncClient = client or httpx.AsyncClient()
        self.is_client_closed: bool = False

    async def close(self: Self) -> None:
        await self.client.aclose()
        self.is_client_closed = True

    @property
    def is_closed(self: Self) -> bool:
        """Checks if the client is closed.

        Returns:
            bool: True if the client is closed, False otherwise.

        """
        return self.is_client_closed

    async def execute_request(self: Self, endpoint: str, request_body: dict) -> str:
        """Performs an HTTP request to a specified endpoint using the client.

        This method sends an HTTP POST request to the specified endpoint, attaching the provided request body data.

        Args:
            endpoint (str): The HTTP endpoint to attach to the URL and query.
            request_body (dict): The request data to be passed, in JSON format.

        Returns:
            str: The response in JSON format.

        Raises:
            TectonServerException: If the server response is invalid.

        """
        url = urljoin(self.url, endpoint)

        response = await self.client.post(url, data=request_body, auth=self.auth)

        if response.status_code == 200:
            return response.json()
        else:
            raise TectonServerException(INVALID_SERVER_RESPONSE(response.status_code, response.reason_phrase,
                                                                response.json()['message']))

    @staticmethod
    def validate_url(url: Optional[str]) -> str:
        """Validate that a given URL string is a valid URL.

        Args:
            url (Optional[str]): The URL string to validate.

        Returns:
            str: The validated URL string.

        Raises:
            InvalidURLException: If the URL is empty or does not have a valid netloc.

        """
        if not url or not urlparse(url).netloc:
            raise InvalidURLException(InvalidParameterMessage.URL.value)

        return url

    @staticmethod
    def validate_key(api_key: Optional[str]) -> str:
        """Validate that a given API key string is valid.

        Args:
            api_key (Optional[str]): The API key string to validate.

        Returns:
            str: The validated API key string.

        Raises:
            InvalidParameterException: If the API key is empty.

        """
        if not api_key:
            raise InvalidParameterException(InvalidParameterMessage.KEY.value)

        return api_key
