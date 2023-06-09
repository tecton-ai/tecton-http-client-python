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
        """Initializing parameters required to make HTTP requests

        :param url: URL to ping
        :param api_key: API Key required as part of header authorization
        :param client: (Optional) HTTPX Asynchronous Client
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
        """Checks if the client is closed
        :return: True if the client is closed, False otherwise
        """
        return self.is_client_closed

    async def execute_request(self: Self, endpoint: str, request_body: dict) -> str:
        """This is a method that performs a given HTTP request
        to an endpoint in the method passed by client

        :param request_body: request data to be passed
        :param endpoint: HTTP endpoint to attach to the URL and query
        :type request_body: String in JSON format
        :type endpoint: String
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
        """Validate that a given url string is a valid URL"""

        if not url or not urlparse(url).netloc:
            raise InvalidURLException(InvalidParameterMessage.URL.value)

        return url

    @staticmethod
    def validate_key(api_key: Optional[str]) -> str:
        """Validate that a given api key string is valid"""
        if not api_key:
            raise InvalidParameterException(InvalidParameterMessage.KEY.value)

        return api_key
