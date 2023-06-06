from typing import Self
from typing import Optional

import httpx
from enum import Enum

from httpx_auth import HeaderApiKey
from urllib.parse import urlparse

from tecton_client.exceptions.exceptions import (
    TectonServerException,
    InvalidParameterException,
    InvalidParameterMessages
)

API_PREFIX = "Tecton-key"


class TectonHttpClient:
    """
    Basic HTTP Client to send and receive requests to a given URL
    """

    class headers(Enum):
        AUTHORIZATION = 'Authorization'
        ACCEPT = 'Accept'
        CONTENT_TYPE = 'Content-Type'

    def __init__(self: Self, url: str, api_key: str,
                 client: Optional[httpx.AsyncClient] = None) -> None:
        """

        :param url: URL to ping
        :param api_key: API Key required as part of header authorization
        :param client: HTTP Asynchronous Client
        """

        self.url = self.validate_url(url)
        self.api_key = self.validate_key(api_key)

        self.auth = HeaderApiKey(header_name=self.headers.AUTHORIZATION.value,
                                 api_key=f"{API_PREFIX} {self.api_key}")

        self.client: httpx.AsyncClient = client or httpx.AsyncClient()
        self.is_client_closed = False

    async def close(self: Self) -> None:
        await self.client.aclose()
        self.is_client_closed = True

    @property
    def is_closed(self: Self) -> bool:
        return self.is_client_closed

    async def execute_request(self: Self, endpoint: str,
                              http_request: dict) -> str:
        """
        This is a method that performs a given HTTP request
        to an endpoint in the method passed by client

        :param http_request: request data to be passed
        :param endpoint: HTTP endpoint to attach to the URL and query
        :type http_request: String in JSON format
        :type endpoint: String
        """
        url = self.url + "/" + endpoint

        response = await self.client.post(url, data=http_request,
                                          auth=self.auth)

        if response.status_code == 200:
            return response.json()
        else:
            error_message = str(response.status_code) + " " \
                            + response.reason_phrase + ": " \
                            + response.json()['message']
            raise TectonServerException(error_message)

    @staticmethod
    def validate_url(url: Optional[str]) -> str:

        """
        Validate that a given URL is valid
        """

        # If the URL is empty or None, raise an exception
        if not url:
            raise InvalidParameterException(InvalidParameterMessages.URL)
        # Otherwise, try parsing the URL and raise an exception if it fails
        try:
            result = urlparse(url)
            if not all([result.scheme, result.netloc]):
                raise Exception
        except Exception:
            raise InvalidParameterException(InvalidParameterMessages.URL)

        return url

    @staticmethod
    def validate_key(api_key: Optional[str]) -> str:
        if not api_key:
            raise InvalidParameterException(InvalidParameterMessages.URL)

        return api_key
