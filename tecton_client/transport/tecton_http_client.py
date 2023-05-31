"""
Basic HTTP Client class to send requests and receive responses
"""
from typing import Self

import httpx
from enum import Enum
import string

from httpx_auth import HeaderApiKey
from urllib.parse import urlparse

from tecton_client.exceptions.exceptions import TectonServerException
from tecton_client.exceptions.exceptions import TectonClientException
from tecton_client.exceptions.exceptions import TectonErrorMessage

API_PREFIX = "Tecton-key"


class TectonHttpClient:
    methods = Enum('methods', ['GET', 'POST', 'PUT', 'DELETE'])

    class headers(Enum):
        AUTHORIZATION = 'Authorization'
        ACCEPT = 'Accept'
        CONTENT_TYPE = 'Content-Type'

    def __init__(self: Self, url: string, api_key: string) -> None:

        self.validate_url(url)
        self.validate_key(api_key)

        self.url = url
        self.api_key = api_key

        self.auth = HeaderApiKey(header_name=self.headers.AUTHORIZATION.value,
                                 api_key=API_PREFIX + " " + self.api_key)

        self.client = httpx.AsyncClient()
        self.is_client_closed = False

    async def close(self: Self) -> None:
        await self.client.aclose()
        self.is_client_closed = True

    @property
    def is_closed(self: Self) -> bool:
        return self.is_client_closed

    async def perform_request(self: Self, endpoint: string, method: Enum,
                              http_request: string) -> string:
        """
        This is a method that performs a given HTTP request
        to an endpoint in the method passed by client

        :param http_request: request data to be passed
        :param method: GET, PUT, POST etc.
        :param endpoint: Tecton endpoint to attach to the URL and query
        :type http_request: String in JSON format
        :type endpoint: String
        :type method: Enum
        """
        if method == self.methods.POST:
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
        else:
            # Other methods like GET, PUT and DELETE are currently unsupported
            pass

    @staticmethod
    def validate_url(url: string) -> None:
        print("ENTERED VALIDATE_URL")

        # If the URL is empty or None, raise an exception
        if not url:
            raise TectonClientException(TectonErrorMessage.INVALID_URL)

        # Otherwise, try parsing the URL and raise an exception if it fails
        try:
            urlparse(url)
        except Exception:
            raise TectonClientException(TectonErrorMessage.INVALID_URL)

    @staticmethod
    def validate_key(api_key: string) -> None:
        print("ENTERED VALIDATE_KEY")
        if not api_key:
            raise TectonClientException(TectonErrorMessage.INVALID_KEY)
