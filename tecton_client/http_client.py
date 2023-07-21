import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
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
class HTTPRequest:
    """Represents an HTTP request object that holds the parameters to make a request to the HTTP API.

    Attributes:
        endpoint (str): The HTTP endpoint to attach to the URL and query.
        request_body (dict): The request data to be passed, in JSON format.

    """

    endpoint: str
    request_body: dict


@dataclass
class HTTPResponse:
    """Represents an HTTP response object to capture the result of making an HTTP request.

    Attributes:
        result (dict): The result of the HTTP request.
        latency (timedelta): The latency of the HTTP request.

    """

    result: dict
    latency: timedelta


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

    async def execute_request(self, request: HTTPRequest) -> HTTPResponse:
        """Performs an HTTP request to a specified endpoint using the client.

        This method sends an HTTP POST request to the specified endpoint, attaching the provided request body data.

        Args:
            request (HTTPRequest): An :class:`HTTPRequest` object containing the endpoint and body of the HTTP request.

        Returns:
            HTTPResponse: An :class:`HTTPResponse` object containing the result and the latency of the HTTP request.

        Raises:
            TectonServerException: If the server returns an error response, different errors based on the
                error response are raised.
            TectonClientError: If the client encounters an error while making the request.

        """
        url = urljoin(self._url, request.endpoint)

        try:
            start_time = time.time()
            async with self._client.post(url, json=request.request_body, headers=self._auth) as response:
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

    async def execute_parallel_requests(
        self,
        endpoint: str,
        request_bodies: List[dict],
        timeout: Optional[timedelta] = None,
    ) -> Tuple[List[Optional[HTTPResponse]], timedelta]:
        """Performs multiple HTTP requests to a specified endpoint in parallel using the client.

        Args:
            endpoint (str): The HTTP endpoint to attach to the URL and query.
            request_bodies (List[dict]): The list of request data to be passed for the parallel requests,
                in JSON format.
            timeout (Optional[timedelta]): The duration of time to wait for the parallel requests to complete before
                returning. Defaults to no timeout.

        Returns:
            Tuple[List[Optional[HTTPResponse]], timedelta]: A tuple of the list of :class:`HTTPResponse` objects,
                or None if the request was unsuccessful, and the overall time taken to execute the parallel requests
                returned as a :class:`timedelta` object.

        """
        # Create a list of tasks to execute the requests in parallel
        tasks = [
            asyncio.create_task(self.execute_request(HTTPRequest(endpoint=endpoint, request_body=request_body)))
            for request_body in request_bodies
        ]

        # Execute the tasks in parallel and wait for them to complete or timeout
        start_time = time.time()
        done, pending = await asyncio.wait(tasks, timeout=timeout.total_seconds() if timeout else timeout)
        end_time = time.time()

        # Calculate the latency of the request
        latency = timedelta(seconds=(end_time - start_time))

        # Capture results of the tasks:-
        # If the task is in the done list, it either completes successfully or returns an exception from the server.
        # If the task is successful, i.e. without an exception, return the result.
        # Else, store None in case the task returned an exception or timed out.
        results = [task.result() if task in done and not task.exception() else None for task in tasks]

        # Get the list of exceptions thrown by the HTTP client
        thrown_exceptions = [task.exception() for task in done if task.exception()]

        # Close all the created tasks
        await self._close_tasks(tasks=pending)

        # If there are any exceptions thrown by the HTTP client, raise the first exception
        if thrown_exceptions:
            raise thrown_exceptions[0]

        return results, latency

    @staticmethod
    async def _close_tasks(tasks: Set[asyncio.Task]) -> None:
        """Closes a set of tasks.

        Args:
            tasks (Set[asyncio.Task]): The set of tasks to be closed.

        """
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

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
