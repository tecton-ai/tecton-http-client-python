import asyncio
import os
from dataclasses import dataclass
from typing import Optional

import httpx

from tecton_client.http_client import TectonHttpClient
from tecton_client.requests import GetFeaturesRequest
from tecton_client.responses import GetFeaturesResponse


@dataclass
class TectonClientOptions:
    """Class to represent the Tecton Client options.

    Examples:
        >>> options = TectonClientOptions(connect_timeout=10, read_timeout=10, keepalive_expiry=600)
        >>> tecton_client = TectonClient(url, api_key, client_options=options)

    Attributes:
        connect_timeout (float): (Optional) The maximum amount of time to wait until a socket connection to the
            requested host is established. Defaults to 2.0 seconds.
        read_timeout (float): (Optional) The maximum duration to wait for a chunk of data to be received (for example,
            a chunk of the response body). Defaults to 2.0 seconds.
        keepalive_expiry (Optional[int]): (Optional) The time limit on idle keep-alive connections in seconds,
            or None for no limits. Defaults to 300 seconds (5 minutes).
        max_connections (Optional[int]): (Optional) The maximum number of allowable connections, or None for no limits.
            Defaults to 10.

    """

    connect_timeout: float = 2.0
    read_timeout: float = 2.0
    keepalive_expiry: Optional[int] = 300
    max_connections: Optional[int] = 10


class TectonClient:
    """Class to represent a Tecton Client.

    A `TectonClient` object represents a client that can be used to interact with the Tecton FeatureService API.
    It provides a collection of methods to make HTTP requests to the respective API endpoints and returns the response
    as a Python object.

    Example:
        >>> tecton_client = TectonClient(url, api_key)

    """

    def __init__(
        self,
        url: str,
        api_key: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
        client_options: Optional[TectonClientOptions] = None,
    ) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The Tecton Base URL
            api_key (Optional[str]): API Key for authenticating with the FeatureService API. See `Authenticating with an
                API key <https://docs.tecton.ai/docs/reading-feature-data/reading-feature-data-for-inference/\
                reading-online-features-for-inference-using-the-http-api#creating-an-api-key-to-authenticate-to-\
                the-http-api>`_  for more information.
                It can be passed in as a parameter or provided through an environment variable called `TECTON_API_KEY`.
            client (Optional[httpx.AsyncClient]): (Optional) The HTTP Asynchronous Client.
                Users can initialize their own HTTP client and pass it in to the :class:`TectonClient` object.
                If no client is passed in, the :class:`TectonClient` object will initialize its own HTTP client.
            client_options (Optional[TectonClientOptions]): (Optional) The HTTP client options.
                If no options are passed in, defaults defined in :class:`TectonClientOptions()` will be used.

        """
        if api_key is None:
            api_key = os.getenv("TECTON_API_KEY")

        if client_options is None:
            client_options = TectonClientOptions()

        self._tecton_http_client = TectonHttpClient(
            url,
            api_key,
            read_timeout=client_options.read_timeout,
            connect_timeout=client_options.connect_timeout,
            keepalive_expiry=client_options.keepalive_expiry,
            max_connections=client_options.max_connections,
            client=client,
        )
        self._loop = asyncio.new_event_loop()

    def get_features(self, request: GetFeaturesRequest) -> GetFeaturesResponse:
        """Makes a request to the /get-features endpoint and returns the response in the form of a
        :class:`GetFeaturesResponse` object

        Args:
            request (GetFeaturesRequest): The :class:`GetFeaturesRequest` object with the request parameters

        Returns:
            GetFeaturesResponse: The :class:`GetFeaturesResponse` object representing the response from the HTTP API

        Example:
            >>> tecton_client = TectonClient(url, api_key)
            >>> join_key_map = {"example_join_key": "example_join_value"}
            >>> request_context_map = {"example_request_context": "example_string_value"}
            >>> request_data = GetFeatureRequestData(join_key_map, request_context_map)
            >>> request = GetFeaturesRequest(
            ...     feature_service_name="example_feature_service",
            ...     request_data=request_data,
            ...     workspace_name="example_workspace",
            ... )
            >>> response = tecton_client.get_features(request)
            `response.feature_values()` returns a dictionary of {feature_name: `FeatureValue`} pairs.

            To access the values of the features stored as :class:`FeatureValue` objects in the dictionary, use:
            >>> print([feature.feature_value for feature in response.feature_values.values()])
            [1, 2, 3, "test_feature", ["test", "array"]]

        Raises:
            BadRequestError: If the response returned from the Tecton Server is 400 Bad Request. Some of the possible
                reasons for this are:
                1. Missing required join key in the :class:`GetFeaturesRequestData` object passed in the request
                2. An expected key is not found in the requestContextMap of the :class:`GetFeaturesRequestData` object
                3. Unexpected type for a key in the joinKeyMap passed in the :class:`GetFeaturesRequestData` object
                4. Unknown join key passed in the :class:`GetFeaturesRequestData` object in the request
            UnauthorizedError: If the response returned from the Tecton Server is 401 Unauthorized, it could be because
                Tecton does not recognize the API Key in your request. Please refer to the `API Key Documentation
                <https://docs.tecton.ai/docs/beta/reading-feature-data/reading-feature-data-for-inference/\
                reading-online-features-for-inference-using-the-http-api#creating-an-api-key-to-authenticate-\
                to-the-http-api>`_ for more information on how to create a Service Account with an API Key
            ForbiddenError: If the response returned from the Tecton Server is 403 Forbidden, it could be because the
                Service Account associated with your API Key does not have the necessary permissions to query
                the feature service. Please refer to the `Tecton Documentation <https://docs.tecton.ai/docs/beta/\
                reading-feature-data/reading-feature-data-for-inference/reading-online-features-for-inference-using-\
                the-http-api#creating-an-api-key-to-authenticate-to-the-http-api>`_ for more information.
            NotFoundError: If the response returned from the Tecton Server is 404 Not Found. Please check the exception
                message for detailed information.
            ResourcesExhaustedError: If the response returned from the Tecton Server is 429 Resources Exhausted. Some
                of the possible reasons for the error are:
                1. GetFeatures exceeded the concurrent request limit, please retry later
                2. DynamoDB throttled the request. The request rate exceeds the AWS account's throughput limit, or
                    you may be requesting a hot key
            ServiceUnavailableError: If the response returned from the Tecton Server is 503 Service Unavailable, it
                could be because Tecton is currently unable to process your request. Please retry later.
            GatewayTimeoutError: If the response returned from the Tecton Server is 504 Gateway Timeout, it indicates
                that processing the request exceeded the 2 seconds timeout limit set by Tecton.

                For more detailed information on the errors, please refer to the error responses `here
                <https://docs.tecton.ai/http-api#operation/GetFeatures>`_.

        """
        response = self._loop.run_until_complete(
            self._tecton_http_client.execute_request(request.ENDPOINT, request.to_json())
        )
        return GetFeaturesResponse(response)

    @property
    def is_closed(self) -> bool:
        """Returns True if the client has been closed, False otherwise."""
        return self._tecton_http_client.is_closed

    def close(self) -> None:
        """Closes the client, releasing allocated resources and allowing connection reuse.

        Important for proper resource management and graceful termination of the client.

        """
        self._loop.run_until_complete(self._tecton_http_client.close())
        self._loop.close()
