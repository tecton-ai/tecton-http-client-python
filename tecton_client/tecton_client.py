import os
from typing import Optional

import aiohttp

from tecton_client.client_options import TectonClientOptions
from tecton_client.exceptions import InvalidParameterError
from tecton_client.exceptions import InvalidParameterMessage
from tecton_client.http_client import HTTPRequest
from tecton_client.http_client import TectonHttpClient
from tecton_client.requests import GetFeaturesBatchRequest
from tecton_client.requests import GetFeatureServiceMetadataRequest
from tecton_client.requests import GetFeaturesRequest
from tecton_client.responses import GetFeaturesBatchResponse
from tecton_client.responses import GetFeatureServiceMetadataResponse
from tecton_client.responses import GetFeaturesResponse
from tecton_client.utils import asyncio_run


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
        client: Optional[aiohttp.ClientSession] = None,
        client_options: Optional[TectonClientOptions] = None,
    ) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The Tecton Base URL
            api_key (Optional[str]): API Key for authenticating with the FeatureService API. See `Authenticating with an
                API key <https://docs.tecton.ai/docs/reading-feature-data/reading-feature-data-for-inference/\
                reading-online-features-for-inference-using-the-http-api#creating-an-api-key-to-authenticate-to-\
                the-http-api>`_  for more information.
                This parameter is optional and can be provided through an environment variable called `TECTON_API_KEY`.
            client (Optional[aiohttp.ClientSession]): (Optional) The HTTP Asynchronous Client.
                Users can initialize their own HTTP client and pass it in to the :class:`TectonClient` object.
                If no client is passed in, the :class:`TectonClient` object will initialize its own HTTP client.
            client_options (Optional[TectonClientOptions]): (Optional) The HTTP client options to be passed in when the
                :class:`TectonClient` object initializes its own HTTP client.
                If no options are passed in, defaults defined in :class:`TectonClientOptions()` will be used.

        """
        api_key = os.getenv("TECTON_API_KEY") if api_key is None else api_key
        if api_key is None:
            raise InvalidParameterError(InvalidParameterMessage.KEY.value)

        if client and client_options:
            message = "Cannot provide both `client` and `client_options` parameters."
            raise InvalidParameterError(message)

        if not client_options:
            client_options = TectonClientOptions()

        self._tecton_http_client = TectonHttpClient(
            url,
            api_key,
            client=client,
            client_options=client_options,
        )

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
            >>> request_data = GetFeaturesRequestData(join_key_map, request_context_map)
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
            ResourceExhaustedError: If the response returned from the Tecton Server is 429 Resources Exhausted. Some
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
        http_request = HTTPRequest(endpoint=request.ENDPOINT, request_body=request.to_json())
        http_response = asyncio_run(self._tecton_http_client.execute_request(request=http_request))
        return GetFeaturesResponse(http_response=http_response)

    def get_features_batch(self, request: GetFeaturesBatchRequest) -> GetFeaturesBatchResponse:
        """Makes a batch request to retrieve a list of feature vectors and metadata for a given workspace
        and feature service, and returns the response in the form of a :class:`GetFeaturesBatchResponse` object.

        Args:
            request (GetFeaturesBatchRequest): The :class:`GetFeaturesBatchRequest` object with the request parameters.

        Returns:
            GetFeaturesBatchResponse: The :class:`GetFeaturesBatchResponse` object representing the response from the
                HTTP API, with the list of feature vectors and metadata (if requested).

        Example:
            >>> tecton_client = TectonClient(url, api_key)
            >>> join_key_map = {"example_join_key": "example_join_value"}
            >>> request_context_map_1 = {"example_request_context1": "example_string_value1"}
            >>> request_context_map_2 = {"example_request_context2": "example_string_value2"}
            >>> request_data_1 = GetFeaturesRequestData(join_key_map, request_context_map_1)
            >>> request_data_2 = GetFeaturesRequestData(join_key_map, request_context_map_2)
            >>> batch_request = GetFeaturesBatchRequest(
            ...     feature_service_name="example_feature_service",
            ...     request_data_list=[request_data_1, request_data_2],
            ...     workspace_name="example_workspace",
            ...     micro_batch_size=1
            ... )
            >>> batch_response = tecton_client.get_features_batch(batch_request)
            `batch_response.response_list` returns a list of :class:`GetFeaturesResponse` objects representing a
            response for each request in the :class:`GetFeaturesBatchRequest` object.
            Each :class:`GetFeaturesResponse` object contains a dictionary of {feature_name: `FeatureValue`} pairs,
            which can be accessed using:
            >>> for response in batch_response.batch_response_list:
            >>>     print([feature.feature_value for feature in response.feature_values.values()])

        Raises:
            TectonClientError: If the client encounters an error while making the request.
            BadRequestError: If the response returned from the Tecton Server is 400 Bad Request. Please
                check the exception message for detailed information.
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
            ResourceExhaustedError: If the response returned from the Tecton Server is 429 Resources Exhausted. Please
                check the exception message for detailed information.
            ServiceUnavailableError: If the response returned from the Tecton Server is 503 Service Unavailable, it
                could be because Tecton is currently unable to process your request. Please retry later.
            GatewayTimeoutError: If the response returned from the Tecton Server is 504 Gateway Timeout, it indicates
                that processing the request exceeded the 2 seconds timeout limit set by Tecton.

                For more detailed information on the errors, please refer to the error responses `here
                <https://docs.tecton.ai/http-api#operation/GetFeaturesBatch>`_.

        """
        results, latency = asyncio_run(
            self._tecton_http_client.execute_parallel_requests(
                endpoint=request.ENDPOINT, request_bodies=request.to_json_list(), timeout=request.timeout
            )
        )
        return GetFeaturesBatchResponse(
            responses_list=results, request_latency=latency, micro_batch_size=request.micro_batch_size
        )

    def get_feature_service_metadata(
        self, request: GetFeatureServiceMetadataRequest
    ) -> GetFeatureServiceMetadataResponse:
        """Makes a request to the /metadata endpoint and returns the response in the form of a
        :class:`GetFeatureServiceMetadataResponse` object.

        Args:
            request (GetFeatureServiceMetadataRequest): The :class:`GetFeatureServiceMetadataRequest` object
                with the request parameters.

        Returns:
            GetFeatureServiceMetadataResponse: The :class:`GetFeatureServiceMetadataResponse` object representing the
                response from the HTTP API with the requested information.

        Example:
            >>> tecton_client = TectonClient(url, api_key)
            >>> metadata_request = GetFeatureServiceMetadataRequest(
            ...     feature_service_name="example_feature_service",
            ...     workspace_name="example_workspace",
            ... )
            >>> metadata_response = tecton_client.get_feature_service_metadata(metadata_request)
            The `metadata_response` object contains the metadata information requested as different
            fields of the object.

            >>> print(metadata_response.feature_values)
            >>> print(metadata_response.input_join_keys)

        Raises:
            TectonClientError: If the client encounters an error while making the request.
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
            ResourcesExhaustedError: If the response returned from the Tecton Server is 429 Resources Exhausted. Please
                check the exception message for detailed information.
            ServiceUnavailableError: If the response returned from the Tecton Server is 503 Service Unavailable, it
                could be because Tecton is currently unable to process your request. Please retry later.
            GatewayTimeoutError: If the response returned from the Tecton Server is 504 Gateway Timeout, it indicates
                that processing the request exceeded the 2 seconds timeout limit set by Tecton.

                For more detailed information on the errors, please refer to the error responses `here
                <https://docs.tecton.ai/http-api#operation/GetFeaturesBatch>`_.

        """
        http_request = HTTPRequest(endpoint=request.ENDPOINT, request_body=request.to_json())
        http_response = asyncio_run(self._tecton_http_client.execute_request(request=http_request))
        return GetFeatureServiceMetadataResponse(http_response=http_response)

    @property
    def is_closed(self) -> bool:
        """Returns True if the client has been closed, False otherwise."""
        return self._tecton_http_client.is_closed

    def close(self) -> None:
        """Closes the client, releasing allocated resources and allowing connection reuse.

        Important for proper resource management and graceful termination of the client.

        """
        asyncio_run(self._tecton_http_client.close())
