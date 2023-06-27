import asyncio

from tecton_client.http_client import TectonHttpClient
from tecton_client.requests import GetFeaturesRequest
from tecton_client.responses import GetFeaturesResponse


class TectonClient:
    """Class to represent a Tecton Client.

    A `TectonClient` object represents a client that can be used to interact with the Tecton FeatureService API.
    It provides a collection of methods to make HTTP requests to the respective API endpoints and returns the response
    as a Python object.

    Example:
        >>> tecton_client = TectonClient(url, api_key)

    """

    def __init__(self, url: str, api_key: str) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The Tecton Base URL
            api_key (str): API Key for authenticating with the FeatureService API. See `Authenticating with an API key
                <https://docs.tecton.ai/docs/reading-feature-data/reading-feature-data-for-inference/\
                reading-online-features-for-inference-using-the-http-api#creating-an-api-key-to-authenticate-to-\
                the-http-api>`_  for more information.
        """
        self._tecton_http_client = TectonHttpClient(url, api_key)
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
            BadRequestError: If the response returned from the Tecton Server is 400 Bad Request
                1. Error: Missing required join key: user_id
                This error indicates that the request is missing one or more of the required join keys for your feature
                service. To resolve this, ensure that the `join_key_map` in your request includes a union of all join
                keys across all feature views in the feature service.
                2. Error: Expected key not found: [xxx] in requestContextMap
                This error indicates that an expected key `xxx` was not found in the query's `requestContextMap` for an
                On Demand Feature View (ODFV). Please verify that the `requestContextMap` includes a union of all
                request context keys across all ODFVs in the feature service.
                3. Error: Unexpected JSON type for xxx in joinKeyMap. Expected int64 or null
                This error indicates that your feature service expects a join_key `xxx` of type int64 but your request
                contains a join_key `xxx` of type string. Please ensure that the join_key types in the request match the
                data types for the corresponding join keys in your feature view definitions.
                4. Error: Unknown join key: join_key, expected one of: [xxxx zzzz]
                This error indicates that your feature service expects join keys `xxx` and `zzzz` but received an
                unknown join key. Please ensure that the join keys in your request match the entity key strings
                defined for your feature views.
            UnauthorizedError: If the response returned from the Tecton Server is 401 Unauthorized
                1. Error: invalid 'Tecton-key' authorization header. Note that newly created credentials may take up to
                    60 seconds to be usable.
                Tecton does not recognize the API Key in your request. Please refer to the [API Key Documentation]
                (https://docs.tecton.ai/docs/beta/reading-feature-data/reading-feature-data-for-inference/\
                reading-online-features-for-inference-using-the-http-api#creating-an-api-key-to-authenticate-\
                to-the-http-api) for more information on how to create a Service Account with an API Key.
                Note that it may take up to 60 seconds for newly created API Keys to be accepted by the API.
            ForbiddenError: If the response returned from the Tecton Server is 403 Forbidden
                1. Error: Not Authorized. Note that access control changes may take up to 60 seconds to apply.
                The Service Account associated with your API Key does not have the necessary permissions to query the
                feature service. Note that the Service Account needs to have at least Consumer privileges for a
                workspace in order to query a feature service in that workspace. Please refer to the
                [Tecton Documentation](https://docs.tecton.ai/docs/beta/reading-feature-data/reading-feature-data-for-\
                inference/reading-online-features-for-inference-using-the-http-api#creating-an-api-key-to-\
                authenticate-to-the-http-api) for more information.
            NotFoundError: If the response returned from the Tecton Server is 404 Not Found
                1. Error: Unable to query FeatureService my_feature_service for workspace my_workspace.
                    Newly created feature services may take up to 60 seconds to query.
                    Also, ensure that the workspace is a live workspace.
                Please ensure that the workspace is a Live workspace and the feature service exists in the workspace.
                2. Error: Cannot retrieve features. FeatureView: xxx has no materialized data in the online store.
                Check that your materialization jobs have completed successfully so that the feature view has
                materialized data in the online store.
                3. Error: Cannot retrieve features. An input for FeatureView: xxx has no materialized data in the
                    online store.
                This error indicates that one or more of the Feature Views that the ODFV my_odfv depends on has no
                materialized data in the online store. Verify that all Feature Views that my_odfv depends on have online
                materialization enabled and have successfully completed materialization jobs.
                4. Error: The DynamoDB table was not found. If this package was newly created and is serving in multiple
                    regions, you may need to wait for DynamoDB Global Tables to finish creating a replica table.
                This error occurs when using DynamoDB as the Online Store and indicates that Tecton was unable to find
                or read from the DynamoDB Table for one or more Feature Views in the Feature Service. Please review any
                permission changes in DynamoDB Tables on your end and if this issue persists,
                please contact Tecton Support for assistance.
            ResourceExhaustedError: If the response returned from the Tecton Server is 429 Resources Exhausted
                1. Error: GetFeatures exceeded the concurrent request limit, please retry later
                This error indicates that you have exceeded the concurrent request limit for your deployment, and you
                need to either reduce your request rate or scale up your feature server deployment. Please refer to the
                Scaling Documentation for more information and contact Tecton support for further assistance
                2. Error: DynamoDB throttled the request. The request rate exceeds the AWS account's throughput limit.
                Tecton uses DynamoDB as the Online Store in OnDemand mode which autoscales the DynamoDB Tables based on
                workloads. This error indicates that the current rate of requests exceeds the allowed throughput for
                your AWS account and the DynamoDB Tables cannot be scaled further. Please reduce your request rate or
                contact AWS Support to request a limit increase for your account. For more information on Throughput
                Default Quotas please refer to the AWS Documentation
                3. Error: DynamoDB throttled the request. You may be requesting a hot key.
                In DynamoDB, each partition on a table can serve up to 3000 Read Request Units and this error indicates
                that the traffic to a partition exceeds this limit. Please reduce your request rate and retry your
                request.
            ServiceUnavailableError: If the response returned from the Tecton Server is 503 Service Unavailable
                1. Error: 503 Service Temporarily Unavailable
                This error indicates that Tecton is currently unable to process your request because either the Online
                Store is unavailable or Tecton's feature server deployment is down. Please check the Online Store
                availability on your end and if the error persists, please contact Tecton Support for assistance.
            GatewayTimeoutError: If the response returned from the Tecton Server is 504 Gateway Timeout
                1. Error: Timed out
                This error indicates that processing the request exceeded the 2 seconds timeout limit set by Tecton,
                because either the Online Store did not respond or Tecton was unable to process the response within the
                time limit. Please verify the availability of your Online Store, and if the problem persists, please
                contact Tecton Support for assistance.

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
