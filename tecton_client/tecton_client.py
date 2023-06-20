from typing import Self

from tecton_client.http_client import TectonHttpClient
from tecton_client.requests import GetFeaturesRequest
from tecton_client.responses import GetFeaturesResponse


class TectonClient:
    """Class to represent a Tecton Client.

    A `TectonClient` object represents a client that can be used to make requests to a Tecton Feature Service.

    Usage:
        tecton_client = TectonClient(url, api_key)

    Attributes:
        _tectonHttpClient (TectonHttpClient): The TectonHttpClient object, which is used to make HTTP requests.
    """

    def __init__(self: Self, url: str, api_key: str) -> None:
        """Initialize the parameters required to make HTTP requests.

        Args:
            url (str): The URL to ping.
            api_key (str): The API Key required as part of header authorization.
        """
        self._tectonHttpClient = TectonHttpClient(url, api_key)

    async def get_features(self: Self, get_features_request: GetFeaturesRequest) -> GetFeaturesResponse:
        """Get features from the Tecton Feature Service.

        Args:
            get_features_request (GetFeaturesRequest): The GetFeaturesRequest object, with request data

        Returns:
            GetFeaturesResponse: The GetFeaturesResponse object, with response data

        Usage:
            join_key_map = {"example_join_key": "example_join_value"}
            request_context_map = {"example_request_context": "example_string_value"}
            get_feature_request_data = GetFeatureRequestData(join_key_map, request_context_map)
            get_features_request = GetFeaturesRequest(
                feature_service_name="example_feature_service",
                request_data=get_feature_request_data,
                workspace_name="example_workspace",
            )
            get_features_response = await tecton_client.get_features(get_features_request)
            print(get_features_response.get_feature_values_dict())
        """
        response = await self._tectonHttpClient.execute_request(
            get_features_request.ENDPOINT, get_features_request.to_json()
        )
        return GetFeaturesResponse(response)

    async def close(self: Self) -> None:
        """Close the HTTPX Asynchronous Client."""
        await self._tectonHttpClient.close()
