from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin

import httpx
from httpx import HTTPStatusError

from tecton_client._internal.data_types import (
    GetFeatureServiceMetadataResponse,
    GetFeaturesResponse,
    MetadataOptions,
    RequestOptions,
)
from tecton_client._internal.utils import (
    build_get_feature_service_metadata_request,
    build_get_features_request,
    get_default_headers,
    validate_request_args,
)
from tecton_client.exceptions import convert_exception


class TectonClient:
    """A lightweight http client for fetching features from Tecton in production applications.

    For feature development and interacting with the rest of your Tecton deployment, use the Tecton SDK
    (`pip install tecton`) and not this client library.

    Examples:

    .. code-block:: python

        client = TectonClient(
            url="http://explore.tecton.ai", api_key="my_key", default_workspace_name="prod"
        )
        client.get_features(
            feature_service_name="fraud_detection_feature_service:v2",
            join_key_map={"user_id": "user_4407104885"},
            request_context_map={"amount": 500.00},
            metadata_options=MetadataOptions(include_data_types=True),
        )

    """

    def __init__(
        self, url: str, api_key: str, default_workspace_name: Optional[str] = None, client: httpx.Client = None
    ):
        """Constructor for the client.

        Args:
            url: Base url for your tecton cluster. Ex: http://explore.tecton.ai
            api_key: A Tecton API key that has read-access to the workspace(s) that this client will be retrieving from.
                See https://docs.tecton.ai/docs/ for how to create an api key.
            default_workspace_name: The workspace from which the features will be retrieved. Can be overridden by
                individual calls to `get_features`.
            client: An httpx.Client, allowing you to provide finer-grained customization on the request behavior,
                such as default timeout or connection settings. See https://www.python-httpx.org/ for more info.
        """
        self.url = url
        self.default_workspace_name = default_workspace_name
        self._api_key = api_key
        self._base_url = urljoin(url, "/api/v1/")
        self._paths = {
            "get_features": urljoin(self._base_url, "feature-service/get-features"),
            "get_feature_service_metadata": urljoin(self._base_url, "feature-service/metadata"),
        }

        headers = get_default_headers(api_key)
        if client is None:
            self._client = httpx.Client(headers=headers)
        else:
            self._client = client
            # add the headers to the existing client headers
            self._client.headers.update(headers)

    def get_features(
        self,
        *,
        feature_service_name: str,
        join_key_map: Optional[Dict[str, Union[int, str, type(None)]]] = None,
        request_context_map: Optional[Dict[str, Any]] = None,
        metadata_options: Optional[MetadataOptions] = None,
        workspace_name: Optional[str] = None,
        request_options: Optional[RequestOptions] = None,
        allow_partial_results: bool = False,
    ) -> GetFeaturesResponse:
        """Get feature values from a Feature Service.

        Args:
            feature_service_name: The name of the Feature Service to fetch features from.
            join_key_map: The join keys (i.e. primary keys) for this Feature Service request.
            request_context_map: The request context map (i.e. request data) for this Feature Service.
            metadata_options: Options for including additional metadata as part of response. Useful for debugging, but
                may affect performance.
            workspace_name: Workspace name where feature_service is deployed. Overrides AsyncTectonClient.default_workspace_name.
            request_options: (Advanced) Request level options to control feature server caching behavior.
            allow_partial_results: (Advanced) Whether incomplete results should be returned when the Online
                Feature Store size limit has been exceeded for this request. If this is not true, then the response
                will be an error in this case. This is an advanced option and should only be set after consulting with
                the Tecton team.

        """
        validate_request_args(feature_service_name, workspace_name, self.default_workspace_name)
        if not workspace_name:
            workspace_name = self.default_workspace_name
        request_data = build_get_features_request(
            feature_service_name=feature_service_name,
            join_key_map=join_key_map,
            request_context_map=request_context_map,
            workspace_name=workspace_name,
            metadata_options=metadata_options,
            allow_partial_results=allow_partial_results,
            request_options=request_options,
        )
        resp = self._client.post(self._paths["get_features"], json=request_data)

        try:
            resp.raise_for_status()
        except HTTPStatusError as exc:
            raise convert_exception(exc) from exc

        return GetFeaturesResponse.from_response(resp.json())

    def get_feature_service_metadata(
        self,
        *,
        feature_service_name: str,
        workspace_name: Optional[str] = None,
    ) -> GetFeatureServiceMetadataResponse:
        """Get metadata about a Feature Service.

        Args:
            feature_service_name: The name of the Feature Service to fetch metadata of.
            workspace_name: Workspace name where feature_service is deployed. Overrides AsyncTectonClient.default_workspace_name.
        """
        validate_request_args(feature_service_name, workspace_name, self.default_workspace_name)
        if not workspace_name:
            workspace_name = self.default_workspace_name
        request_data = build_get_feature_service_metadata_request(
            feature_service_name=feature_service_name,
            workspace_name=workspace_name,
        )
        resp = self._client.post(url=self._paths["get_feature_service_metadata"], json=request_data)
        try:
            resp.raise_for_status()
        except HTTPStatusError as exc:
            raise convert_exception(exc) from exc

        return GetFeatureServiceMetadataResponse.from_response(resp.json())
