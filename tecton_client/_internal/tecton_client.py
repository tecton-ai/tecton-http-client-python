import json
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union
from urllib.parse import urljoin

import httpx
from httpx import HTTPStatusError

from tecton_client.__about__ import __version__ as tecton_version
from tecton_client._internal.data_types import GetFeatureServiceMetadataResponse
from tecton_client._internal.data_types import GetFeaturesResponse
from tecton_client._internal.data_types import MetadataOptions
from tecton_client._internal.data_types import RequestOptions
from tecton_client.exceptions import convert_exception


class TectonClient:
    """A lightweight http client for interacting with features in Tecton. For the full sdk, use tecton-sdk"""

    def __init__(
        self, url: str, api_key: str, default_workspace_name: Optional[str] = None, client: httpx.Client = None
    ):
        """Constructor for the client

        Args:
            url: base url to your tecton cluster. Ex: http://explore.tecton.ai
            api_key: See https://docs.tecton.ai/docs/ for how to create an api key.
            default_workspace_name: The workspace from which the features will be retrieved.
                Can be over-ridden by individual function calls.
            client: An httpx.Client, allowing you to provide finer-grained customization on the request behavior,
                such as default timeout or connection settings. See https://www.python-httpx.org/ for more info.
        """
        self.url = url
        self.default_workspace_name = default_workspace_name
        self._api_key = api_key
        self._base_url = urljoin(url, "/api/v1/")

        headers = httpx.Headers(
            {"Authorization": "Tecton-key " + api_key, "User-Agent": "tecton-http-python-client " + tecton_version}
        )
        if client is None:
            self._client = httpx.Client(headers=headers)
        else:
            self._client = client
            # add the headers to the existing client headers
            self._client.headers.update(headers)

    def get_features(
        self,
        *,
        feature_service_name: Optional[str] = None,
        feature_service_id: Optional[str] = None,
        join_key_map: Optional[Dict[str, Union[int, str, type(None)]]] = None,
        request_context_map: Optional[Dict[str, Any]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        workspace_name: Optional[str] = None,
        request_options: Optional[Dict[str, bool]] = None,
        allow_partial_results: bool = False,
    ) -> GetFeaturesResponse:
        self._validate(feature_service_id, feature_service_name, workspace_name)
        request_data = self._build_get_features_request(
            feature_service_id,
            feature_service_name,
            join_key_map,
            request_context_map,
            workspace_name,
            metadata_options,
            allow_partial_results,
            request_options,
        )
        resp = self._client.post(url=urljoin(self._base_url, "feature-service/get-features"), json=request_data)

        try:
            resp.raise_for_status()
        except HTTPStatusError as exc:
            raise convert_exception(exc) from exc

        return GetFeaturesResponse.from_dict(json.loads(resp.text))

    def get_feature_service_metadata(
        self,
        *,
        feature_service_name: Optional[str] = None,
        feature_service_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
    ) -> GetFeatureServiceMetadataResponse:
        self._validate(feature_service_id, feature_service_name, workspace_name)
        request_data = self._build_get_feature_service_metadata_request(
            feature_service_id=feature_service_id,
            feature_service_name=feature_service_name,
            workspace_name=workspace_name,
        )
        resp = self._client.post(url=urljoin(self._base_url, "feature-service/metadata"), json=request_data)
        try:
            resp.raise_for_status()
        except HTTPStatusError as exc:
            raise convert_exception(exc) from exc

        return GetFeatureServiceMetadataResponse.from_dict(json.loads(resp.text))

    def _build_get_features_request(
        self,
        feature_service_id,
        feature_service_name,
        join_key_map,
        request_context_map,
        workspace_name,
        metadata_options,
        allow_partial_results,
        request_options,
    ):
        workspace_name = workspace_name or self.default_workspace_name
        params = {
            "workspaceName": workspace_name,
            "featureServiceName": feature_service_name,
            "featureServiceId": feature_service_id,
            "joinKeyMap": join_key_map or {},
            "requestContextMap": request_context_map or {},
            "allowPartialResults": allow_partial_results,
        }
        if metadata_options:
            params["metadataOptions"] = {
                # these two default to True
                "includeNames": metadata_options.get(MetadataOptions.include_names, True),
                "includeDataTypes": metadata_options.get(MetadataOptions.include_data_types, True),
                # the rest default to False
                "includeEffectiveTimes": metadata_options.get(MetadataOptions.include_effective_times, False),
                "includeSloInfo": metadata_options.get(MetadataOptions.include_slo_info, False),
                "includeServingStatus": metadata_options.get(MetadataOptions.include_serving_status, False),
            }
        if request_options:
            params["requestOptions"] = {
                "readFromCache": request_options.get(RequestOptions.read_from_cache),
                "writeToCache": request_options.get(RequestOptions.write_to_cache),
            }
        request_data = {"params": params}
        return request_data

    def _build_get_feature_service_metadata_request(self, feature_service_id, feature_service_name, workspace_name):
        return {
            "params": {
                "featureServiceName": feature_service_name,
                "featureServiceId": feature_service_id,
                "workspaceName": workspace_name,
            }
        }

    def _validate(self, feature_service_id, feature_service_name, workspace_name):
        workspace_name = workspace_name or self.default_workspace_name
        if not workspace_name and not self.default_workspace_name:
            msg = (
                "workspace_name not set. Parameter workspace_name must be set either in TectonClient "
                "initialization or in get_features"
            )
            raise ValueError(msg)
        if not (feature_service_id or feature_service_name) or (feature_service_id and feature_service_name):
            msg = "must pass exactly one of feature_service_name or feature_service_id"
            raise ValueError(msg)
