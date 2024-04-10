from typing import Any, Dict, Optional, Union

import httpx

from tecton_client.__about__ import __version__ as tecton_version
from tecton_client._internal.data_types import MetadataOptions, RequestOptions


def get_default_headers(api_key):
    return httpx.Headers(
        {"Authorization": "Tecton-key " + api_key, "User-Agent": "tecton-http-python-client " + tecton_version}
    )


def build_get_features_request(
    feature_service_name: str,
    join_key_map: Optional[Dict[str, Optional[Union[int, str]]]] = None,
    request_context_map: Optional[Dict[str, Any]] = None,
    metadata_options: Optional[MetadataOptions] = None,
    workspace_name: Optional[str] = None,
    request_options: Optional[Dict[str, bool]] = None,
    allow_partial_results: bool = False,
):
    params = {
        "workspaceName": workspace_name,
        "featureServiceName": feature_service_name,
        "joinKeyMap": join_key_map or {},
        "requestContextMap": request_context_map or {},
        "allowPartialResults": allow_partial_results,
    }
    if not metadata_options:
        # creates metadata options with defaults
        metadata_options = MetadataOptions()
    params["metadataOptions"] = metadata_options.to_request()
    if not request_options:
        # creates request options with defaults
        request_options = RequestOptions()
    params["requestOptions"] = request_options.to_request()
    request_data = {"params": params}
    return request_data


def build_get_feature_service_metadata_request(
    feature_service_name: Optional[str] = None,
    workspace_name: Optional[str] = None,
):
    return {
        "params": {
            "featureServiceName": feature_service_name,
            "workspaceName": workspace_name,
        }
    }


def validate_request_args(
    feature_service_name: Optional[str] = None,
    workspace_name: Optional[str] = None,
    default_workspace_name: Optional[str] = None,
):
    if not workspace_name and not default_workspace_name:
        msg = (
            "workspace_name not set. Parameter workspace_name must be set either in TectonClient "
            "initialization or in get_features"
        )
        raise ValueError(msg)
    if not feature_service_name:
        msg = "feature_service_name must be provided"
        raise ValueError(msg)
