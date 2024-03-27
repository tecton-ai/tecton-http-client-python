import json
from typing import Optional, Dict, Union
from urllib.parse import urljoin

import httpx

from tecton_client import TectonClient
from tecton_client._internal.data_types import GetFeaturesResponse, GetFeatureServiceMetadataResponse


class AsyncTectonClient(TectonClient):
    # redefining init just to make the type of 'client' explicit
    def __init__(self,
                 url: str,
                 api_key: str,
                 default_workspace_name: Optional[str] = None,
                 client: httpx.AsyncClient = None):
        """Constructor for the client

        Args:
            url: base url to your tecton cluster. Ex: http://explore.tecton.ai
            api_key: See https://docs.tecton.ai/docs/ for how to create an api key.
            default_workspace_name: The workspace from which the features will be retrieved.
                Can be over-ridden by individual function calls.
            client: An httpx.SyncClient, allowing you to provide finer-grained customization on the request behavior,
                such as default timeout or connection settings. See https://www.python-httpx.org/ for more info.
        """
        super().__init__(url=url, api_key=api_key, default_workspace_name=default_workspace_name, client=client)

    # overwriting to make async
    async def get_features(self,
                           *,
                           feature_service_name: Optional[str] = None,
                           feature_service_id: Optional[str] = None,
                           join_key_map: Optional[Dict[str, Union[int, str, type(None)]]] = None,
                           request_context_map: Optional[Dict[str, Union[int, str, float]]] = None,
                           metadata_options: Optional[Dict[str, bool]] = None,
                           workspace_name: Optional[str] = None,
                           request_options: Optional[Dict[str, bool]] = None,
                           allow_partial_results: bool = False,
                           ) -> GetFeaturesResponse:
        return super().get_features(feature_service_name=feature_service_name,
                                    feature_service_id=feature_service_id,
                                    join_key_map=join_key_map,
                                    request_context_map=request_context_map,
                                    metadata_options=metadata_options,
                                    workspace_name=workspace_name,
                                    request_options=request_options,
                                    allow_partial_results=allow_partial_results)

    # overwriting to make async
    async def get_feature_service_metadata(self, *,
                                           feature_service_name: Optional[str] = None,
                                           feature_service_id: Optional[str] = None,
                                           workspace_name: Optional[str] = None,
                                           ) -> GetFeatureServiceMetadataResponse:
        return super().get_feature_service_metadata(feature_service_name=feature_service_name,
                                                    feature_service_id=feature_service_id,
                                                    workspace_name=workspace_name)
