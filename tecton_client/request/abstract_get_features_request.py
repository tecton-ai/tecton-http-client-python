from typing import Self

from tecton_client.exceptions.exceptions import TectonClientException, \
    EMPTY_REQUEST_MAPS
from tecton_client.request.abstract_tecton_request import AbstractTectonRequest
from tecton_client.request.get_feature_request_data import \
    GetFeatureRequestData


class AbstractGetFeaturesRequest(AbstractTectonRequest):

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str) -> None:
        super().__init__(endpoint, workspace_name, feature_service_name)

    @staticmethod
    def validate_request_parameters(
            get_features_request_data: GetFeatureRequestData) -> None:

        if get_features_request_data.is_empty_request_context_map() and \
                get_features_request_data.is_empty_join_key_map():
            raise TectonClientException(EMPTY_REQUEST_MAPS)
