from typing import Self

from tecton_client.exceptions.exceptions import TectonClientException, \
    EMPTY_REQUEST_MAPS
from tecton_client.model.metadata_option import MetadataOptions
from tecton_client.request.abstract_tecton_request import AbstractTectonRequest
from tecton_client.request.get_feature_request_data import \
    GetFeatureRequestData
from tecton_client.request.request_constants import DEFAULT_METADATA_OPTIONS, \
    ALL_METADATA_OPTIONS


class AbstractGetFeaturesRequest(AbstractTectonRequest):

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str,
                 metadata_options: set) -> None:

        super().__init__(endpoint, workspace_name, feature_service_name)
        if metadata_options is None or len(metadata_options) == 0:
            self._metadata_options = DEFAULT_METADATA_OPTIONS
        else:
            self._metadata_options = \
                self.get_metadata_options(metadata_options)

    @property
    def metadata_options(self: Self) -> set:
        return self._metadata_options

    @staticmethod
    def get_metadata_options(metadata_options: set) -> set:
        if MetadataOptions.ALL in metadata_options:
            return ALL_METADATA_OPTIONS
        elif MetadataOptions.ALL in metadata_options:
            return DEFAULT_METADATA_OPTIONS
        else:
            return metadata_options | DEFAULT_METADATA_OPTIONS

    @staticmethod
    def validate_request_parameters(
            get_features_request_data: GetFeatureRequestData) -> None:

        if get_features_request_data.is_empty_request_context_map() and \
                get_features_request_data.is_empty_join_key_map():
            raise TectonClientException(EMPTY_REQUEST_MAPS)
