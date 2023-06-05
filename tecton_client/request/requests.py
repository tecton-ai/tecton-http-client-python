import json
from abc import ABC
from typing import Self, Optional

from tecton_client.exceptions.exceptions import INVALID_WORKSPACE_NAME, \
    INVALID_FEATURE_SERVICE_NAME, TectonInvalidParameterException, \
    EMPTY_REQUEST_MAPS
from tecton_client.request.requests_data import DEFAULT_METADATA_OPTIONS, \
    GetFeatureRequestData


class AbstractTectonRequest(ABC):

    def __init__(self: Self, endpoint: str,
                 workspace_name: str,
                 feature_service_name: str) -> None:

        if not workspace_name:
            raise TectonInvalidParameterException(INVALID_WORKSPACE_NAME)
        if not feature_service_name:
            raise TectonInvalidParameterException(INVALID_FEATURE_SERVICE_NAME)

        self.endpoint = endpoint
        self.workspace_name = workspace_name
        self.feature_service_name = feature_service_name


class AbstractGetFeaturesRequest(AbstractTectonRequest):

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str,
                 metadata_options: set) -> None:

        super().__init__(endpoint, workspace_name, feature_service_name)
        if metadata_options is None or len(metadata_options) == 0:
            self.metadata_options = DEFAULT_METADATA_OPTIONS
        else:
            self.metadata_options = \
                self.get_metadata_options(metadata_options)

    @staticmethod
    def get_metadata_options(metadata_options: set) -> set:
        # union of two sets (add default options to their set)
        return metadata_options | DEFAULT_METADATA_OPTIONS

    @staticmethod
    def validate_request_data(
            get_features_request_data: GetFeatureRequestData) -> None:

        if len(get_features_request_data.request_context_map) == 0 and \
                len(get_features_request_data.join_key_map) == 0:
            raise TectonInvalidParameterException(EMPTY_REQUEST_MAPS)


class GetFeaturesRequest(AbstractGetFeaturesRequest):

    def __init__(self: Self, workspace_name: str, feature_service_name: str,
                 metadata_options: Optional[set] = None,
                 get_feature_request_data:
                 Optional[GetFeatureRequestData] = None) -> None:
        self.endpoint = "/api/v1/feature-service/get-features"
        super().__init__(self.endpoint, workspace_name,
                         feature_service_name, metadata_options)

        if len(get_feature_request_data.request_context_map) == 0 and \
                len(get_feature_request_data.join_key_map) == 0:
            raise TectonInvalidParameterException(EMPTY_REQUEST_MAPS)

        self.get_feature_request_data = get_feature_request_data

    def request_to_json(self: Self) -> str:
        final_metadata_options = {}
        if len(self.metadata_options) != 0:
            metadata_option_values = \
                sorted([option.value for option in self.metadata_options])
            final_metadata_options = \
                {value: True for value in metadata_option_values}

        request_value_dictionary = {
            "feature_service_name": self.feature_service_name,
            "join_key_map": self.get_feature_request_data.join_key_map,
            "metadata_options": final_metadata_options,
            "request_context_map":
                self.get_feature_request_data.request_context_map,
            "workspace_name": self.workspace_name
        }
        request = {"params": request_value_dictionary}

        json_request = json.dumps(request)

        return json_request
