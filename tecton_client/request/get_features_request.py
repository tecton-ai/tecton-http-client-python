import json
from typing import Self, Optional

from tecton_client.request.abstract_get_features_request \
    import AbstractGetFeaturesRequest
from tecton_client.request.get_feature_request_data \
    import GetFeatureRequestData


class GetFeaturesRequest(AbstractGetFeaturesRequest):

    def __init__(self: Self, workspace_name: str, feature_service_name: str,
                 metadata_options: Optional[set] = None,
                 get_feature_request_data:
                 Optional[GetFeatureRequestData] = None) -> None:
        self._endpoint = "/api/v1/feature-service/get-features"
        super().__init__(self._endpoint, workspace_name,
                         feature_service_name, metadata_options)

        self.validate_request_parameters(get_feature_request_data)
        self._get_feature_request_data = \
            get_feature_request_data or GetFeatureRequestData()

    @property
    def endpoint(self: Self) -> str:
        return self._endpoint

    @property
    def get_feature_request_data(self: Self) -> GetFeatureRequestData:
        return self._get_feature_request_data

    @get_feature_request_data.setter
    def get_feature_request_data(self: Self,
                                 get_feature_request_data:
                                 GetFeatureRequestData) -> None:
        self._get_feature_request_data = get_feature_request_data

    def request_to_json(self: Self) -> str:
        final_metadata_options = {}
        if len(self.metadata_options) != 0:
            metadata_option_values = sorted([option.value for option in self.metadata_options])
            final_metadata_options = {value: True for value in metadata_option_values}

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
