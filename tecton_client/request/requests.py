import json
from abc import ABC
from typing import Self, Optional, Final, Set

from tecton_client.exceptions.exceptions import (
    InvalidParameterMessages,
    InvalidParameterException
)
from tecton_client.request.requests_data import (
    MetadataOptions,
    GetFeatureRequestData, GetFeaturesRequestJSON
)


class TectonRequest(ABC):
    """
    Abstract base class for all Tecton related requests
    """

    def __init__(self: Self, endpoint: str,
                 workspace_name: str,
                 feature_service_name: str) -> None:

        """

        :param endpoint: HTTP endpoint to send request to
        :param workspace_name: Name of the Tecton Workspace to connect to
        :param feature_service_name: Name of the Feature Service to query
        """

        if not workspace_name:
            raise InvalidParameterException(
                InvalidParameterMessages.WORKSPACE_NAME)
        if not feature_service_name:
            raise InvalidParameterException(
                InvalidParameterMessages.FEATURE_SERVICE_NAME)

        self.endpoint = endpoint
        self.workspace_name = workspace_name
        self.feature_service_name = feature_service_name


class AbstractGetFeaturesRequest(TectonRequest):
    """
    Abstract class for all get_features requests
    """

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str,
                 get_features_request_data: GetFeatureRequestData,
                 metadata_options: Set["MetadataOptions"]) -> None:

        """

        :param endpoint: HTTP endpoint to send request to
        :param workspace_name: Name of the Tecton Workspace to connect to
        :param feature_service_name: Name of the Feature Service to query
        :param get_features_request_data: Request parameters for the query
        :param metadata_options: Options for retrieving additional metadata
        """

        super().__init__(endpoint, workspace_name, feature_service_name)

        if metadata_options is None or len(metadata_options) == 0:
            self.metadata_options = MetadataOptions.defaults()
        else:
            self.metadata_options = \
                metadata_options | MetadataOptions.defaults()

        if len(get_features_request_data.request_context_map) == 0 and \
                len(get_features_request_data.join_key_map) == 0:
            raise InvalidParameterException(
                InvalidParameterMessages.REQUEST_MAPS)


class GetFeaturesRequest(AbstractGetFeaturesRequest):
    """
    This is the class for requests to the get-features/ endpoint
    """

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features"

    def __init__(self: Self, workspace_name: str,
                 feature_service_name: str,
                 request_data: GetFeatureRequestData,
                 metadata_options: Optional[Set["MetadataOptions"]] = None)\
            -> None:
        """

        :param workspace_name: Name of the Tecton workspace to connect to
        :param feature_service_name: Name of the Feature Service to query
        :param request_data: Request parameters for the query
        :param metadata_options: Options for retrieving additional metadata
        """

        super().__init__(GetFeaturesRequest.ENDPOINT, workspace_name,
                         feature_service_name, request_data,
                         metadata_options)

        self.request_data = request_data

    @property
    def request_to_json(self: Self) -> str:
        """
        Converts the get-features request to JSON format to be passed
        :return: JSON formatted string
        """

        final_metadata_options = {}
        if len(self.metadata_options) != 0:
            metadata_option_values = \
                sorted([option.value for option in self.metadata_options])
            final_metadata_options = \
                {value: True for value in metadata_option_values}

        request_value_dictionary = GetFeaturesRequestJSON(
            feature_service_name=self.feature_service_name,
            join_key_map=self.request_data.join_key_map,
            metadata_options=final_metadata_options,
            request_context_map=self.request_data.request_context_map,
            workspace_name=self.workspace_name
        ).to_dict

        return json.dumps({"params": request_value_dictionary})
