import json
from abc import ABC
from dataclasses import dataclass
from typing import Self, Optional, Final, Set

from tecton_client.exceptions import (
    InvalidParameterMessage,
    InvalidParameterException
)
from tecton_client.request.requests_data import (
    MetadataOptions,
    GetFeatureRequestData
)


@dataclass
class TectonRequest(ABC):
    """
    Base class for all requests to the Tecton API
    """

    def __init__(self: Self, endpoint: str,
                 workspace_name: str,
                 feature_service_name: str) -> None:

        """
        Parent class constructor that configures the request endpoint,
        workspace_name and feature_service_name

        :param endpoint: HTTP endpoint to send request to
        :param workspace_name: Name of the workspace in which the
        Feature Service is defined
        :param feature_service_name: Name of the Feature Service for which
        the feature vector is being requested
        """

        if not workspace_name:
            raise InvalidParameterException(
                InvalidParameterMessage.WORKSPACE_NAME.value)
        if not feature_service_name:
            raise InvalidParameterException(
                InvalidParameterMessage.FEATURE_SERVICE_NAME.value)

        self.endpoint = endpoint
        self.feature_service_name = feature_service_name
        self.workspace_name = workspace_name


@dataclass
class AbstractGetFeaturesRequest(TectonRequest):
    """
    Base class for all requests to fetch feature values from Tecton API
    """

    def __init__(self: Self, endpoint: str,
                 workspace_name: str, feature_service_name: str,
                 metadata_options: Set["MetadataOptions"]) -> None:

        """
        Constructor that configures the request endpoint, workspace_name,
        feature_service_name and metadata_options

        :param endpoint: HTTP endpoint to send request to
        :param workspace_name: Name of the workspace in which
        the Feature Service is defined
        :param feature_service_name: Name of the Feature Service for which
        the feature vector is being requested
        :param metadata_options: Options for retrieving additional metadata
        about feature values
        """

        super().__init__(endpoint, workspace_name, feature_service_name)

        self.metadata_options = MetadataOptions.defaults() \
            if not metadata_options \
            else metadata_options | MetadataOptions.defaults()


@dataclass
class GetFeaturesRequest(AbstractGetFeaturesRequest):
    """
    Class that represents a request to the /get-features endpoint
    """

    ENDPOINT: Final[str] = "/api/v1/feature-service/get-features"

    def __init__(self: Self, workspace_name: str,
                 feature_service_name: str,
                 request_data: GetFeatureRequestData,
                 metadata_options: Optional[Set["MetadataOptions"]] = None) \
            -> None:
        """
        Constructor that configures the workspace_name, feature_service_name,
        request_data and metadata_options

        :param workspace_name: Name of the workspace in which
        the Feature Service is defined
        :param feature_service_name: Name of the Feature Service for which
        the feature vector is being requested
        :param request_data: Request parameters for the query
        :param metadata_options: (Optional) Options for retrieving additional
        metadata about feature values
        """

        super().__init__(GetFeaturesRequest.ENDPOINT, workspace_name,
                         feature_service_name,
                         metadata_options)

        self.request_data = request_data

    def to_dict(self: Self) -> dict:

        self_dict = vars(self)
        self_dict.pop("endpoint")

        if self.request_data.join_key_map:
            self_dict["join_key_map"] = self.request_data.join_key_map
        if self.request_data.request_context_map:
            self_dict["request_context_map"] = \
                self.request_data.request_context_map

        self_dict.pop("request_data")

        return self_dict

    @property
    def to_json(self: Self) -> str:
        """
        Returns a JSON representation of the GetFeaturesRequest
        :return: JSON formatted string
        """

        final_metadata_options = {}
        if len(self.metadata_options) != 0:
            metadata_option_values = \
                sorted([option.value for option in self.metadata_options])
            final_metadata_options = \
                {value: True for value in metadata_option_values}

        request_value_dictionary = self.to_dict()
        request_value_dictionary["metadata_options"] = final_metadata_options

        return json.dumps({"params": request_value_dictionary})
