from dataclasses import dataclass
from enum import Enum
from types import NoneType
from typing import Self, Optional, Dict, Union, Set

from tecton_client.exceptions.exceptions import (
    InvalidParameterMessages,
    InvalidParameterException
)


class MetadataOptions(str, Enum):
    """
    Options for retrieving metadata for get-features request
    """

    NAME = "include_names"
    EFFECTIVE_TIME = "include_effective_times"
    DATA_TYPE = "include_data_types"
    SLO_INFO = "include_slo_info"
    FEATURE_STATUS = "include_serving_status"

    @staticmethod
    def defaults() -> Set["MetadataOptions"]:
        """
        Setting default options to include names and data_types
        :return: Set["MetadataOptions"]
        """
        return {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}


class GetFeatureRequestData:
    """
    Class for request data needed for get-features queries
    """

    def __init__(self: Self,
                 join_key_map: Optional[Dict[str,
                                        Union[int, str, NoneType]]] = None,
                 request_context_map: Optional[Dict[str,
                                               Union[int, str, float]]]
                 = None) -> None:
        """
        Refer to API docs for more information:
        https://docs.tecton.ai/http-api#operation/GetFeatures

        :param join_key_map: Dictionary of Join keys used for
                             Batch and Stream FeatureViews
        :param request_context_map: Dictionary of Request context used for
                                    OnDemand FeatureViews
        """
        if join_key_map is not None:
            for key, value in join_key_map.items():
                if not key or type(key) is not str:
                    raise InvalidParameterException(
                        InvalidParameterMessages.KEY_VALUE)

                if type(value) not in (int, str, NoneType) or value == "":
                    raise InvalidParameterException(
                        InvalidParameterMessages.KEY_VALUE)

                join_key_map[key] = str(value) if type(value) is int else value

        if request_context_map is not None:
            for key, value in request_context_map.items():
                if not key or type(key) is not str:
                    raise InvalidParameterException(
                        InvalidParameterMessages.KEY_VALUE)

                if not value or type(value) not in (int, str, float):
                    raise InvalidParameterException(
                        InvalidParameterMessages.KEY_VALUE)

                request_context_map[key] = str(value) if \
                    type(value) not in (str, float) else value

        self.join_key_map: dict = join_key_map or {}
        self.request_context_map: dict = request_context_map or {}


@dataclass
class GetFeaturesRequestJSON:
    """
    Data required for the get-features request to be parsed into JSON format
    """

    feature_service_name: str
    join_key_map: Dict[str, Union[int, str, NoneType]]
    request_context_map: Dict[str, Union[int, str, float]]
    metadata_options: Optional[Dict[str, bool]]
    workspace_name: str

    @property
    def to_dict(self: Self) -> dict:
        """
        Converting the features into a dictionary
        :return: dict
        """
        return vars(self)
