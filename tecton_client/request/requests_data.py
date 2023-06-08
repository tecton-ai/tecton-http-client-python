from enum import Enum
from dataclasses import dataclass
from types import NoneType
from typing import Self, Optional, Dict, Union, Set

from tecton_client.exceptions import (
    InvalidParameterMessage,
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


@dataclass
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
        Constructor that configures join_key_map and request_context_map
        :param join_key_map: Join keys used for table-based FeatureViews
        :param request_context_map: Request context used for
        OnDemand FeatureViews
        """

        if join_key_map is None and request_context_map is None:
            raise InvalidParameterException(
                InvalidParameterMessage.REQUEST_MAPS.value)

        if join_key_map is not None:
            for key, value in join_key_map.items():
                if not key or value == "":
                    raise InvalidParameterException(
                        InvalidParameterMessage.KEY_VALUE.value)

                if type(key) is not str:
                    msg = f"{InvalidParameterMessage.TYPE_KEY.value} " \
                          f"Join Key-Map key: {key}"
                    raise InvalidParameterException(
                        msg)

                if type(value) not in (int, str, NoneType):
                    msg = f"{InvalidParameterMessage.TYPE_JOIN_VALUE.value}" \
                          f" {value}"
                    raise InvalidParameterException(
                        msg)

                join_key_map[key] = str(value) if type(value) is int else value

        if request_context_map is not None:
            for key, value in request_context_map.items():
                if not key or not value:
                    raise InvalidParameterException(
                        InvalidParameterMessage.KEY_VALUE.value)

                if type(key) is not str:
                    msg = f"{InvalidParameterMessage.TYPE_KEY.value} " \
                          f"Request Context Map key: {key}"
                    raise InvalidParameterException(
                        msg)

                if type(value) not in (int, str, float):
                    msg = f"{InvalidParameterMessage.TYPE_REQ_VALUE.value}" \
                          f" {value}"
                    raise InvalidParameterException(
                        msg)

                request_context_map[key] = str(value) if \
                    type(value) not in (str, float) else value

        self.join_key_map: dict = join_key_map or {}
        self.request_context_map: dict = request_context_map or {}
