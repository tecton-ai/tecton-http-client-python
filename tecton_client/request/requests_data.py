from enum import Enum
from typing import Self, Optional

from tecton_client.exceptions.exceptions import INVALID_KEY_VALUE, \
    TectonInvalidParameterException


class MetadataOptions(Enum):
    NAME = "include_names"
    EFFECTIVE_TIME = "include_effective_times"
    DATA_TYPE = "include_data_types"
    SLO_INFO = "include_slo_info"
    FEATURE_STATUS = "include_serving_status"


DEFAULT_METADATA_OPTIONS = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}


class GetFeatureRequestData:

    def __init__(self: Self, request_context_map: Optional[dict] = None,
                 join_key_map: Optional[dict] = None) -> None:

        if join_key_map is not None:
            for k, v in join_key_map.items():
                self.validate_key_value(k, v)

        if request_context_map is not None:
            for k, v in request_context_map.items():
                self.validate_key_value_disallowing_none(k, v)

        self.join_key_map: dict = join_key_map or {}
        self.request_context_map: dict = request_context_map or {}

    def add_join_key(self: Self, key: str, value: object) -> None:

        if value:
            value = str(value) if type(value) is not str else value

        self.validate_key_value(key, value)
        self.join_key_map[key] = value

    def add_request_context(self: Self, key: str, value: object) -> None:

        if value:
            value = str(value) if type(value) not in (str, float) else value

        self.validate_key_value_disallowing_none(key, value)
        self.request_context_map[key] = value

    @staticmethod
    def validate_key_value(key: str, value: object) -> None:
        if key == "" or key is None:
            raise TectonInvalidParameterException(INVALID_KEY_VALUE)

        if type(value) is str and value == "":
            raise TectonInvalidParameterException(INVALID_KEY_VALUE)

    @staticmethod
    def validate_key_value_disallowing_none(key: str, value: object) -> None:
        if key is None or key == "":
            raise TectonInvalidParameterException(INVALID_KEY_VALUE)

        if (type(value) is str and value == "") or value is None:
            raise TectonInvalidParameterException(INVALID_KEY_VALUE)
