from typing import Self, Optional

from tecton_client.exceptions.exceptions import EMPTY_REQUEST_MAPS, \
    INVALID_KEY_VALUE, TectonInvalidParameterException, \
    TectonEmptyFieldsException

from tecton_client.utils import MetadataOptions

DEFAULT_METADATA_OPTIONS = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}
ALL_METADATA_OPTIONS = set(MetadataOptions) - \
                       {MetadataOptions.ALL, MetadataOptions.NONE}
NONE_METADATA_OPTIONS = set()


class GetFeatureRequestData:

    def __init__(self: Self, request_context_map: Optional[dict] = None,
                 join_key_map: Optional[dict] = None) -> None:
        self.join_key_map: dict = join_key_map or {}
        self.request_context_map: dict = request_context_map or {}

    def is_empty_join_key_map(self: Self) -> bool:
        return True if len(self.join_key_map) == 0 else False

    def is_empty_request_context_map(self: Self) -> bool:
        return True if len(self.request_context_map) == 0 else False

<<<<<<< HEAD
    def add_join_key_map(self: Self, join_key_map: dict) -> None:
        if join_key_map is {}:
            raise TectonEmptyFieldsException(EMPTY_REQUEST_MAPS)
=======
    feature_service_name: str
    join_key_map: Dict[str, Union[int, str, NoneType]]
    request_context_map: Dict[str, Union[int, str, float]]
    metadata_options: Optional[Dict[str, bool]]
    workspace_name: str
>>>>>>> cd43997 (Fixing conflicts 3)

        for k, v in join_key_map.items():
            self.validate_key_value(k, v)

        self.join_key_map = join_key_map

    def add_request_context_map(self: Self, request_context_map: dict) -> None:
        if request_context_map is {}:
            raise TectonEmptyFieldsException(EMPTY_REQUEST_MAPS)

        for k, v in request_context_map.items():
            self.validate_key_value_disallowing_none(k, v)

        self.request_context_map = request_context_map

    def add_join_key(self: Self, key: str, value: object) -> None:

        if value:
            value = str(value) if type(value) is not str else value

        self.validate_key_value(key, value)
        self.join_key_map[key] = value

    def add_request_context(self: Self, key: str, value: object) -> None:

        if value:
            value = str(value) if type(value) is not str else value

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
