from typing import Self, Optional

from tecton_client.exceptions.exceptions import TectonClientException, \
    EMPTY_REQUEST_MAPS, INVALID_KEY_VALUE


class GetFeatureRequestData:

    def __init__(self: Self, request_context_map: Optional[dict] = None,
                 join_key_map: Optional[dict] = None) -> None:
        self._join_key_map: dict = join_key_map or {}
        self._request_context_map: dict = request_context_map or {}

    @property
    def join_key_map(self: Self) -> dict:
        return self._join_key_map

    @join_key_map.setter
    def join_key_map(self: Self, join_key_map: dict) -> None:
        self._join_key_map = join_key_map

    @property
    def request_context_map(self: Self) -> dict:
        return self._request_context_map

    @request_context_map.setter
    def request_context_map(self: Self, request_context_map: dict) -> None:
        self._request_context_map = request_context_map

    def is_empty_join_key_map(self: Self) -> bool:
        return True if len(self.join_key_map) == 0 else False

    def is_empty_request_context_map(self: Self) -> bool:
        return True if len(self.request_context_map) == 0 else False

    def add_join_key_map(self: Self, join_key_map: dict) -> None:
        if join_key_map is {}:
            raise TectonClientException(EMPTY_REQUEST_MAPS)

        for k, v in join_key_map.items():
            self.validate_key_value(k, v)

        self.join_key_map = join_key_map

    def add_request_context_map(self: Self, request_context_map: dict) -> None:
        if request_context_map is {}:
            raise TectonClientException(EMPTY_REQUEST_MAPS)

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
            raise TectonClientException(INVALID_KEY_VALUE)

        if type(value) is str and value == "":
            raise TectonClientException(INVALID_KEY_VALUE)

    @staticmethod
    def validate_key_value_disallowing_none(key: str, value: object) -> None:
        if key is None or key == "":
            raise TectonClientException(INVALID_KEY_VALUE)

        if (type(value) is str and value == "") or value is None:
            raise TectonClientException(INVALID_KEY_VALUE)
