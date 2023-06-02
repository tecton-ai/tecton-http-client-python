from abc import ABC
from typing import Self

from tecton_client.exceptions.exceptions import TectonClientException, \
    INVALID_WORKSPACE_NAME, INVALID_FEATURE_SERVICE_NAME


class AbstractTectonRequest(ABC):

    def __init__(self: Self, endpoint: str,
                 workspace_name: str,
                 feature_service_name: str) -> None:

        AbstractTectonRequest.validate_request_parameters(
            workspace_name, feature_service_name)

        self._endpoint = endpoint
        self._workspace_name = workspace_name
        self._feature_service_name = feature_service_name

    @property
    def endpoint(self: Self) -> str:
        return self._endpoint

    @property
    def workspace_name(self: Self) -> str:
        return self._workspace_name

    @property
    def feature_service_name(self: Self) -> str:
        return self._feature_service_name

    @staticmethod
    def validate_request_parameters(workspace_name: str,
                                    feature_service_name: str) -> None:
        if not workspace_name:
            raise TectonClientException(INVALID_WORKSPACE_NAME)
        if not feature_service_name:
            raise TectonClientException(INVALID_FEATURE_SERVICE_NAME)
