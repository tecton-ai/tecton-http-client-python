import httpx
from enum import Enum

import pytest
from httpx_auth import HeaderApiKey

from tecton_client.exceptions.exceptions import TectonServerException, TectonClientException
from tecton_client.exceptions.tecton_error_message import TectonErrorMessage

API_PREFIX = "Tecton-key"


class TectonHttpClient:
    methods = Enum('methods', ['GET', 'POST', 'PUT', 'DELETE'])

    class headers(Enum):
        AUTHORIZATION = 'Authorization'
        ACCEPT = 'Accept'
        CONTENT_TYPE = 'Content-Type'

    def __init__(self, url, api_key):
        self.url = self.validate_url(url)
        self.apiKey = self.validate_key(api_key)

        self.auth = HeaderApiKey(header_name=self.headers.AUTHORIZATION.value,
                                 api_key=API_PREFIX + " " + self.apiKey)

        self.client = httpx.AsyncClient()
        self.isClientClosed = False

    async def close(self):
        await self.client.aclose()
        self.isClientClosed = True

    def is_closed(self):
        return self.isClientClosed

    async def perform_request(self, endpoint, method, http_request):
        """
        :param http_request: request data to be passed
        :param method: GET, PUT, POST etc.
        :param endpoint: Tecton endpoint to attach to the URL and query
        :type http_request: String in JSON format
        :type endpoint: String
        :type method: Enum
        """
        if method == self.methods.POST:
            url = self.url + "/" + endpoint

            response = await self.client.post(url, data=http_request, auth=self.auth)

            if response.status_code == 200:
                return response.json()
            else:
                error_message = str(response.status_code) + " " + response.reason_phrase + ": " + response.json()[
                    'message']
                raise TectonServerException(error_message)

    @staticmethod
    def validate_url(url):
        if url:
            return url
        else:
            raise (TectonClientException(TectonErrorMessage.INVALID_URL))

    @staticmethod
    def validate_key(api_key):
        if api_key:
            return api_key
        else:
            raise (TectonClientException(TectonErrorMessage.INVALID_KEY))


# FOR TESTING ONLY:
# if __name__ == '__main__':

@pytest.mark.asyncio
async def test():
    http_client = TectonHttpClient("https://app.tecton.ai/", "492dbdb681eb254b32f605324e144571")

    endpoint = "api/v1/feature-service/get-features"
    request = '{"params":{"feature_service_name":"fraud_detection_feature_service","join_key_map":{' \
              '"user_id":"user_205125746682"},"request_context_map":{"merch_long":35.0,"amt":500.0,"merch_lat":30.0},' \
              '"workspace_name":"tecton-fundamentals-tutorial-live"}}'

    response = await http_client.perform_request(endpoint, http_client.methods.POST, request)
    print(response)

    assert type(response) == type({})
