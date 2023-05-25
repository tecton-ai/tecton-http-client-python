import httpx
from enum import Enum
from httpx_auth import HeaderApiKey
from tectonClient.exceptions.tectonErrorMessage import TectonErrorMessage
from tectonClient.exceptions.tectonClientException import TectonClientException
from tectonClient.exceptions.tectonServerException import TectonServerException


class TectonHttpClient:
    API_PREFIX = "Tecton-key"
    methods = Enum('methods', ['GET', 'POST', 'PUT', 'DELETE'])

    class headers(Enum):
        AUTHORIZATION = 'Authorization'
        ACCEPT = 'Accept'
        CONTENT_TYPE = 'Content-Type'

    def __init__(self, url, apiKey, client: httpx.Client | None = None):
        self.url = self.validate_url(url)
        self.apiKey = self.validate_key(apiKey)

        self.auth = HeaderApiKey(header_name=self.headers.AUTHORIZATION.value,
                                 api_key=self.API_PREFIX + " " + self.apiKey)

        self.client: httpx.Client = client or httpx.Client()

    def close(self):
        self.client.close()

    def perform_request(self, endpoint, method, httpRequest):
        """
        :param httpRequest: request data to be passed
        :param method: GET, PUT, POST etc.
        :param endpoint: Tecton endpoint to attach to the URL and query
        :type httpRequest: String in JSON format
        :type endpoint: String
        :type method: Enum
        """
        if method == self.methods.POST:
            url = self.url + "/" + endpoint

            response = self.client.post(url, data=httpRequest, auth=self.auth)

            if response.status_code == 200:
                return response.json()
            else:
                errorMessage = str(response.status_code) + " " + response.reason_phrase + ": " + response.json()[
                    'message']
                raise TectonServerException(errorMessage)

    @staticmethod
    def validate_url(url):
        if url:
            return url
        else:
            raise (TectonClientException(TectonErrorMessage.INVALID_URL))

    @staticmethod
    def validate_key(apiKey):
        if apiKey:
            return apiKey
        else:
            raise (TectonClientException(TectonErrorMessage.INVALID_KEY))


if __name__ == '__main__':
    httpClient = TectonHttpClient("https://app.tecton.ai/", "492dbdb681eb254b32f605324e14457")

    endpoint = "api/v1/feature-service/get-features"
    request = '{"params":{"feature_service_name":"fraud_detection_feature_service","join_key_map":{' \
              '"user_id":"user_205125746682"},"request_context_map":{"merch_long":35.0,"amt":500.0,"merch_lat":30.0},' \
              '"workspace_name":"tecton-fundamentals-tutorial-live"}}'

    httpClient.perform_request(endpoint, httpClient.methods.POST, request)
