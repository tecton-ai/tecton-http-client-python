from tectonClient.exceptions.tectonServerException import TectonServerException
from tectonClient.transport.tectonHttpClient import TectonHttpClient

url = "https://thisisaurl.ai"
apiKey = "abcd1234"

def testHttpClient():

    httpClient = TectonHttpClient(url, apiKey)
    assert httpClient.isClientClosed == False

def testPerformHttpRequestSuccess():

    url = "https://app.tecton.ai/"
    apiKey = "492dbdb681eb254b32f605324e144571"

    httpClient = TectonHttpClient(url, apiKey)

    endpoint = "api/v1/feature-service/get-features"
    request = '{"params":{"feature_service_name":"fraud_detection_feature_service","join_key_map":{' \
                  '"user_id":"user_205125746682"},"request_context_map":{"merch_long":35.0,"amt":500.0,"merch_lat":30.0},' \
                  '"workspace_name":"tecton-fundamentals-tutorial-live"}}'

    response = httpClient.perform_request(endpoint, httpClient.methods.POST, request)
    assert type(response) == type({})

def testPerformHttpRequestFailure():

    url = "https://app.tecton.ai/"
    apiKey = "dummy-key"

    httpClient = TectonHttpClient(url, apiKey)

    endpoint = "api/v1/feature-service/get-features"
    request = '{"params":{"feature_service_name":"fraud_detection_feature_service","join_key_map":{' \
                  '"user_id":"user_205125746682"},"request_context_map":{"merch_long":35.0,"amt":500.0,"merch_lat":30.0},' \
                  '"workspace_name":"tecton-fundamentals-tutorial-live"}}'

    try:
        response = httpClient.perform_request(endpoint, httpClient.methods.POST, request)

    except Exception as e:
        assert type(e) == TectonServerException