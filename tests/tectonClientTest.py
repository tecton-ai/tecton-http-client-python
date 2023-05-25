from pytest_httpx import HTTPXMock
from tectonClient.exceptions.tectonClientException import TectonClientException
from tectonClient.exceptions.tectonServerException import TectonServerException
from tectonClient.tectonClient import TectonClient
import pytest

# class TectonClientTest:

    # apiKey = "1234"
    # def __init__(self, httpx_mock: HTTPXMock):
    #     httpx_mock.add_response("Standard Response")

apiKey = "1234"
url = "https://thisisaurl.ai"
def testEmptyURL():
    with pytest.raises(TectonClientException):
        tectonClient = TectonClient("", "1234")

def testEmptyKey():
    with pytest.raises(TectonClientException):
        tectonClient = TectonClient(url, "")

def testNoneKey():
    with pytest.raises(TectonClientException):
        tectonClient = TectonClient("", None)

def testInvalidApiKey(httpx_mock: HTTPXMock):
    expectedMessage = "401 Unauthorized: invalid 'Tecton-key' authorization header. Note that newly created credentials may take up to 60 seconds to be usable."

    try:
        tectonClient = TectonClient(url, apiKey)
    except TectonServerException as e:
        print(e)
        assert e == expectedMessage