from tectonClient.transport.tectonHttpClient import TectonHttpClient


class TectonClient:
    def __init__(self, url, apiKey):
        self.tectonHttpClient = TectonHttpClient(url, apiKey)
