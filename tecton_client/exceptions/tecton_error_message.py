from enum import Enum

"""
 Class that declares all the different error messages included in the {@link TectonClientException}
"""

class TectonErrorMessage(Enum):
    INVALID_KEY = "API Key cannot be empty"
    INVALID_URL = "Cannot connect to Tecton because the URL is invalid"