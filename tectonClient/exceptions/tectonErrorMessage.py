from enum import Enum

"""
Class that declares all the different error messages included in the {@link TectonClientException}
"""

class TectonErrorMessage(Enum):
    INVALID_KEY = "API Key cannot be empty"
    INVALID_URL = "Cannot connect to Tecton because the URL is invalid"
    INVALID_KEY_VALUE = "Key/Value cannot be null or empty"

    CALL_FAILURE = "Unable to perform call. %s"
    ERROR_RESPONSE = "Received Error Response from Tecton wih code %s and error message: %s"

    INVALID_WORKSPACENAME = "Workspace Name cannot be null or empty"
    INVALID_FEATURESERVICENAME = "FeatureService Name cannot be null or empty"
    EMPTY_REQUEST_MAPS = "Both Join Key map and Request Context Map cannot be empty"

    INVALID_GET_FEATURE_REQUEST = "The parameters passed to the GetFeatureRequest are invalid. %s"
    INVALID_GET_FEATURE_BATCH_REQUEST = "The parameters passed to the GetFeaturesBatchRequest are invalid. %s"
    INVALID_GET_SERVICE_METADATA_REQUEST = "The parameters passed to the GetFeatureServiceMetadataRequest are " \
                                           "invalid. %s"
    INVALID_RESPONSE_FORMAT = "Unable to parse JSON response from Tecton" 
    EMPTY_RESPONSE = "Received empty response body from Tecton" 
    EMPTY_FEATURE_VECTOR = "Received empty feature vector from Tecton" 

    MISSING_EXPECTED_METADATA = "Required metadata %s is missing in the response"
    UNKNOWN_DATA_TYPE = "Unknown Data Type %s in response" 
    MISMATCHED_TYPE = "Invalid method used to access value of type %s" 
    UNSUPPORTED_LIST_DATA_TYPE = "Unsupported data type detected for array feature values"
    UNKNOWN_DATETIME_FORMAT = "Unable to parse effectiveTime in the response metadata"
    INVALID_MICRO_BATCH_SIZE = "The microBatchSize is out of bounds and should be in the range [ %s , %s ]"
    INVALID_REQUEST_DATA_LIST = "The list of GetFeaturesRequestData objects cannot be null or empty"

