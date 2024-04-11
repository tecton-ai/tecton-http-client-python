from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass
class GetFeaturesResult:
    """The raw feature values returned by the service.

    Attributes:
        features: List of raw feature values returned by the service.
    """

    features: List


@dataclass
class GetFeaturesResponse:
    """Response from get_feature_service_metadata.

    Attributes:
        result: The raw data returned by the FeatureService; includes just the feature values.
            For a mapping of feature names to values, use get_features_dict()
        metadata: Any metadata returned. Control what metadata is returned from service using `metadata_options`
            parameter of `get_features`.
    """

    result: GetFeaturesResult
    metadata: Optional[Dict] = None

    def get_features_dict(self) -> Dict[str, Any]:
        """Return the feature values as a dictionary mapping name to value, and converting str to int64 if needed"""
        if self.metadata is None or self.metadata.get("features") is None:
            raise ValueError(
                "Metadata is not included in response. To use get_features_dict, "
                "MetadataOptions include_names and include_data_types must be set to True."
            )
        features_metadata = self.metadata["features"]
        # check that both name and datatypes are present
        if features_metadata[0].get("name") is None:
            raise ValueError("To use get_features_dict MetadataOptions include_names must be set to True.")
        if features_metadata[0].get("dataType") is None:
            raise ValueError("To use get_features_dict MetadataOptions include_data_types must be set to True.")
        return {
            metadata["name"]: self._get_feature_value(metadata["dataType"], raw_feature_value)
            for metadata, raw_feature_value in zip(features_metadata, self.result.features)
        }

    @staticmethod
    def _get_feature_value(data_type_info: dict, raw_feature_value: Optional[Union[int, float, str, list, dict]]):
        """Convert response value from str to int if the data type is int64"""
        data_type = data_type_info.get("type")
        if raw_feature_value is None:
            return None
        elif data_type == "int64":
            return int(raw_feature_value)
        elif data_type == "array":
            element_type = data_type_info.get("elementType")
            return [GetFeaturesResponse._get_feature_value(element_type, value) for value in raw_feature_value]
        elif data_type == "map":
            # apply the unpacking function to the keys and values separately
            key_type = data_type_info.get("keyType")
            value_type = data_type_info.get("valueType")
            return {
                GetFeaturesResponse._get_feature_value(key_type, key): GetFeaturesResponse._get_feature_value(
                    value_type, value
                )
                for key, value in raw_feature_value.items()
            }
        elif data_type == "struct":
            field_meta_list = data_type_info.get("fields")
            return {
                field_meta.get("name"): GetFeaturesResponse._get_feature_value(field_meta.get("dataType"), field_value)
                for field_meta, field_value in zip(field_meta_list, raw_feature_value)
            }
        else:
            return raw_feature_value

    @classmethod
    def from_response(cls, resp: dict) -> "GetFeaturesResponse":
        """Internal method for converting response of Service into an object"""
        return GetFeaturesResponse(result=GetFeaturesResult(**resp["result"]), metadata=resp.get("metadata"))


@dataclass
class GetFeatureServiceMetadataResponse:
    """Response from get_feature_service_metadata.

    Attributes:
        input_join_keys: The expected fields to be passed in the joinKeyMap parameter.
        input_request_context_keys: The expected fields to be passed in the requestContextMap parameter.
        feature_values: The fields to be returned in the features list in GetFeaturesResponse or QueryFeaturesResponse.
            The order of returned features will match the order returned by GetFeaturesResponse or QueryFeaturesResponse.

    """

    input_join_keys: Optional[List[dict]]
    input_request_context_keys: Optional[List[dict]]
    feature_values: List[dict]

    @classmethod
    def from_response(cls, resp: dict):
        """Constructor to create a GetFeatureServiceMetadataResponse from the json response of api"""
        return GetFeatureServiceMetadataResponse(
            input_join_keys=resp.get("inputJoinKeys"),
            input_request_context_keys=resp.get("inputRequestContextKeys"),
            feature_values=resp["featureValues"],
        )


class MetadataOptions:
    """Passed into metadata_options on get_features, controls what metadata is returned as part of the response."""

    def __init__(
        self,
        include_names: bool = True,
        include_data_types: bool = True,
        include_effective_times: bool = False,
        include_slo_info: bool = False,
        include_serving_status: bool = False,
    ):
        """
        Args:
            include_names: Include the name of each feature in the response
            include_data_types: Include the data type of each feature in the response
            include_effective_times: Include the effective times of the feature values in the response.
            include_slo_info: Include the SLO information as well as the Batch SLO Information in the response.
            include_serving_status: Include feature statuses in the response.

        """
        self.include_names = include_names
        self.include_data_types = include_data_types
        self.include_effective_times = include_effective_times
        self.include_slo_info = include_slo_info
        self.include_serving_status = include_serving_status

    def to_request(self) -> Dict[str, bool]:
        """Format for inclusion in GetFeaturesRequest"""
        return {
            "includeNames": self.include_names,
            "includeDataTypes": self.include_data_types,
            "includeEffectiveTimes": self.include_effective_times,
            "includeSloInfo": self.include_slo_info,
            "includeServingStatus": self.include_serving_status,
        }


class RequestOptions:
    """Passed into request_options on get_features, request level options to control feature server behavior."""

    def __init__(self, read_from_cache: bool = True, write_to_cache: bool = True):
        """this

        Args:
            read_from_cache: Disable if you want to skip the cache and read from the online store. Defaults to True.
            write_to_cache: Disable if you want to skip writing to the cache. Defaults to True.
        """
        self.read_from_cache = read_from_cache
        self.write_to_cache = write_to_cache

    def to_request(self) -> Dict[str, bool]:
        """Format for inclusion in GetFeaturesRequest"""
        return {
            "readFromCache": self.read_from_cache,
            "writeToCache": self.write_to_cache,
        }
