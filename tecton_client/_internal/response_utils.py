from dataclasses import dataclass
from typing import Dict, List, Optional, Union

FeatureType = Union[None, int, float, str, List, Dict]

# Raw features values have not yet been parsed using type metadata. For example, Int64 features are encoded as strings
# and Struct features are encoded as a list of values instead of a dictionary.
RawFeatureValue = Union[None, int, float, str, List, Dict]


@dataclass
class GetFeaturesResult:
    """The raw feature values returned by the service.

    Attributes:
        features: List of raw feature values returned by the service.
    """

    features: List[RawFeatureValue]


@dataclass
class SLOInfo:
    """SLO and Serving time information. This is useful for debugging latency. Note: This will only be
            included if MetadataOption.include_slo_info is set to True in get_features(), otherwise it will be None.

    Attributes:
        slo_eligible: Whether the request was eligible for the latency SLO.
        slo_ineligibility_reasons: If slo_eligible is False, indicates the reason why.
        slo_server_time_seconds: The latency, in seconds of this request. This is the value that will be used for the
            latency SLI.
        server_time_seconds: The latency, in seconds, of this request as measured by the server. This includes the
            total time spent in the feature server including online transforms and store latency.
        store_max_latency: The maximum latency observed by the request from the online store in seconds.
        store_response_size_bytes: Total online store response size in bytes.
    """

    slo_eligible: bool
    slo_ineligibility_reasons: Optional[List[str]]
    slo_server_time_seconds: float
    server_time_seconds: float
    store_max_latency: float
    store_response_size_bytes: int


@dataclass
class GetFeaturesResponse:
    """Response from get_feature_service_metadata.

    Attributes:
        result: The raw data returned by the FeatureService; includes just the feature values.
            For a mapping of feature names to values, use get_features_dict()
        metadata: Any metadata returned. Control what metadata is returned from service using `metadata_options`
            parameter of `get_features`.
        slo_info: SLO and Serving time information. This is useful for debugging latency. Note: This will only be
            included if MetadataOption.include_slo_info is set to True in get_features(), otherwise it will be None.
    """

    result: GetFeaturesResult
    metadata: Optional[Dict]
    slo_info: Optional[SLOInfo]

    def get_features_dict(self) -> Dict[str, FeatureType]:
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
    def _get_feature_value(data_type_info: dict, raw_feature_value: RawFeatureValue) -> FeatureType:
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
        raw_slo_info = resp.get("metadata", {}).get("sloInfo")
        if resp.get("metadata", {}).get("sloInfo") is not None:
            slo_info = SLOInfo(
                slo_eligible=raw_slo_info.get("sloEligible"),
                slo_ineligibility_reasons=raw_slo_info.get("sloIneligibilityReasons"),
                slo_server_time_seconds=raw_slo_info.get("sloServerTimeSeconds"),
                server_time_seconds=raw_slo_info.get("serverTimeSeconds"),
                store_max_latency=raw_slo_info.get("storeMaxLatency"),
                store_response_size_bytes=raw_slo_info.get("storeResponseSizeBytes"),
            )
        else:
            slo_info = None
        return GetFeaturesResponse(
            result=GetFeaturesResult(**resp["result"]), metadata=resp.get("metadata"), slo_info=slo_info
        )


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
