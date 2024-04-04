from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class GetFeaturesResult:
    features: list


@dataclass
class GetFeaturesResponse:
    result: GetFeaturesResult
    metadata: Optional[Dict] = None

    @classmethod
    def from_dict(cls, resp: dict) -> "GetFeaturesResponse":
        return GetFeaturesResponse(result=GetFeaturesResult(**resp["result"]), metadata=resp.get("metadata"))


@dataclass
class GetFeatureServiceMetadataResponse:
    feature_service_type: str
    input_join_keys: List[dict]
    input_request_context_keys: List[dict]
    feature_values: List[dict]

    @classmethod
    def from_dict(cls, resp: dict):
        """Constructor to create a GetFeatureServiceMetadataResponse from the json resonse of api"""
        return GetFeatureServiceMetadataResponse(
            feature_service_type=resp["featureServiceType"],
            input_join_keys=resp["inputJoinKeys"],
            input_request_context_keys=resp["inputRequestContextKeys"],
            feature_values=resp["featureValues"],
        )


class MetadataOptions:
    include_names = "include_names"
    include_effective_times = "include_effective_times"
    include_data_types = "include_data_types"
    include_slo_info = "include_slo_info"
    include_serving_status = "include_serving_status"


class RequestOptions:
    read_from_cache = "readFromCache"
    write_to_cache = "writeToCache"
