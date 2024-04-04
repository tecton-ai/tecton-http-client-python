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
    def __init__(
        self,
        include_names: bool = True,
        include_data_types: bool = True,
        include_effective_times: bool = False,
        include_slo_info: bool = False,
        include_serving_status: bool = False,
    ):
        self.include_names = include_names
        self.include_data_types = include_data_types
        self.include_effective_times = include_effective_times
        self.include_slo_info = include_slo_info
        self.include_serving_status = include_serving_status

    def to_request(self):
        """Format for inclusion in GetFeaturesRequest"""
        return {
            "includeNames": self.include_names,
            "includeDataTypes": self.include_data_types,
            "includeEffectiveTimes": self.include_effective_times,
            "includeSloInfo": self.include_slo_info,
            "includeServingStatus": self.include_serving_status,
        }


class RequestOptions:
    def __init__(self, read_from_cache: bool = True, write_to_cache: bool = True):
        self.read_from_cache = read_from_cache
        self.write_to_cache = write_to_cache

    def to_request(self):
        """Format for inclusion in GetFeaturesRequest"""
        return {
            "readFromCache": self.read_from_cache,
            "writeToCache": self.write_to_cache,
        }
