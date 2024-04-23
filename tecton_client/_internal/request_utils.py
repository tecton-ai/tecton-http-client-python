from dataclasses import dataclass
from typing import Dict


@dataclass
class MetadataOptions:
    """Passed into metadata_options on get_features, controls what metadata is returned as part of the response.

    Attributes:
        include_names: Include the name of each feature in the response
        include_data_types: Include the data type of each feature in the response
        include_effective_times: Include the effective times of the feature values in the response.
        include_slo_info: Include the SLO information as well as the Batch SLO Information in the response.
        include_serving_status: Include feature statuses in the response.
    """

    include_names: bool = True
    include_data_types: bool = True
    include_effective_times: bool = False
    include_slo_info: bool = False
    include_serving_status: bool = False

    @classmethod
    def all(cls):
        return MetadataOptions(
            include_names=True,
            include_data_types=True,
            include_effective_times=True,
            include_slo_info=True,
            include_serving_status=True,
        )

    def to_request(self) -> Dict[str, bool]:
        """Format for inclusion in GetFeaturesRequest"""
        return {
            "includeNames": self.include_names,
            "includeDataTypes": self.include_data_types,
            "includeEffectiveTimes": self.include_effective_times,
            "includeSloInfo": self.include_slo_info,
            "includeServingStatus": self.include_serving_status,
        }


@dataclass
class RequestOptions:
    """Passed into request_options on get_features, request level options to control feature server behavior.

    Attributes:
        read_from_cache: Disable if you want to skip the cache and read from the online store. Defaults to True.
        write_to_cache: Disable if you want to skip writing to the cache. Defaults to True.
    """

    read_from_cache: bool = True
    write_to_cache: bool = True

    def to_request(self) -> Dict[str, bool]:
        """Format for inclusion in GetFeaturesRequest"""
        return {
            "readFromCache": self.read_from_cache,
            "writeToCache": self.write_to_cache,
        }
