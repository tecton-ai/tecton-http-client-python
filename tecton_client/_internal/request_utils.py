from typing import Dict


class MetadataOptions:
    """Passed into metadata_options on get_features, controls what metadata is returned as part of the response."""

    def __init__(
        self,
        include_names: bool = True,
        include_data_types: bool = True,
        include_effective_times: bool = False,
        include_slo_info: bool = False,
        include_serving_status: bool = False,
        include_all: bool = False,
    ):
        """
        Args:
            include_names: Include the name of each feature in the response
            include_data_types: Include the data type of each feature in the response
            include_effective_times: Include the effective times of the feature values in the response.
            include_slo_info: Include the SLO information as well as the Batch SLO Information in the response.
            include_serving_status: Include feature statuses in the response.
            include_all: Include all metadata options

        """
        if include_all:
            self.include_names = True
            self.include_data_types = True
            self.include_effective_times = True
            self.include_slo_info = True
            self.include_serving_status = True
        else:
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
