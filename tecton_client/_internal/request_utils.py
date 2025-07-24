from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MetadataOptions:
    """Passed into metadata_options on get_features, controls what metadata is returned as part of the response.

    Attributes:
        include_names: Include the name of each feature in the response. Defaults to None (not set).
        include_data_types: Include the data type of each feature in the response. Defaults to None (not set).
        include_effective_times: Include the effective times of the feature values in the response. Defaults to None (not set).
        include_slo_info: Include the SLO information as well as the Batch SLO Information in the response. Defaults to None (not set).
        include_serving_status: Include feature statuses in the response. Defaults to None (not set).
        include_feature_descriptions: Include user-defined feature descriptions in the response. Defaults to None (not set).
        include_feature_tags: Include user-defined feature tags in the response. Defaults to None (not set).
    """

    include_names: Optional[bool] = None
    include_data_types: Optional[bool] = None
    include_effective_times: Optional[bool] = None
    include_slo_info: Optional[bool] = None
    include_serving_status: Optional[bool] = None
    include_feature_descriptions: Optional[bool] = None
    include_feature_tags: Optional[bool] = None

    @classmethod
    def all(cls):
        """Return a MetadataOptions object with all options set to True"""
        return MetadataOptions(
            include_names=True,
            include_data_types=True,
            include_effective_times=True,
            include_slo_info=True,
            include_serving_status=True,
            include_feature_descriptions=True,
            include_feature_tags=True,
        )

    def to_request(self) -> Dict[str, bool]:
        """Format for inclusion in GetFeaturesRequest"""
        request_dict = {}

        if self.include_names is not None:
            request_dict["includeNames"] = self.include_names
        if self.include_data_types is not None:
            request_dict["includeDataTypes"] = self.include_data_types
        if self.include_effective_times is not None:
            request_dict["includeEffectiveTimes"] = self.include_effective_times
        if self.include_slo_info is not None:
            request_dict["includeSloInfo"] = self.include_slo_info
        if self.include_serving_status is not None:
            request_dict["includeServingStatus"] = self.include_serving_status
        if self.include_feature_descriptions is not None:
            request_dict["includeFeatureDescriptions"] = self.include_feature_descriptions
        if self.include_feature_tags is not None:
            request_dict["includeFeatureTags"] = self.include_feature_tags

        return request_dict


@dataclass
class RequestOptions:
    """Passed into request_options on get_features, request level options to control feature server behavior.

    Attributes:
        read_from_cache: Disable if you want to skip the cache and read from the online store. Defaults to None (not set).
        write_to_cache: Disable if you want to skip writing to the cache. Defaults to None (not set).
        ignore_extra_request_context_fields: Enable if you don't want to fail the request if there are extra fields nested in Struct fields of request context. Defaults to None (not set).
        latency_budget_ms: Cutoff time for collecting results from feature service request. Once time is elapsed the feature server will make a best effort to return all feature values that have already been computed. Should be 100 or greater. Defaults to None (not set).
        coerce_null_counts_to_zero: Enable if you want to convert null count aggregation feature results to zero. Default behavior is determined by a cluster-wide flag (false by default). Contact Tecton support to change the default behavior. Defaults to None (not set).
    """

    read_from_cache: Optional[bool] = None
    write_to_cache: Optional[bool] = None
    ignore_extra_request_context_fields: Optional[bool] = None
    latency_budget_ms: Optional[int] = None
    coerce_null_counts_to_zero: Optional[bool] = None

    def to_request(self) -> Dict[str, Any]:
        """Format for inclusion in GetFeaturesRequest"""
        request_dict = {}

        if self.read_from_cache is not None:
            request_dict["readFromCache"] = self.read_from_cache
        if self.write_to_cache is not None:
            request_dict["writeToCache"] = self.write_to_cache
        if self.ignore_extra_request_context_fields is not None:
            request_dict["ignoreExtraRequestContextFields"] = self.ignore_extra_request_context_fields
        if self.latency_budget_ms is not None:
            request_dict["latencyBudgetMs"] = self.latency_budget_ms
        if self.coerce_null_counts_to_zero is not None:
            request_dict["coerceNullCountsToZero"] = self.coerce_null_counts_to_zero

        return request_dict
