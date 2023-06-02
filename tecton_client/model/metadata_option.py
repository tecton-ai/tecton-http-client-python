from enum import Enum


class MetadataOptions(Enum):
    NAME = "include_names"
    EFFECTIVE_TIME = "include_effective_times"
    DATA_TYPE = "include_data_types"
    SLO_INFO = "include_slo_info"
    FEATURE_STATUS = "include_serving_status"
    ALL = ""
    NONE = ""


