from tecton_client.model.metadata_option import MetadataOptions

DEFAULT_METADATA_OPTIONS = {MetadataOptions.NAME, MetadataOptions.DATA_TYPE}
ALL_METADATA_OPTIONS = set(MetadataOptions) - \
                       {MetadataOptions.ALL, MetadataOptions.NONE}
NONE_METADATA_OPTIONS = set()
