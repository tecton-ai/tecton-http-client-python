import os

from tecton_client import MetadataOptions, TectonClient

my_url = "https://explore.tecton.ai/"
workspace = "prod"
my_api_key = os.environ.get("TECTON_API_KEY")

client = TectonClient(url=my_url, default_workspace_name=workspace, api_key=my_api_key)

resp = client.get_features(
    feature_service_name="fraud_detection_feature_service:v2",
    join_key_map={"user_id": "user_4407104885"},
    request_context_map={"amount": 500.00},
    metadata_options=MetadataOptions(include_data_types=True),
)

print("A nicely formatted output")
print(resp.get_features_dict())

print("Full response: ")
print(resp)
features = resp.result.features
print("Just the feature values: ")
print(features)
print("The names of the features: ")
print(resp.metadata.keys())

resp = client.get_feature_service_metadata(feature_service_name="fraud_detection_feature_service:v2")

print("Just metadata about the feature servie: ")
print(resp)
