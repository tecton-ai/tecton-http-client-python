import asyncio
import os

from tecton_client import AsyncTectonClient
from tecton_client._internal.data_types import MetadataOptions
from tecton_client._internal.tecton_client import TectonClient

my_url = "https://explore.tecton.ai/"
workspace = "prod"
my_api_key = os.environ.get("TECTON_API_KEY")

client1 = TectonClient(url=my_url, default_workspace_name=workspace, api_key=my_api_key)

resp = client1.get_features(
    feature_service_name="fraud_detection_feature_service:v2",
    join_key_map={"user_id": "user_4407104885"},
    request_context_map={"amount": 500.00},
    metadata_options={MetadataOptions.include_data_types: True},
)


print(resp)
features = resp.result.features
print(features)
print(resp.metadata.keys())

resp = client1.get_feature_service_metadata(feature_service_name="fraud_detection_feature_service:v2")

print(resp)

async_client = AsyncTectonClient(url=my_url, api_key=my_api_key, default_workspace_name=workspace)


async def main():
    for i in range(10):
        response = await async_client.get_features(
            feature_service_name="fraud_detection_feature_service:v2",
            join_key_map={"user_id": "user_4407104885"},
            request_context_map={"amount": 500.00},
            metadata_options={MetadataOptions.include_data_types: True},
        )
        print("Async", response.result.features)


asyncio.run(main())
