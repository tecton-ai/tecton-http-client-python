import asyncio
import os

from tecton_client import AsyncTectonClient, MetadataOptions

my_url = "https://explore.tecton.ai/"
workspace = "prod"
my_api_key = os.environ.get("TECTON_API_KEY")


async_client = AsyncTectonClient(url=my_url, api_key=my_api_key, default_workspace_name=workspace)


async def call_api_ten_times():
    requests = [
        async_client.get_features(
            feature_service_name="fraud_detection_feature_service:v2",
            join_key_map={"user_id": "user_4407104885"},
            request_context_map={"amount": 500.00},
            metadata_options=MetadataOptions(include_data_types=True),
        )
        for i in range(10)
    ]
    responses = await asyncio.gather(*requests)
    for resp in responses:
        print(resp.result.features)


asyncio.run(call_api_ten_times())
