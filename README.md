# Python Client Library for Tecton Online Feature Store

A simple Python client for the Feature Server HTTP API that helps customers integrate with Tecton easily.


## Documentation


* [Fetching Online Features](https://docs.tecton.ai/latest/examples/fetch-real-time-features.html)

* [FeatureServer API Reference](https://docs.tecton.ai/rest-swagger/docs.html)

* [Tecton Python Client API Reference](https://tecton-ai.github.io/tecton-http-client-python/html/index.html)

* [Tecton Python Client Example Code](https://github.com/tecton-ai/tecton-http-client-python-demo/)


## Troubleshooting


If you have any questions or need help, please [open an Issue](https://github.com/tecton-ai/tecton-http-client-python)
or reach out to us on Slack!

## Installing the client

The client can be installed using `pip`.

```bash
pip install tecton-client
```

The client can then be used as below:


```python
from tecton_client import TectonClient

url = 'https://explore.tecton.ai/'
workspace = 'prod'
api_key = 'my-secret-key'

client1 = TectonClient(url=url,
                       default_workspace_name=workspace,
                       api_key=api_key)

resp = client1.get_features(feature_service_name="fraud_detection_feature_service:v2",
                             join_key_map={"user_id": "user_4407104885"},
                             request_context_map={"amount": 500.00}
                             )

print(resp.result.features)
```

For more information, please refer to Tecton documentation on the Python Client Library.

## License

The project is licensed
under [Apache License 2.0](https://github.com/tecton-ai/tecton-http-client-python/blob/main/LICENSE.md)
