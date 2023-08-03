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
tecton_client = TectonClient(url, api_key)

get_features_request_data = GetFeaturesRequestData(
    join_key_map={"user_id": "123", "merchant": "xyz"},
    request_context_map={"amt": 500.00},
)

get_features_request = GetFeaturesRequest(
    workspace_name="<Your-workspace>",
    feature_service_name="fraud_detection_feature_service",
    request_data=get_features_request_data,
)

get_features_response = tecton_client.get_features(get_features_request)

print(
    [feature.feature_value for feature in get_features_response.feature_values.values()]
)
```

For more information, please refer to Tecton documentation on the Python Client Library.

## License

The project is licensed
under [Apache License 2.0](https://github.com/tecton-ai/tecton-http-client-python/blob/main/LICENSE.md)
