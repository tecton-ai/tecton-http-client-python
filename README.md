# Python Client Library for Tecton Online Feature Store

A simple Python client for the Feature Server HTTP API that helps customers integrate with Tecton easily.


## Documentation

* [Tecton Python Client API Reference](https://tecton-ai.github.io/tecton-http-client-python/html/index.html)

* [Tecton Python Client Example Code](/examples)

* [FeatureServer API Reference](https://docs.tecton.ai/rest-swagger/docs.html)

* [Fetching Online Features](https://docs.tecton.ai/latest/examples/fetch-real-time-features.html)


## Troubleshooting


If you have any questions or need help, please contact us using the instructions in the
[Tecton Docs](https://docs.tecton.ai/creating-a-tecton-support-ticket).

## Installing the client

The client can be installed using `pip`.

```bash
pip install tecton-client
```

The client can then be used as below:


```python
from tecton_client import TectonClient

url = "https://explore.tecton.ai/"
workspace = "prod"
api_key = "my-secret-key"

client = TectonClient(url=url, default_workspace_name=workspace, api_key=api_key)

resp = client.get_features(
    feature_service_name="fraud_detection_feature_service:v2",
    join_key_map={"user_id": "user_4407104885"},
    request_context_map={"amount": 500.00},
)

print(resp.get_features_dict())
```

For more information, please refer to Tecton documentation on the Python Client Library.

## Filing Issues and Feature Requests

### Python Client Issues
If you encounter a problem specifically related to the Python client, please file a but using the instructions in the
[Tecton Docs](https://docs.tecton.ai/creating-a-tecton-support-ticket). Please include the following information:

   - Description of the problem.
   - Steps to reproduce the issue.
   - Any relevant error messages or stack traces.
   - Versions of Python and the Python client you are using.

Please provide as much detail as possible when filing a support ticket to help us understand and resolve the issue efficiently.

Thank you for helping us improve our platform!

## Contributing

If you would like to contribute to the client, see [CONTRIBUTING.md](CONTRIBUTING.md)

## License

The project is licensed
under [Apache License 2.0](https://github.com/tecton-ai/tecton-http-client-python/blob/main/LICENSE.md)
