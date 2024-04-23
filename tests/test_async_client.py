import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from httpx import Headers

from tecton_client import AsyncTectonClient, MetadataOptions, RequestOptions
from tecton_client.exceptions import NotFoundError


class TestTectonClient(IsolatedAsyncioTestCase):
    def mockPatch(self, *args, **kwargs):
        patcher = patch(*args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def setUp(self):
        _request_log = []

        def handler(request):
            _request_log.append(json.loads(request.content.decode("utf8")))
            return httpx.Response(200, json={"result": {"features": []}})

        self.mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.mock_client._request_log = _request_log

    @patch("tecton_client._internal.utils.tecton_client_version", "0.1.0test")
    def test_client_construction(self):
        mock_httpx_constructor = self.mockPatch("httpx.AsyncClient", autospec=True)
        AsyncTectonClient(url="https://fake.tecton.ai", api_key="fake-api-key", default_workspace_name="workspace")
        mock_httpx_constructor.assert_called_once_with(
            headers=Headers(
                {"authorization": "Tecton-key fake-api-key", "user-agent": "tecton-http-python-client 0.1.0test"}
            )
        )

    @patch("tecton_client._internal.utils.tecton_client_version", "0.1.0test")
    def test_client_construction_custom_client(self):
        mock_httpx_constructor = self.mockPatch("httpx.AsyncClient", autospec=True)
        mock_client = MagicMock()
        mock_client.headers = {}
        AsyncTectonClient(
            url="https://fake.tecton.ai", api_key="fake-api-key", default_workspace_name="workspace", client=mock_client
        )
        mock_httpx_constructor.assert_not_called()
        self.assertEqual(
            mock_client.headers,
            {"authorization": "Tecton-key fake-api-key", "user-agent": "tecton-http-python-client 0.1.0test"},
        )

    async def test_default_workspace_null(self):
        client = AsyncTectonClient(url="https://fake.tecton.ai", api_key="fake-api-key", client=self.mock_client)

        with self.assertRaisesRegexp(ValueError, "workspace_name not set"):
            await client.get_features(feature_service_name="fake-feature-service", join_key_map={"user_id": "id123"})

        await client.get_features(
            feature_service_name="fake-feature-service",
            join_key_map={"user_id": "id123"},
            workspace_name="override-workspace",
        )
        workspace = self.mock_client._request_log[0]["params"]["workspaceName"]
        self.assertEqual(workspace, "override-workspace")

    async def test_default_workspace(self):
        client = AsyncTectonClient(
            url="https://fake.tecton.ai",
            api_key="fake-api-key",
            client=self.mock_client,
            default_workspace_name="fake-workspace",
        )

        await client.get_features(feature_service_name="fake-feature-service", join_key_map={"user_id": "id123"})
        workspace = self.mock_client._request_log[0]["params"]["workspaceName"]
        self.assertEqual(workspace, "fake-workspace")

    async def test_override_workspace(self):
        client = AsyncTectonClient(
            url="https://fake.tecton.ai",
            api_key="fake-api-key",
            client=self.mock_client,
            default_workspace_name="fake-workspace",
        )

        await client.get_features(
            feature_service_name="fake-feature-service",
            join_key_map={"user_id": "id123"},
            workspace_name="override-workspace",
        )

        workspace = self.mock_client._request_log[0]["params"]["workspaceName"]
        self.assertEqual(workspace, "override-workspace")

    async def test_get_features_encode(self):
        # using just magic_mock here in order to assert on client.post.assert_called_with
        mock_response = MagicMock()
        mock_response.json.return_value = {"result": {"features": []}}
        mock_http_client = MagicMock(post=AsyncMock())
        mock_http_client.post.return_value = mock_response
        client = AsyncTectonClient(
            url="https://fake.tecton.ai",
            api_key="fake-api-key",
            default_workspace_name="workspace",
            client=mock_http_client,
        )
        await client.get_features(
            feature_service_name="fake-feature-service",
            join_key_map={"user_id": "id123"},
            metadata_options=MetadataOptions(include_effective_times=True, include_data_types=False),
            request_options=RequestOptions(read_from_cache=False),
        )
        mock_http_client.post.assert_called_with(
            url="https://fake.tecton.ai/api/v1/feature-service/get-features",
            json={
                "params": {
                    "workspaceName": "workspace",
                    "featureServiceName": "fake-feature-service",
                    "joinKeyMap": {"user_id": "id123"},
                    "requestContextMap": {},
                    "allowPartialResults": False,
                    "metadataOptions": {
                        "includeNames": True,
                        "includeDataTypes": False,
                        "includeEffectiveTimes": True,
                        "includeSloInfo": False,
                        "includeServingStatus": False,
                    },
                    "requestOptions": {"readFromCache": False, "writeToCache": True},
                }
            },
        )

    async def test_get_feature_service_metadata_encode(self):
        # using just magic_mock here in order to assert on client.post.assert_called_with
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "inputJoinKeys": [],
            "inputRequestContextKeys": [],
            "featureValues": [],
        }
        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        client = AsyncTectonClient(
            url="https://fake.tecton.ai",
            api_key="fake-api-key",
            default_workspace_name="workspace",
            client=mock_http_client,
        )
        await client.get_feature_service_metadata(
            feature_service_name="fake-feature-service",
        )
        mock_http_client.post.assert_called_with(
            url="https://fake.tecton.ai/api/v1/feature-service/metadata",
            json={
                "params": {
                    "workspaceName": "workspace",
                    "featureServiceName": "fake-feature-service",
                }
            },
        )

    async def test_get_features_decode(self):
        test_client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"result": {"features": []}}))
        )
        client = AsyncTectonClient(
            url="https://fake.tecton.ai", api_key="fake-api-key", default_workspace_name="workspace", client=test_client
        )
        resp = await client.get_features(feature_service_name="fake-feature-service", join_key_map={"user_id": "id123"})
        self.assertEqual(resp.result.features, [])

    async def test_get_feature_service_metadata_decode(self):
        test_client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"featureValues": ["this"]}))
        )
        client = AsyncTectonClient(
            url="https://fake.tecton.ai", api_key="fake-api-key", default_workspace_name="workspace", client=test_client
        )
        resp = await client.get_feature_service_metadata(feature_service_name="fake-feature-service")
        self.assertEqual(resp.feature_values, ["this"])

    async def test_raise_error(self):
        test_client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(404, json={"result": {"features": []}}))
        )
        client = AsyncTectonClient(
            url="https://fake.tecton.ai", api_key="fake-api-key", default_workspace_name="workspace", client=test_client
        )
        with self.assertRaises(NotFoundError):
            await client.get_features(feature_service_name="fake-feature-service", join_key_map={"user_id": "id123"})
