## Release Steps (Tecton internal)

1. Ensure the version in [`./tecton_client/__about__.py`](./tecton_client/__about__.py) has been updated following
[Semantic Versioning Guidelines](https://semver.org/). You can use [hatch](https://hatch.pypa.io/latest/version/) to
update the version.
2. If the release is meant as a beta release for testing, ensure that it has a beta version format (i.e. `X.X.XbX`)
3. Create a new [release from Github](https://github.com/tecton-ai/tecton-http-client-python/releases/new)
4. Publish the release. This will automatically trigger a client deployment to pypi
