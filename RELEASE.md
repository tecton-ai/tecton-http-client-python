## Release Steps (Tecton internal)

1. Choose a version, following [Semantic Versioning Guidelines](https://semver.org/). If the release is meant as a beta release,
ensure that it has a beta version format (i.e. `0.2.0b1`)
2. Ensure that the version in [`./tecton_client/__about__.py`](./tecton_client/__about__.py) has been updated to match.
Keep the `+local` at the end of the local version; it will be removed during the release.
3. Create a new [release from Github](https://github.com/tecton-ai/tecton-http-client-python/releases/new). Use the tag `tecton-vX.Y.Z`, entering the version chosen above.
4. Fill in information about what has changed
5. Publish the release. This will automatically trigger a client deployment to pypi, using the version you
chose for the tag.
6. If this fails for some reason, delete the release, fix the issue, and publish the release again.
