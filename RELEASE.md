## Release Steps (Tecton internal)

1. Choose a version, following [Semantic Versioning Guidelines](https://semver.org/). If the release is meant as a beta release,
ensure that it has a beta version format (i.e. `0.2.0b1`)
2. Create a new [release from Github](https://github.com/tecton-ai/tecton-http-client-python/releases/new). Use the tag `tecton-vX.Y.Z`, entering the version chosen above.
3. Fill in information about what has changed
4. Publish the release. This will automatically trigger a client deployment to pypi, using the version you
chose for the tag.
