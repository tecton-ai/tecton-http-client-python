# Contributing Guidelines

## Reporting Bugs

If you encounter a bug, please help us by submitting a detailed bug report. Include information such as:

* Steps to reproduce the bug
* Expected behavior
* Actual behavior
* Environment details (operating system, Python version, etc.)

## Suggesting Enhancements

We welcome suggestions for new features or enhancements. Please provide a clear description of the proposed enhancement,
along with any relevant use cases or examples.

## Pull Requests

* Fork the repository and create a new branch for your contribution.
* Ensure your code adheres to the project's coding style and conventions.
* Write clear, concise commit messages.
* Include tests for any new functionality or changes.
* Update documentation as needed.
* Submit a pull request, explaining the purpose of your changes.

## Development Setup

To set up the project for development:

* (suggested) Create a virtual env: `python -m venv test_venv; . ./test_venv/bin/activate`
* Install dependencies: `pip install .[test]`
* Run tests to ensure everything is set up correctly: `pytest tests`


### Release Steps (Tecton internal)

1. Ensure the version in [`./tecton_client/__about__.py`](./tecton_client/__about__.py) has been updated following
[Semantic Versioning Guidelines](https://semver.org/). You can use [hatch](https://hatch.pypa.io/latest/version/) to
update the version.
2. If the release is meant as a beta release for testing, ensure that it has a beta version format (i.e. `X.X.XbX`)
3. Create a new [release from Github](https://github.com/tecton-ai/tecton-http-client-python/releases/new)
4. Publish the release. This will automatically trigger a client deployment to pypi
