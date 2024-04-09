# Contributing Guidelines

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
* Install the pre-commit hooks:
```bash
pip install pre-commit
pre-commit install
```
* Install dependencies: `pip install -e .[test]`
* Run tests to ensure everything is set up correctly: `pytest tests`

## Style Guildelines

Style and formatting is enforced using a tool called [pre-commit](https://pre-commit.com/). This can be installed locally and run over your changes prior to opening a PR, and will also be run as part of the CI approval process before a change is merged.

You can find the full list of formatting requirements specified in the .[pre-commit-config.yaml](./.pre-commit-config.yaml) at the top level directory.

## Docs

Docs can be built using the command:

```commandline
pip install docs/requirements.txt
sphinx-build -M html docs/ docs/_build
```
You can then view the docs by opening `docs/_build/html/index.html` in a browser.
