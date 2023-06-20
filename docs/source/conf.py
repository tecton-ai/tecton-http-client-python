# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import os
import sys

sys.path.insert(0, os.path.abspath("../../"))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Tecton HTTP API Client Python"
copyright = "2023, Tecton Inc."
author = "Tecton Inc."
release = "0.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc.typehints",
    "sphinx.ext.autosummary",
]

# AutosectionLabel settings.
# Uses a <page>:<label> schema which doesn't work for duplicate sub-section
# labels, so set max depth.
autosectionlabel_prefix_document = True
autosectionlabel_maxdepth = 2

# AutodocTypehints settings.
always_document_param_types = True
typehints_defaults = "comma"

templates_path = ["_templates"]
exclude_patterns = []

# The suffix of source filenames.
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
