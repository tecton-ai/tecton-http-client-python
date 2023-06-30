.. Tecton HTTP API Client Python documentation master file, created by
   sphinx-quickstart on Tue Jun 20 16:29:00 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Tecton HTTP API Client Python's documentation!
=========================================================

Tecton provides a low-latency feature server that exposes HTTP endpoints to retrieve feature values and metadata from the online store.
These endpoints are typically used during model predictions. The feature servers retrieve data from the online store and perform any additional aggregations and filtering as necessary.
For more information on the HTTP API, see the `Tecton HTTP API documentation <https://docs.tecton.ai/http-api>`_.

This is a Python client library to make it easy to call the feature server endpoints from your Python code.

.. note::

   This library is currently under development. Please contact Tecton support if you have any questions.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   data_types
   exceptions
   requests
   responses
   tecton_client


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
