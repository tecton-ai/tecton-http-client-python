==========
Quickstart
==========

Installation
============

.. code-block:: bash

   pip install tecton-client

Usage
=====

TectonClient Example
--------------------

The TectonClient is designed to provide a straightforward and efficient way to make HTTP requests against
Tecton's online feature store.



.. code-block:: python

   from tecton_client import TectonClient

   client = TectonClient(
       url="https://explore.tecton.ai/",
       api_key="my-api-key",
       default_workspace_name="prod",
   )

   resp = client.get_features(
       feature_service_name="fraud_detection_feature_service:v2",
       join_key_map={"user_id": "user_4407104885"},
       request_context_map={"amount": 500.00},
   )

   print(resp.result.features)



AsyncTectonClient Example
-------------------------

The AsyncTectonClient has the same method signatures as the (synchronous) TectonClient, but
is designed to leverage Python’s async/await syntax for non-blocking I/O operations.
This version is particularly suited for applications that benefit from concurrent execution,
such as high-performance web servers, integrations requiring simultaneous API calls, or UI applications demanding
responsive user interactions without blocking the main thread.

To use the asynchronous features, you will need some familiarity with Python's async/await syntax.
Replace your synchronous client calls with the async counterparts provided by Tecton,
and manage them within an async function or an event loop.

.. code-block:: python

   import asyncio
   from tecton_client import AsycTectonClient

   client = AsycTectonClient(
       url="https://explore.tecton.ai/",
       api_key="my-api-key",
       default_workspace_name="prod",
   )


   async def fetch_data():
       resp = client.get_features(
           feature_service_name="fraud_detection_feature_service:v2",
           join_key_map={"user_id": "user_4407104885"},
           request_context_map={"amount": 500.00},
       )
       print(resp.result.features)


   asyncio.run(fetch_data())
