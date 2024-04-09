tecton\_client package
======================

.. module:: tecton_client

.. autoclass:: TectonClient
   :members:

   .. automethod:: __init__

.. autoclass:: AsyncTectonClient
   :members:

   .. automethod:: __init__

.. autoclass:: MetadataOptions
   :exclude-members: to_request

   .. automethod:: __init__

.. autoclass:: RequestOptions
   :exclude-members: to_request

   .. automethod:: __init__

.. autoclass:: GetFeaturesResponse
   :exclude-members: result, metadata, from_response

.. autoclass:: GetFeatureServiceMetadataResponse
   :exclude-members: from_response, input_join_keys, input_request_context_keys, feature_values, feature_service_type, output_join_keys

exceptions
----------

.. automodule:: tecton_client.exceptions
   :members:
   :show-inheritance:
   :exclude-members: convert_exception
