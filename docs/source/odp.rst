Quickstart
==========

Authenticate
------------

In order to use the ODP SDK, you need to authenticate using your provided API-key. This is achieved by setting the
`api_key`-argument when instantiating `ODPClient`:

.. code:: python

    from odp_sdk import ODPClient
    client = ODPClient(api_key="<my-api-key>")

You can also set the `COGNITE_API_KEY` environment variable:

.. code:: bash

    $ export COGNITE_API_KEY=<my-api-key>

Download Ocean Data
-------------------

Downloading ocean data is very easy once you have instantiated the `ODPClient`. The data is then returned as a
Pandas DataFrame_

.. code:: python

    df = client.casts(longitude=[-25, 35], latitude=[50, 80], timespan=["2018-06-01", "2018-06-30"])

.. _DataFrame: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

It is also possible to specify what parameters to download:

.. code:: python

    df = client.casts(
        longitude = [-25, 35],
        latitude = [50, 80],
        timespan = ["2018-06-01", "2018-06-30"],
        parameters = ["date", "lon", "lat", "z", "Temperature", "Salinity"
    )

In some instances, some filtering is necessary before downloading the data. This is achieved by first
listing the available casts:

.. code:: python

    casts = client.get_available_casts(
        longitude = [-25, 35],
        latitude = [50, 80],
        timespan = ["2018-06-01", "2018-06-30"],
        metadata_parameters = ["extId", "date", "time", "lat", "lon", "country", "Platform", "dataset_code"
    )

Then apply any desirable filters before downloading the data:

.. code:: python

    casts_norway = casts[casts.country == "NORWAY"]
    df = client.download_data_from_casts(casts_norway.extId.tolist(),
                                         parameters=["date", "lat", "lon", "z", "Temperature", "Salinity")

You can also download the cast metadata:

.. code:: python

    df = client.get_metadata(casts_norway.extId.tolist())

API
===
ODPClient
---------
.. autoclass:: odp_sdk.ODPClient
    :members:
    :member-order: bysource

Utilities
=========

Advanced Helper Functions
-------------------------

.. py:currentmodule:: Examples

Interpolate Casts to Z
^^^^^^^^^^^^^^^^^^^^^^

.. automethod:: UtilityFunctions.interpolate_casts_to_z

Interpolate Casts to grid
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.interpolate_to_grid

Interpolate profile
^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.interpolate_profile

Plot Casts
^^^^^^^^^^
.. automethod:: UtilityFunctions.plot_casts

Plot Grid
^^^^^^^^^
.. automethod:: UtilityFunctions.plot_grid

Get Units
^^^^^^^^^
.. automethod:: UtilityFunctions.get_units

Plot percentage of nulls for each variable in variable list
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.plot_nulls

Plot metadata-statistics
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.plot_meta_stats

Plot distribution of values
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.plot_distributions

Plot casts belonging to specific dataset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.plot_datasets

Internal Helper Functions
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: UtilityFunctions.geo_map
.. automethod:: UtilityFunctions.missing_values

Geographic Utilities
--------------------

Convert Latitude and Longitude to Geo-Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: odp_sdk.utils.gcs_to_index

Convert Latitude and Longitude to grid-coordinates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: odp_sdk.utils.gcs_to_grid

Convert Geo-Index to grid-coordinates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: odp_sdk.utils.index_to_grid

Convert Geo-Index to Latitude and Longitude
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: odp_sdk.utils.index_to_gcs

Get all grid-coordinates within a rectangle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: odp_sdk.utils.grid_rect_members

Get all Geo-Indices within a rectangle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: odp_sdk.utils.index_rect_members
