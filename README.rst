BQTOOLS
=======

Tools for executing and downloading query results from Googles BigQuery.


Example
-------

.. code-block:: python

    import bqtools
    bigq = bqtools.BigQuery()
    bigq.query_and_extract(proj_id, query, gcs_path)  
    bqtools.gs_mv(gcs_path, local_path)


Developing
----------

.. code-block:: console

    git clone https://github.com/GlobalFishingWatch/bqtools
    cd bqtools
    pip install -e .
    py.test --cov bqtools --cov-report term-missing \
            --proj-id=YOUR_GCS_PROJ_ID \
            --gcs-temp-dir=YOUR_GCS_TEMP_DIR
            
For example:

.. code-block:: console

    py.test --cov bqtools --cov-report term-missing \
            --proj-id=world-fishing-827 \
            --gcs-temp-dir=gs://world-fishing-827/scratch/timh
           
Note, these tests run queries against BiqQuery using your account,
so (a) will take a while and (b) could incur some charges.


