"""Test the API by running a simple query against one of the
public datasets
"""


import os

import tempfile

import pytest

import bqtools



# Query run against the NOAA GSOD database. Based on the one at:
# https://cloud.google.com/bigquery/public-data/noaa-gsod

query = """
SELECT
  max,
  (max-32)*5/9 celsius,
  mo,
  da,
  state,
  stn,
  name
FROM (
  SELECT
    max,
    mo,
    da,
    state,
    stn,
    name,
    ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
  FROM
    [bigquery-public-data:noaa_gsod.gsod2015] a
  JOIN
    [bigquery-public-data:noaa_gsod.stations] b
  ON
    a.stn=b.usaf
    AND a.wban=b.wban
  WHERE
    state IS NOT NULL
    AND max<1000
    AND country='US' )
WHERE
  rn=1
ORDER BY
  max DESC, name
LIMIT 10;
"""

expected_query_result = """
max,celsius,mo,da,state,stn,name
129.9,54.388888888888886,06,29,AK,703605,PLATINUM AIRPORT
127.4,53,05,17,CO,740002,LA VETA PASS AWOS-3 ARPT
126.1,52.277777777777779,07,22,TX,720647,LAMPASAS AIRPORT
121.8,49.888888888888886,06,21,CA,999999,STOVEPIPE WELLS 1 SW
117,47.222222222222221,08,16,AZ,740035,YUMA MCAS
115,46.111111111111114,07,17,OK,723525,HOBART MUNICIPAL AIRPORT
113,45,06,27,NV,723860,MCCARRAN INTERNATIONAL AIRPOR
113,45,06,29,WA,727846,WALLA WALLA REGIONAL ARPT
111.2,44,06,26,UT,724754,ST GEORGE MUNICIPAL ARPT
111,43.888888888888886,06,28,OR,726883,HERMISTON MUNICIPAL ARPT
""".strip()



local_path = tempfile.gettempdir() + "/test_bqtools.csv"


# Currently only testing the small result interface. Testing the larger
# result interface requires also having a scratch BQ table path
# XXX add that as a passed parameter and only run if set. (XXX -> issues)
# XXX Many of the options still untested as well.

def test_basic_async_query(proj_id, std_gcs_path):
    bigq = bqtools.BigQuery()
    query_job = bigq.async_query(proj_id, query, allow_large_results=False)
    bigq.poll_job(query_job)
    extract_job = bigq.async_extract_query(query_job, std_gcs_path, compression=None)
    bigq.poll_job(extract_job)
    _download_and_check(std_gcs_path)


def test_basic_query_and_extract(proj_id, std_gcs_path):
    "Test the basic, small-result interface"
    bigq = bqtools.BigQuery()
    bigq.query_and_extract(proj_id, query, std_gcs_path, compression="NONE")  
    _download_and_check(std_gcs_path)


def test_basic_parallel_query_and_extract(proj_id, gcs_temp_dir):
    queries = []
    for i in range(3):
        # We are creating three identical queries here,
        # typically, you'd use different queries
        gcs_path = gcs_temp_dir + "temp_bqtools_{}.csv".format(i)
        queries.append(dict(
            proj_id=proj_id,
            query=query,
            compression="NONE",
            path=gcs_path))
    bigq = bqtools.BigQuery()
    for gcs_path in bigq.parallel_query_and_extract(queries):
        _download_and_check(gcs_path)


def test_broken_query(proj_id, std_gcs_path):
    query = "SELECT COUNT(*) FRM_MISPELLED [bigquery-public-data:noaa_gsod.gsod2015]"
    bigq = bqtools.BigQuery()
    assert pytest.raises(RuntimeError, bigq.query_and_extract, 
                         proj_id, query, std_gcs_path, compression="NONE")  




def _download_and_check(gcs_path):
    bqtools.gs_mv(gcs_path, local_path)
    with open(local_path) as f:
        query_result = f.read().strip()
    assert query_result == expected_query_result, query_result

