"""
Tools for making queries, moving the results to GCS and then
downloading via gsutil.

Here's the most basic way to use it:

bigq = BiqQuery()
query_job = bigq.async_query(proj_id, query, dataset, table)
bigq.poll_job(query_job)
extract_job = bigq.async_extract_query(query_job, gcs_path)
bigq.poll_job(extract_job)
gs_mv(gcs_path, local_path)


A more compact approach is:

# Suitable for small queries
bigq.query_and_extract(proj_id, query, gcs_path)  

# Suitable for large queries
bigq.query_and_extract(proj_id, query, gcs_path,
    temp_dest={'dataset': dataset, 'table': table})  


Finally, if issuing many parallizable queries, see 
`parallel_query_and_extract` below. It can speed up
these queries significantly at the cost of some 
complexity.


"""
from __future__ import unicode_literals, print_function, division
import uuid
import time
import subprocess
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


__version__ = '0.1'
__author__ = 'Tim Hochberg'
__email__ = 'tim@skytruth.org'
__source__ = 'COMING SOON'
__license__ = """
Copyright 2016 SkyTruth
Tim Hochberg <tim@skytruth.org>

COMING SOOON
"""



class BigQuery:

    def __init__(self):
        credentials = GoogleCredentials.get_application_default()
        self._bq = discovery.build('bigquery', 'v2', credentials=credentials)


    def async_query(self, project_id, query,
                    dataset=None, table=None, allow_large_results=True,
                    overwrite=False, batch=False, num_retries=5):
        """Create an asynchronous BigQuery query

        Parameters
        ----------
        project_id : str
            Google project id. Example: 'example-project-a'
        query : str
            The query to run on BigQuery
        dataset : str | None
            The dataset to place results in. Example: `scratch_dataset`
        table : str | None
            Table within dataset where results go. Example: `scratch_table_1`
        allow_large_results: bool

        batch : bool, optional
            Set the priority of the query to BATCH (low) or INTERACTIVE (high).
            Default is False (INTERACTIVE). Note that there are some quota
            or limit advantages to running in batch mode.
        num_retries: int
            Number of times BigQuery should retry the query. Default is 5.
            I'm not sure under what circumstances these can fail.

        If both `dataset` and `table` are `None` (the default) then
        a temporary, unnamed table, is created.

        Returns
        -------
        BiqQuery job

            This is a dictionary with fields describing the
            submitted job. Mainly it is passed to other BQ functions.

    """
        # Generate a unique job_id so retries
        # don't accidentally duplicate query
        job_data = {
            'jobReference': {
                'projectId': project_id,
                'job_id': str(uuid.uuid4())
            },
            'configuration': {
                'query': {
                    'allowLargeResults':
                        'true' if allow_large_results else 'false',
                    'query': query,
                    'priority': 'BATCH' if batch else 'INTERACTIVE',
                    'writeDisposition' :
                         'WRITE_TRUNCATE' if overwrite else 'WRITE_EMPTY',
                }
            }
        }
        if bool(dataset) ^ bool(table):
            raise ValueError("`dataset` and `table` must both be set or be None")
        if dataset:
            job_data['configuration']['query']['destinationTable'] = {
                                              "projectId": project_id,
                                              "datasetId": dataset,
                                              "tableId": table
                                              }
        elif allow_large_results:
            raise ValueError("Destination table must be set for allow large results")
        return self._bq.jobs().insert(
            projectId=project_id,
            body=job_data).execute(num_retries=num_retries)

    def is_done(self, job):
        """Is for BigQuery job done

        Parameters
        ----------
        job : BiqQuery job
            A dictionary with fields describing the submitted job. Source
            is typically another BQ function.

        Returns
        -------
        True | False | None

        None is returned if there is an error in the initial query.

        """
        request = self._bq.jobs().get(
            projectId=job['jobReference']['projectId'],
            jobId=job['jobReference']['jobId'])
        try:
            result = request.execute(num_retries=2)
        except:
            return None
        #
        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                err = result['status']['errorResult']
                text = "{reason}: {message}".format(**err)
                raise RuntimeError(text)
            else:
                return True
        else:
            return False

    def poll_job(self, job, max_tries=4000):
        """Waits for BigQuery job to complete

        Parameters
        ----------
        job : BiqQuery job
            A dictionary with fields describing the submitted job. Source
            is typically another BQ function.
        max_tries : int
            Max time to poll for results. Polls are about 1 s apart,
            so this also about how long to wait in seconds. Default
            is 4000 or a little over an hour.

        """
        for trial in range(max_tries):
            if self.is_done(job):
                return
            time.sleep(1)
        raise RuntimeError("timeout")


    def async_extract_query(self, job, path,
                            format="CSV",
                            compression="GZIP",
                            num_retries=5):
        """Extracts query into Google Cloud storage

        Parameters
        ----------
        project_id : BiqQuery job
            A dictionary with fields describing the submitted job. Source
            is typically another BQ function.
        path : str
            A path to a bucket in GCS. Example: 'gs://some_id/some/path/name.csv'
        format: {"CSV", "NEWLINE_DELIMITED_JSON", "AVRO"}

        compression: {"GZIP", "NONE"}

        num_retries: int
            Number of times BigQuery should retry the query. Default is 5.
            I'm not sure under what circumstances these can fail.

        Returns
        -------
        BiqQuery job

            This is a dictionary with fields describing the
            submitted job. Mainly it is passed to other BQ functions.

        """

        job_data = {
          'jobReference': {
              'projectId': job['jobReference']['projectId'],
              'jobId': str(uuid.uuid4())
          },
          'configuration': {
              'extract': {
                  'sourceTable': {
                      'projectId': job['configuration']['query']['destinationTable']['projectId'],
                      'datasetId': job['configuration']['query']['destinationTable']['datasetId'],
                      'tableId':   job['configuration']['query']['destinationTable']['tableId'],
                  },
                  'destinationUris': [path],
                  'destinationFormat': format,
                  'compression': compression
              }
          }
        }
        return self._bq.jobs().insert(
            projectId=job['jobReference']['projectId'],
            body=job_data).execute(num_retries=num_retries)

    @staticmethod
    def _extract_table_params(dest):
        if dest:
            return dest['dataset'], dest['table']
        return None, None

    def query_and_extract(self, proj_id, query, path,
                          format="CSV",
                          compression="GZIP",
                          temp_dest=None,
                          num_retries=5):
        """Perform a query and move the results to cloud storage

        Parameters
        ----------
        project_id : BiqQuery job
            A dictionary with fields describing the submitted job. Source
            is typically another BQ function.
        query : str
            The query to run on BigQuery
        path : str
            A path to a bucket in GCS. Example: 'gs://some_id/some/path/name.csv'
        format: {"CSV", "NEWLINE_DELIMITED_JSON", "AVRO"}

        compression: {"GZIP", "NONE"}

        temp_dest: {'dataset': str, 'table': str} | None
            If `None`, `allow_large_results` is set to false and an
            automatically generated temporary table is used. Otherwise,
            the given table is used. NOTE: `overwrite` is set to `True`,
            for this table!

        num_retries: int
            Number of times BigQuery should retry the query / extraction.
            Default is 5. I'm not sure under what circumstances these
            can fail.

        """
        dataset, table = self._extract_table_params(temp_dest)
        query_job = self.async_query(proj_id, query,
                                     dataset=dataset,
                                     table=table,
                                     allow_large_results=bool(temp_dest),
                                     overwrite=bool(temp_dest),
                                     num_retries=num_retries)
        self.poll_job(query_job)
        extract_job = self.async_extract_query(query_job, path,
                                               format=format,
                                               compression=compression,
                                               num_retries=num_retries)
        self.poll_job(extract_job)


    def parallel_query_and_extract(self, descrs):
        """Perform parallel queries, yielding gcs paths as extracted

        Parameters
        ----------

        descrs: sequencs of dicts
            dicts contain kwargs suitable for passing to `query_and_extract`

        Returns
        -------
        Iterator yielding GCS paths as extractions complete.


        Example
        -------

        # build queries
        for i in range(N):
            gcs_path = base_path + "data_{i}_*".format(index) + ".csv.gz"
            table = "scratch_{0}".format(i)
            temp_dest={'dataset': 'scratch', 'table': table}
            query = create_query(argument=i)
            queries.append(dict(
                proj_id=proj_id,
                query=query,
                path=gcs_path,
                temp_dest=temp_dest))

        for gcs_path in bigq.parallel_query_and_extract(queries):
            gcs_mv(gcs_path, destination_dir) 

        """
        query_jobs = []
        for dmap in descrs:
            temp_dest = dmap.get('temp_dest', None)
            dataset, table = self._extract_table_params(temp_dest)
            query_jobs.append(
                (self.async_query(dmap['proj_id'], dmap['query'],
                                 dataset=dataset,
                                 table=table,
                                 allow_large_results=bool(temp_dest),
                                 overwrite=bool(temp_dest),
                                 num_retries=dmap.get('num_retries', 5)),
                 dmap))
        extract_jobs = []
        while query_jobs or extract_jobs:
            time.sleep(0.1)
            new_query_jobs = []
            for job, dmap in query_jobs:
                if self.is_done(job):
                    extract_jobs.append(
                        (self.async_extract_query(job, dmap['path'],
                                               format=dmap.get('format', "CSV"),
                                               compression=dmap.get('compression', "GZIP"),
                                               num_retries=dmap.get('num_retries', 5)),
                         dmap))
                else:
                    new_query_jobs.append((job, dmap))
            query_jobs = new_query_jobs
            #
            new_extract_jobs = []
            for job, dmap in extract_jobs:
                if self.is_done(job):
                    yield dmap['path']
                else:
                    new_extract_jobs.append((job, dmap))
            extract_jobs = new_extract_jobs


def gs_mv(src_path, dest_path, quiet=True):
    """Move data using gsutil

    This was written to move data from cloud
    storage down to your computer and hasn't been
    tested for other things.

    Example:

    gs_mv("gs://world-fishing-827/scratch/SOME_DIR/SOME_FILE",
                "some/local/path/.")
    """
    args = ["gsutil", "-m", "mv", src_path, dest_path]
    if quiet:
        args.insert(1, '-q')
    subprocess.call(args)