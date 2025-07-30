import typing
from multiprocessing.pool import ThreadPool
from google.cloud import bigquery


def run_jobs_in_parallel(
    jobs: typing.List,
    callback: typing.Callable[[bigquery.job.CopyJobConfig], None],
    max_threads: int = 32
):
    """
    Maps callback function of a job in a thread pool.

    Args:
        jobs: List of job ids.
        callback: process that takes a CopyJobConfig as parameter.
        max_threads: Max threads, deafault to 32."""
    n_threads = min(max_threads, len(jobs))
    pool = ThreadPool(n_threads)
    pool.map(callback, jobs)
    pool.close()

