import typing
from multiprocessing.pool import ThreadPool


def run_jobs_in_parallel(
    jobs: typing.List,
    callback: typing.Callable,
    kw_args: typing.Dict = None,
    max_threads: int = 32
):
    """
    Maps callback function of a job in a thread pool.

    Args:
        jobs: List of job ids.
        callback: process that takes an item of jobs as parameter.
        max_threads: Max threads, deafault to 32."""
    n_threads = min(max_threads, len(jobs))
    pool = ThreadPool(n_threads)
    pool.map(
        lambda j: callback(j, **kw_args) if kw_args is not None else callback(j),
        jobs
    )
    pool.close()
