"""
The script will calclulate how much a dataset tables costs in GB.
Then it gets the tables that the dataset has.
Then it runs the copy in a separate project
Then compares the amoun of Gb between the source and destination dataset.

Usage:
  python migrate_dataset.py \
    --source_project "world-fishing-827" --source_dataset "VMS_Brazil"
    --dest_project "gfw-int-vms-sources" --dest_dataset "VMS_BRA"

"""

import logging
import typing
import re
from gfw.common.cli import Option, ParametrizedCommand
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig, CopyJobConfig
from .utils import run_jobs_in_parallel


logger = logging.getLogger(__name__)

COSTS_QUERY = """select dataset_id, sum(size_bytes) total_bytes,
round(sum(size_bytes) / pow(1024, 3),2) total_gb from `{dataset}.__TABLES__`
group by 1"""

TABLES_PATTERN = "[0-9]{8}$"
PRESENT_TABLES_QUERY = """select if(regexp_contains(table_id, "{pattern}"),
regexp_replace(table_id, "{pattern}", "*"), table_id) as table_name from
`{args.source_dataset}.__TABLES__` group by 1"""

JOB_PREFIX = "migrationtools_migrate_dataset"


def get_costs(
    client: bigquery.Client,
    job_config: bigquery.QueryJobConfig,
    dataset: str
) -> int:
    """
    Get costs of a dataset in bytes.

    Args:
        client: Bigquery Client.
        job_config: Bigquery QueryJobConfig.
        dataset: The dataset.

    Returns:
        Storage size in bytes.
    """
    query = COSTS_QUERY.format(dataset=dataset)
    result = client.query_and_wait(query, job_config=job_config)
    size = 0
    for r in result:
        size = r.get("total_bytes", "NOT-KEY")
        logger.info(list(r.items()))
    return size


def present_tables(
    client: bigquery.Client,
    job_config: bigquery.QueryJobConfig,
    args: dict[str, str]
):
    """
    Present a summary of the tables to be copied.

    Args:
        client: Bigquery Client.
        job_config: Bigquery QueryJobConfig.
        args: The original arguments.
    """
    query = PRESENT_TABLES_QUERY.format(args=args, pattern=TABLES_PATTERN)
    result = client.query_and_wait(query, job_config=job_config)
    logger.info("Present tables to be copied:")
    for r in result:
        logger.info(list(r.values()))


def get_tables(
    client: bigquery.Client,
    job_config: bigquery.QueryJobConfig,
    dataset: str
) -> typing.Tuple:
    """
    Collects the simple tables, partitioned tables and views.

    Args:
        client: Bigquery Client.
        job_config: Bigquery QueryJobConfig.
        dataset: The project and dataset (ex: project.dataset)

    Returns:
        Three list as tuple in the following order: simple tables, partitioned
        tables and views.
    """
    simple_table_result = []
    partitioned_table_result = []
    not_simple_table_result = []
    items = client.list_tables(dataset=dataset)
    for t in items:
        if t.table_type == "TABLE":
            if not t.time_partitioning:
                simple_table_result.append(t.full_table_id)
            else:
                # (name, modified)
                pt = client.get_table(t.full_table_id.replace(':', '.'))
                partitioned_table_result.append((t.full_table_id, pt.modified))
        else:
            not_simple_table_result.append(t.full_table_id)
    logger.info(f"List tables from dataset {dataset} <Simple-Tables: {len(simple_table_result)}, "
                f"Partitioned-Tables: {len(partitioned_table_result)}, "
                f"Not-tables: {len(not_simple_table_result)}>")
    return simple_table_result, partitioned_table_result, not_simple_table_result


def trigger_job(
    job: CopyJobConfig
):
    """
    Post the copy job and catches if the table already exists.

    Args:
        job: Copy job config."""
    try:
        job.result()
    except Conflict as conf:
        logger.warning(f"It already exists is ok. {conf}")


def migrate_tables(
    client: bigquery.Client,
    tables: typing.List[str],
    source: str,
    dest: str,
    job_config: CopyJobConfig = None
):
    """
    Copies the tables from the source dataset to the dest dataset.

    Args:
        client: The bigquery Client.
        tables: The list of tables having the table_id.
        source: The project and dataset of the source.
        dest: The project and dataset of the destination.
        job_config: The copy job config."""
    jobs = []
    job_config = job_config or CopyJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_EMPTY",
    )
    for table_id in tables:
        src_table = f"{source}.{table_id}"
        dest_table = f"{dest}.{table_id}"
        try:
            job = client.copy_table(
                src_table,
                dest_table,
                job_config=job_config,
                job_id_prefix=JOB_PREFIX
            )
            jobs.append(job)
            logger.info(f"= SINGLE TABLE SCHED {src_table} -> {dest_table}")
        except Conflict as conf:
            logger.warning(f"Job id already exists is ok. {conf}")

    run_jobs_in_parallel(jobs, trigger_job)


def create_views(
    client: bigquery.Client,
    src_views: typing.List[str],
    dst_views: typing.List[str],
    dst_dataset: str
):
    """
    Create the views in the destination dataset.

    Args:
        client: The bigquery Client.
        views: List of views to migrate.
    """
    def split_parts(total: str) -> str:
        return re.split(r"[:\.]", total)

    dst_p, dst_d = split_parts(dst_dataset)
    for view_fti in src_views:
        src_p, src_d, src_t = split_parts(view_fti)
        matched = list(filter(lambda x: x.split('.')[1] == src_t, dst_views))
        if len(matched) == 0:
            view = client.get_table(view_fti)
            updated_query = view.view_query.replace(src_p, dst_p).replace(src_d, dst_d)
            dst_view = bigquery.Table(f"{dst_dataset}.{src_t}")
            dst_view.view_use_legacy_sql = view.view_use_legacy_sql
            dst_view.view_query = updated_query
            client.create_table(dst_views)
            logger.info(f"New View: {dst_view.table_type}: {str(dst_view.reference)}")
            logger.info(f"Original query: {view.view_query}\n\nUpdated query: {updated_query}")



def are_equal_size(
    client: bigquery.Client,
    job_config: QueryJobConfig,
    source: str,
    dest: str
) -> bool:
    """
    Checks if the size in bytes of the source dataset is the same as the
    destination dataset.

    Args:
        client: The bigquery Client.
        job_config: The copy job config.
        source: The project and dataset of the source.
        dest: The project and dataset of the destination.

    Returns:
        If both datasets are equals in bytes."""

    logger.info("Getting costs in source and destionation datasets:")
    logger.info(f"## Dataset: {source}")
    src_bytes = get_costs(client, job_config, source)
    logger.info(f"## Dataset: {dest}")
    dest_bytes = get_costs(client, job_config, dest)
    if src_bytes == dest_bytes:
        logger.info(f"### MIGRATION {source} -> {dest} COMPLETELY SUCCESS")
    else:
        logger.error(f"See differences in bytes {src_bytes-dest_bytes} ({(src_bytes-dest_bytes)/(1<<30)} GB)")
    return src_bytes == dest_bytes


def table_name(full_table_id: str) -> str:
    """
    Gets the table id.

    Args:
        full_table_id: Full table id with syntax project:dataset.table."""
    return full_table_id.split('.')[1]


def make_patitioned_comparable(
    item: typing.Tuple[str, str]
) -> typing.Tuple[str, str]:
    """
    Receives a tuple having full table id and modified date and returns a
    tuple having table_id and modified date.

    Args:
        item: Tuple with full table id and modified date.

    Returns:
        tuple with table id and date modified."""
    full_table_id, modified = item
    return (table_name(full_table_id), modified)


def difference(
    comparable: typing.Callable,
    src: typing.Iterable,
    dst: typing.Iterable
) -> typing.Set[str]:
    """
    Returns the difference between two iterables.

    Args:
        comparable: Function that makes compatible to compare the iterables.
        src: Iterable of sources tables.
        dst: Iterable of destination tables.

    Returns:
        A set with the difference having table_id."""
    diff = set(map(comparable, src)).difference(map(comparable, dst))
    if len(diff) != 0:
        logger.info(f"Tables that were not copied yet: {diff if len(diff) <= 20 else len(diff)}")
    return diff


def get_only_updated(
    src: typing.Iterable,
    dst: typing.Iterable,
) -> typing.List[str]:
    """
    Gets only the tables that should be updated.

    Args:
        src: An iterable having table and modification from the source.
        dst: An iterable having table and modification from the destination.

    Returns:
        List of table_id that should be updated."""
    to_update = []
    for fti, mod in src:
        src_tn = table_name(fti)
        dst_filtered = list(filter(lambda x: table_name(x[0]) == src_tn, dst))
        if len(dst_filtered) > 0:
            dst_tn, dst_mod = dst_filtered[0]
            if mod > dst_mod:
                logger.info(f"{src_tn}({mod.isoformat()} > {dst_mod.isoformat()}))")
                to_update.append(src_tn)
    return to_update


def run(
    args: typing.List[str]
) -> int:
    """
    Run the migrate of tables from a source dataset to a destination dataset.

    Args:
        args: The arguments to run the migration.

    Returns:
        Number if the process ended well or not."""
    client = bigquery.Client(project=args.source_project)
    job_config = QueryJobConfig(
        dry_run=args.dry_run,  # To estimate costs.
        priority="BATCH",
        use_legacy_sql=False,
        use_query_cache=False,
    )
    copy_job_config = CopyJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_EMPTY",
    )
    if args.force_override:
        copy_job_config = CopyJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )
    source = f"{args.source_project}.{args.source_dataset}"
    dest = f"{args.dest_project}.{args.dest_dataset}"

    present_tables(client, job_config, args)
    simple_tables, partitioned_tables, views = get_tables(client, job_config, source)
    if not are_equal_size(client, job_config, source, dest):
        dest_simple_tables, dest_partitioned_tables, dest_views = get_tables(client, job_config, dest)
        logger.info(
            f"\nSimple-tables <{source}: {len(simple_tables)}, {dest}: {len(dest_simple_tables)}>, "
            f"\nPartitioned-tables <{source}: {len(partitioned_tables)}, {dest}: {len(dest_partitioned_tables)}>, "
            f"\nNot-tables <{source}: {len(views)}, {dest}: {len(dest_views)}>\n"
        )

        # Simple tables
        simple_tbl_missing = difference(table_name, simple_tables, dest_simple_tables)
        migrate_tables(client, simple_tbl_missing, source, dest, copy_job_config)

        # Partitioned tables
        part_tbl_missing = difference(lambda x: table_name(x[0]), partitioned_tables, dest_partitioned_tables)
        part_tbl_updated = get_only_updated(partitioned_tables, dest_partitioned_tables)
        print("part_tbl_updated", part_tbl_updated)
        migrate_tables(client, part_tbl_missing, source, dest, copy_job_config)
        migrate_tables(client, part_tbl_updated, source, dest, CopyJobConfig(write_disposition="WRITE_TRUNCATE"))

        # Views
        create_views(client, views, dest_views, dest)
    return 0


migrate_dataset_command = ParametrizedCommand(
    name="migrate_dataset",
    description="Copies the dataset from one project to another.",
    options=[
        Option("-i", "--source_dataset", help="Source dataset.", required=True, type=str),
        Option("-ip", "--source_project", help="Source project.", required=True, type=str),
        Option("-o", "--dest_dataset", help="Dest dataset.", required=True, type=str),
        Option("-op", "--dest_project", help="Dest project.", required=True, type=str),
        Option("-force", "--force_override", help="Forces the override of all the VMS content.", default=False, type=bool),
    ],
    run=lambda config, **kwargs: run(config)
)
