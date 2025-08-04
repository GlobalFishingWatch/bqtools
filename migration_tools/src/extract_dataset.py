import typing
import logging
import json
import re
import time
from gfw.common.cli import Option, ParametrizedCommand
from .utils import run_jobs_in_parallel
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery
from google.cloud import storage


logger = logging.getLogger(__name__)


JOB_PREFIX = "migrationtools_extract_dataset"
EXTRACT_JOB_CONFIG = bigquery.job.ExtractJobConfig(
    destination_format=bigquery.DestinationFormat.AVRO,
    use_avro_logical_types=True
)
LOCATION = "US"
DDL_QUERY = """SELECT ddl FROM `{dataset}.INFORMATION_SCHEMA.TABLES` WHERE  table_name = '{table_name}'"""
DDL_FILE = "ddl.sql"
SCHEMA_FILE = "schema.json"


def upload_blob(
    gcs: storage.Client,
    bucket_name: str,
    source_data: str,
    destination_blob_name: str,
    force_overwrite: bool
) -> bool:
    """
    Uploads data to the bucket.

    Args:
        gcs: storage client.
        bucket_name: Name of the gcs bucket.
        source_data: what we want to store.
        destination_blob_name: gcs destination.
        force_overwrite: overwrite and do not check precondition.

    Returns:
        True if it has been uploaded, False instead.
    """
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    # For a destination object that does not yet exist set if_generation_match precondition to 0.
    generation_match_precondition = None if force_overwrite else 0
    try:
        blob.upload_from_string(source_data, if_generation_match=generation_match_precondition)
        logger.info(f"Data was uploaded to gs://{bucket_name}/{destination_blob_name}.")
        return True
    except PreconditionFailed:
        logger.info(f"ALREADY EXIST IN gs://{bucket_name}/{destination_blob_name}.")
    return False


def split_table(
    full_table_id: str
) -> typing.Iterable:
    """
    Splits the full table id.

    Args:
        full_table_id: having formatting project:dataset.table.

    Returns:
        project, dataset, table.
    """
    return re.split(r"[:\.]", full_table_id)


def get_tables(
    client: bigquery.Client,
    dataset: str
) -> typing.List[str]:
    """
    Collects the simple tables, partitioned tables and views.

    Args:
        client: Bigquery Client.
        dataset: The dataset_id.

    Returns:
        List of full table ids.
    """
    return list(
        map(
            lambda t: t.full_table_id,
            filter(lambda x: x.table_type=="TABLE", client.list_tables(dataset=dataset))
        )
    )


def extract_tables(
    tables: typing.List[str],
    bq_client: bigquery.Client,
    bucket_name: str
) -> typing.Iterable:
    """
    Extracts the table content.

    Args:
        tables: List of full table id.
        bq_client: bigquery client.
        bucket_name: name of the bucket.

    Returns:
        List of extract jobs.
    """
    jobs = []
    for table in tables:
        project, dataset_id, table_id = split_table(table)
        gcs_path = f"gs://{bucket_name}/{project}/{dataset_id}/tables/{table_id}.avro"
        jobs.append(
            bq_client.extract_table(
                f"{project}.{dataset_id}.{table_id}",
                gcs_path,
                location=LOCATION,
                job_id_prefix=JOB_PREFIX,
                job_config=EXTRACT_JOB_CONFIG,
            )
        )
        logger.info(f"Table {table} extracted to {gcs_path}.")
    return jobs


def trigger_job(
    job: bigquery.ExtractJobConfig
):
    """
    Posts the extract job.

    Args:
        job: Extract job config.
    """
    job.result()


def upload_schema(
    table: str,
    *,
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    bucket_name: str,
    force_overwrite: bool,
):
    """
    Uploads the schema in json of the table.

    Args:
        table: Full table id.
        bq_client: bigquery client.
        storage_client: storage client.
        bucket_name: Name of the bucket.
        force_overwrite: overwrite the uploaded file.
    """
    project, dataset_id, table_id = split_table(table)
    gcs_path = f"{project}/{dataset_id}/tables/{table_id}_{SCHEMA_FILE}"
    t = bq_client.get_table(f"{project}.{dataset_id}.{table_id}")
    schema_json = json.dumps([x.to_api_repr() for x in t.schema])
    upload_blob(storage_client, bucket_name, schema_json, gcs_path, force_overwrite)


def upload_ddl(
    table: str,
    *,
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    bucket_name: str,
    force_overwrite: bool
):
    """
    Uploads the ddl of the table.

    Args:
        table: Full table id.
        bq_client: bigquery client.
        storage_client: storage client.
        bucket_name: Name of the bucket.
        force_overwrite: overwrite the uploaded file.
    """
    project, dataset_id, table_id = split_table(table)
    gcs_path = f"{project}/{dataset_id}/tables/{table_id}_{DDL_FILE}"
    ddl_rows = bq_client.query_and_wait(DDL_QUERY.format(dataset=dataset_id, table_name=table_id))
    for ddl_row in ddl_rows:
        upload_blob(storage_client, bucket_name, ddl_row.get("ddl"), gcs_path, force_overwrite)


def run(
    args: typing.List[str]
) -> int:
    """
    Run the copy of tables from a source dataset to a destination dataset.

    Args:
        args: The arguments to run the copy.

    Returns:
        Number if the process ended well or not."""
    start = time.time()
    client = bigquery.Client(project=args.source_project)
    storage_client = storage.Client(project=args.source_project)
    job_config = bigquery.QueryJobConfig(
        dry_run=args.dry_run,  # To estimate costs.
        priority="BATCH",
        use_legacy_sql=False,
        use_query_cache=False,
    )
    dataset_src = bigquery.DatasetReference(args.source_project, args.source_dataset)

    tables = get_tables(client, args.source_dataset)

    logger.info(f"Tables: {len(tables)}")
    extract_jobs = extract_tables(tables, bq_client=client, bucket_name=args.bucket_name)
    upload_args = dict(
        bq_client=client,
        storage_client=storage_client,
        bucket_name=args.bucket_name,
        force_overwrite=args.force_overwrite
    )

    logger.info("Extracting jobs..")
    run_jobs_in_parallel(extract_jobs, trigger_job)
    logger.info("Uploading schemas..")
    run_jobs_in_parallel(tables, upload_schema, upload_args)
    logger.info("Uploading ddls..")
    run_jobs_in_parallel(tables, upload_ddl, upload_args)

    end = time.time()
    logger.info(f"Time elapsed: {end - start} seconds")
    return 0


extract_dataset_command = ParametrizedCommand(
    name="extract_dataset",
    description="Extracts the dataset from one project to GCS.",
    options=[
        Option("-i", "--source_dataset", help="Source dataset.", required=True, type=str),
        Option("-ip", "--source_project", help="Source project.", required=True, type=str),
        Option("-gcsb", "--bucket_name", help="Dest bucket.", required=True, type=str),
        Option("-force", "--force_overwrite", help="Force the overwrite of the files.", default=False, type=bool),
    ],
    run=lambda config, **kwargs: run(config)
)
