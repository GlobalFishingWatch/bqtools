import typing
import logging
import json
import re
import time
from gfw.common.cli import Option, ParametrizedCommand
from .utils import run_jobs_in_parallel
from google.api_core.exceptions import BadRequest, PreconditionFailed
from google.cloud import bigquery
from google.cloud import storage


logger = logging.getLogger(__name__)


JOB_PREFIX = "migrationtools_extract_dataset"
EXTRACT_JOB_CONFIG = bigquery.job.ExtractJobConfig(
    destination_format=bigquery.DestinationFormat.AVRO,
    use_avro_logical_types=True
)
LOCATION = "US"
DDL_QUERY = """
SELECT ddl FROM `{dataset}.INFORMATION_SCHEMA.TABLES` WHERE  table_name = '{table_name}'
"""
DDL_FILE = "ddl.sql"
SCHEMA_FILE = "schema.json"
EXTRACT_JOBS_1DAY_USED_QUERY = """
SELECT count(*) triggered
FROM `{source_project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE job_type = 'EXTRACT'
AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
AND CURRENT_TIMESTAMP()"""
EXTRACT_DAILY_HALF_LIMIT = int(100000/2)
GCS_TO_BQ_REGEX = r"([^/]*)/([^/]*)/tables/([^\.]*)\.avro"
TABLE_TYPE_TABLE = "TABLE"
TABLE_TYPE_VIEW = "VIEW"


class ExtractQuotaExceeded(Exception):
    def __init__(self):
        super().__init__("Extract jobs reached the limit of #"
                         f"{EXTRACT_DAILY_HALF_LIMIT} jobs for this process.")
        self.error_code = 2


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
    # return re.split(r"[:\.]", full_table_id)
    return full_table_id.split('.')


def get_tables(
    client: bigquery.Client,
    storage_client: storage.Client,
    dataset: str,
    table_type: str
) -> typing.Set[str]:
    """
    Collects the simple tables, partitioned tables that pending to be stored in GCS.

    Args:
        client: Bigquery Client.
        storage_client: Storage Client.
        dataset: The dataset_id.
        table_type: The table type.

    Returns:
        Set of full table ids.
    """
    logger.info(f"Getting tables under {dataset}..")
    tables_in_dataset = set(
        map(
            lambda t: t.full_table_id.replace(':', '.'),
            filter(lambda x: x.table_type == table_type, client.list_tables(dataset=dataset))
        )
    )
    logger.info(f"Tables in dataset: {len(tables_in_dataset)}")
    if table_type == TABLE_TYPE_VIEW:
        return tables_in_dataset
    matcher_path = f"{client.project}/{dataset}/tables/*.avro"
    logger.info(f"Getting avro files under gs://{storage_client.project}/{matcher_path}..")
    blobs_stored = list(map(
        lambda x: '.'.join(re.search(GCS_TO_BQ_REGEX, x.name).group(1, 2, 3)),
        storage_client.list_blobs(storage_client.project, match_glob=matcher_path)
    ))
    logger.info(f"Tables stored in GCS: {len(blobs_stored)}")
    result = tables_in_dataset.difference(blobs_stored)
    logger.info(f"Tables to extract: {len(result)}")
    return result


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

    Raises:
        ExtractQuotaExceeded if reached the extract job quota defined in EXTRACT_DAILY_HALF_LIMIT.
    """
    jobs = []

    rows = bq_client.query_and_wait(
        EXTRACT_JOBS_1DAY_USED_QUERY.format(source_project=bq_client.project)
    )
    limitation = EXTRACT_DAILY_HALF_LIMIT - next(rows).get("triggered")
    logger.info(f"Available daily export jobs: {limitation}.")
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
        if len(jobs) >= limitation:
            raise ExtractQuotaExceeded()
        logger.info(f"Extract table {table} scheduled.")
    return jobs


def trigger_job(
    job: bigquery.ExtractJobConfig
):
    """
    Posts the extract job.

    Args:
        job: Extract job config.
    """
    try:
        job.result()
    except BadRequest as err:
        # Check if alredy stored in GCS if not extract in multi parts.
        gcs_path = job.destination_uris[0]
        bucket, blob = re.search("gs://([^/]*)/(.*)", gcs_path).group(1, 2)
        storage_client = storage.Client(project=bucket)
        stored = list(
            storage_client.list_blobs(
                storage_client.project,
                match_glob=f"{blob}/part_*.avro"
            )
        )
        if len(stored) == 0:
            logger.error(f"Extract table {job.source} failed due to {err}")
            bq_client = bigquery.Client(project=job.project)
            new_job = bq_client.extract_table(
                job.source,
                f"{gcs_path}/part_*.avro",
                location=job.location,
                job_id_prefix=JOB_PREFIX,
                job_config=EXTRACT_JOB_CONFIG,
            )
            new_job.result()
            logger.info(f"Extract table {job.source} in several files {gcs_path}/part_*.avro")
        else:
            logger.info(f"Table {job.source} ALREADY EXISTS in several files {gcs_path}/part_*.avro")




def upload_schema(
    table: str,
    *,
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    bucket_name: str,
    force_overwrite: bool,
    table_type: str
):
    """
    Uploads the schema in json of the table.

    Args:
        table: Full table id.
        bq_client: bigquery client.
        storage_client: storage client.
        bucket_name: Name of the bucket.
        force_overwrite: overwrite the uploaded file.
        table_type: The table type.
    """
    project, dataset_id, table_id = split_table(table)
    gcs_path = f"{project}/{dataset_id}/{table_type}s/{table_id}_{SCHEMA_FILE}"
    t = bq_client.get_table(f"{project}.{dataset_id}.{table_id}")
    schema_json = json.dumps([x.to_api_repr() for x in t.schema])
    upload_blob(storage_client, bucket_name, schema_json, gcs_path, force_overwrite)


def upload_ddl(
    table: str,
    *,
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    bucket_name: str,
    force_overwrite: bool,
    table_type: str
):
    """
    Uploads the ddl of the table.

    Args:
        table: Full table id.
        bq_client: bigquery client.
        storage_client: storage client.
        bucket_name: Name of the bucket.
        force_overwrite: overwrite the uploaded file.
        table_type: The table type.
    """
    project, dataset_id, table_id = split_table(table)
    gcs_path = f"{project}/{dataset_id}/{table_type}s/{table_id}_{DDL_FILE}"
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
    try:
        client = bigquery.Client(project=args.source_project)
        storage_client = storage.Client(project=args.dest_project)
        upload_args = dict(
            bq_client=client,
            storage_client=storage_client,
            bucket_name=args.bucket_name,
            force_overwrite=args.force_overwrite,
            table_type=args.table_type.lower(),
        )

        tables = get_tables(client, storage_client, args.source_dataset, args.table_type)
        if args.table_type == TABLE_TYPE_TABLE:
            extract_jobs = extract_tables(tables, bq_client=client, bucket_name=args.bucket_name)
            logger.info("Extracting jobs..")
            run_jobs_in_parallel(extract_jobs, trigger_job)
        logger.info("Uploading schemas..")
        run_jobs_in_parallel(tables, upload_schema, upload_args)
        logger.info("Uploading ddls..")
        run_jobs_in_parallel(tables, upload_ddl, upload_args)
        return 0
    finally:
        end = time.time()
        logger.info(f"Time elapsed: {end - start} seconds")
    return 1


extract_dataset_command = ParametrizedCommand(
    name="extract_dataset",
    description="Extracts the dataset from one project to GCS.",
    options=[
        Option("-i", "--source_dataset", help="Source dataset.", required=True, type=str),
        Option("-ip", "--source_project", help="Source project.", required=True, type=str),
        Option("-op", "--dest_project", help="Dest project.", required=True, type=str),
        Option("-gcsb", "--bucket_name", help="Dest bucket.", required=True, type=str),
        Option("-force", "--force_overwrite", help="Overwrite files.", default=False, type=bool),
        Option("-tt", "--table_type", help="Table type.",
               choices=[TABLE_TYPE_TABLE, TABLE_TYPE_VIEW],
               default=TABLE_TYPE_TABLE, type=str),
    ],
    run=lambda config, **kwargs: run(config)
)
