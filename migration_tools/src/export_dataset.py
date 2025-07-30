import typing
import logging
import json
from gfw.common.cli import Option, ParametrizedCommand
from google.cloud import bigquery
from google.cloud import storage


logger = logging.getLogger(__name__)


JOB_PREFIX = "migrationtools_extract_dataset"
DDL_QUERY = """SELECT table_name, table_type, ddl FROM `{dataset}.INFORMATION_SCHEMA.TABLES`"""


def upload_blob(
    gcs: storage.Client,
    bucket_name: str,
    source_data: str,
    destination_blob_name: str,
    force: bool
):
    """
    Uploads data to the bucket.

    Args:
        gcs: storage client.
        bucket_name: Name of the gcs bucket.
        source_data: what we want to store.
        destination_blob_name: gcs destination.
        force: override and do not check precondition."""
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    # For a destination object that does not yet exist set if_generation_match precondition to 0.
    generation_match_precondition = None if force else 0
    blob.upload_from_string(source_data, if_generation_match=generation_match_precondition)
    logger.info(f"Data was uploaded to gs://{bucket_name}/{destination_blob_name}.")


def get_tables(
    client: bigquery.Client,
    job_config: bigquery.QueryJobConfig,
    dataset: bigquery.DatasetReference
) -> typing.Tuple:
    """
    Collects the simple tables, partitioned tables and views.

    Args:
        client: Bigquery Client.
        job_config: Bigquery QueryJobConfig.
        dataset: The project and dataset (ex: project.dataset)

    Returns:
        Two list as tuple: tables and views."""
    tables = []
    views = []
    query = DDL_QUERY.format(dataset=dataset.dataset_id)
    logger.info(f"query: {query}")
    rows = client.query_and_wait(query, job_config=job_config)
    for row in rows:
        t = client.get_table(f"{dataset.project}.{dataset.dataset_id}.{row.table_name}")
        if row.table_type == "VIEW":
            views.append((row.table_name, t.schema, row.ddl))
        else:
            tables.append((row.table_name, t.schema, row.ddl))
    logger.info(f"List tables from dataset {dataset.path} "
                f"<Simple-Tables: {len(tables)}, Views: {len(views)}>")
    return tables, views


def extract_tables(
    client: bigquery.Client,
    gcs: storage.Client,
    tables: typing.List[typing.Tuple],
    source: bigquery.DatasetReference,
    backet_name: str,
    force: bool
):
    """
    Extracts the table content, the schema and the ddl.

    Args:
        client: bigquery client.
        gcs: storage client.
        tables: list of tables.
        source: dataset source.
        backet_name: name of the backet.
        force: override upload."""
    for table in tables:
        table_name, schema, ddl = table
        backet_path = f"{source.project}/{source.dataset_id}/{table_name}"
        gcs_path = f"gs://{backet_name}/{backet_path}/table.AVRO"
        extract = client.extract_table(
            f"{source.project}.{source.dataset_id}.{table_name}",
            f"{gcs_path}",
            location="US",
            job_id_prefix=JOB_PREFIX,
            job_config=bigquery.job.ExtractJobConfig(
                destination_format=bigquery.DestinationFormat.AVRO,
                use_avro_logical_types=True
            )
        )
        logger.info(f"Table {table_name} extracted to {gcs_path}")
        upload_blob(gcs, backet_name, json.dumps([x.to_api_repr() for x in schema]), f"{backet_path}/schema.sql", force)
        upload_blob(gcs, backet_name, ddl, f"{source.project}/{source.dataset_id}/{table_name}/ddl.sql", force)




def run(
    args: typing.List[str]
) -> int:
    """
    Run the copy of tables from a source dataset to a destination dataset.

    Args:
        args: The arguments to run the copy.

    Returns:
        Number if the process ended well or not."""
    client = bigquery.Client(project=args.source_project)
    job_config = bigquery.QueryJobConfig(
        dry_run=args.dry_run,  # To estimate costs.
        priority="BATCH",
        use_legacy_sql=False,
        use_query_cache=False,
    )
    dataset_src = bigquery.DatasetReference(args.source_project, args.source_dataset)

    tables, views = get_tables(client, job_config, dataset_src)

    storage_client = storage.Client(project=args.source_project)
    extract_tables(client, storage_client, tables, dataset_src, args.backet_name, args.force_override)
    extract_tables(client, storage_client, views, dataset_src, args.backet_name, args.force_override)
    return 0


export_dataset_command = ParametrizedCommand(
    name="export_dataset",
    description="Exports the dataset from one project to GCS.",
    options=[
        Option("-i", "--source_dataset", help="Source dataset.", required=True, type=str),
        Option("-ip", "--source_project", help="Source project.", required=True, type=str),
        Option("-gcsb", "--backet_name", help="Dest backet.", required=True, type=str),
        Option("-force", "--force_override", help="Force the override of the files.", default=False, type=bool),
    ],
    run=lambda config, **kwargs: run(config)
)
