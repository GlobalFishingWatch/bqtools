import typing
import logging
import time
from google.cloud import bigquery
from gfw.common.cli import Option, ParametrizedCommand

logger = logging.getLogger(__name__)
VIEWS_QUERY = """
SELECT concat(table_catalog, '.', table_schema, '.', table_name) name
FROM `{dataset}.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ("VIEW", "MATERIALIZED_VIEW", "EXTERNAL")
"""


def run(
    args: typing.List[str]
) -> int:
    start = time.time()
    client = bigquery.Client(project=args.source_project)
    views = client.query_and_wait(VIEWS_QUERY.format(dataset=args.source_dataset))
    logger.info(f"There are {views.total_rows} views.")
    for view in views:
        name = view.get("name")
        logger.info(f" - {name}")
    logger.info(f"Time demanded: {time.time() - start} sec.")
    return 0


list_views_in_dataset_command = ParametrizedCommand(
    name="list_views_in_dataset",
    description="Lists views inside the dataset.",
    options=[
        Option("-id", "--source_dataset", help="Source dataset.", required=True, type=str),
        Option("-ip", "--source_project", help="Source project.", required=True, type=str),
    ],
    run=lambda config, **kwargs: run(config)
)
