# The script will calclulate how much a dataset tables costs in GB.
# Then it gets the tables that the dataset has.
# Then it runs the copy in a separate project
# Then compares the amoun of Gb between the source and destination dataset.
import argparse
import logging
import sys
import typing
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig, CopyJobConfig


logger = logging.getLogger(__name__)

WORLD_FISHING_827 = "world-fishing-827"
GFW_INT_VMS_SOURCES = "gfw-int-vms-sources"

COSTS_QUERY = """select dataset_id, sum(size_bytes) total_bytes,
round(sum(size_bytes) / pow(1024, 3),2) total_gb from `{dataset}.__TABLES__`
group by 1"""

TABLES_PATTERN = "[0-9]{8}$"
PRESENT_TABLES_QUERY = """select if(regexp_contains(table_id, "{pattern}"),
regexp_replace(table_id, "{pattern}", "*"), table_id) as table_name from
`{args.source_dataset}.__TABLES__` group by 1"""



def get_costs(client, job_config, dataset):
    query = COSTS_QUERY.format(dataset=dataset)
    result = client.query_and_wait(query, job_config=job_config)
    size=0
    for r in result:
        size = r.get('total_bytes','NOT-KEY')
        logger.info(list(r.items()))
    return size



def present_tables(client, job_config, args):
    query = PRESENT_TABLES_QUERY.format(args=args, pattern=TABLES_PATTERN)
    result = client.query_and_wait(query, job_config=job_config)
    logger.info("Present tables to be copied:")
    for r in result:
        logger.info(list(r.values()))


def get_tables(client, job_config, dataset) -> typing.Tuple:
    table_result=[]
    not_table_result=[]
    items = client.list_tables(dataset=dataset)
    for t in items:
        if t.table_type == "TABLE":
            table_result.append(t.full_table_id)
        else:
            not_table_result.append(t.full_table_id)
    logger.info(f"List tables from dataset {dataset} <Tables: {len(table_result)}, Not-tables: {len(not_table_result)}>")
    return table_result, not_table_result

# from multiprocessing.pool import ThreadPool
# def run_commands_in_parallel(jobs, max_threads=32):
#     n_threads = min(max_threads, len(jobs))
#     pool = ThreadPool(n_threads)
#     pool.map(lambda job: job.result(), jobs)
#     pool.close()

def copy_tables(client, tables, source, dest):
    for full_table_id in tables:
        dest_table = f"{dest}.{full_table_id.split('.')[1]}"
        # job = client.copy_table(
        #     full_table_id
        #     dest_table,
        #     job_config=CopyJobConfig(create_disposition="CREATE_IF_NEEDED")
        # )
        # job.result()
        logger.info(f"= SINGLE TABLE COPIED {full_table_id} -> {dest_table}")


def list_not_tables(items):
    for item in items:
        logger.info(f"Not a table: ", item)


def are_costs_clear(client, job_config, source, dest) -> bool:
    logger.info("Getting costs in source and destionation datasets:")
    logger.info(f"## Dataset: {source}")
    src_bytes = get_costs(client, job_config, source)
    logger.info(f"## Dataset: {dest}")
    dest_bytes = get_costs(client, job_config, dest)
    if src_bytes == dest_bytes:
        logger.info(f"### COPY {source} -> {dest} COMPLETELY SUCCESS")
    else:
        logger.error(f"See differences in bytes ({src_bytes-dest_bytes})")
    return src_bytes == dest_bytes


def run(args):
    client = bigquery.Client(project=args.source_project)
    job_config = QueryJobConfig(
        # dry_run=True,
        dry_run=False,
        use_legacy_sql=False,
    )
    source = f"{args.source_project}.{args.source_dataset}"
    dest = f"{args.dest_project}.{args.dest_dataset}"

    present_tables(client, job_config, args)
    tables, not_tables = get_tables(client, job_config, source)
    copy_tables(client, tables, source, dest)

    clear_costs = are_costs_clear(client, job_config, source, dest)
    if not clear_costs:
        dest_tables, dest_not_tables = get_tables(client, job_config, dest)
        logger.info(f"tables <source:{len(tables)}, dest:{len(dest_tables)}>, not_tables <source:{len(not_tables)}, dest:{len(dest_not_tables)}>")
        diff = set(map(lambda x: x.split('.')[1], tables)).difference(map(lambda x: x.split('.')[1], dest_tables))
        logger.info(f"Table that was not copied yet: {diff}")

    list_not_tables(not_tables)
    return 0





def cli(args):
    parser = argparse.ArgumentParser(description='Copies the dataset from one project to another.')
    parser.add_argument('-i','--source_dataset', help='Source dataset.', required=True, type=str)
    parser.add_argument('-ip','--source_project', help='Source project.', default=WORLD_FISHING_827, type=str)
    parser.add_argument('-o','--dest_dataset', help='Dest dataset.', required=True, type=str)
    parser.add_argument('-op','--dest_project', help='Dest project.', default=GFW_INT_VMS_SOURCES, type=str)
    args = parser.parse_args()
    return run(args)

def main():
    logging.basicConfig(
        # filename='copy_dataset.log',
        level=logging.INFO,
        format="%(levelname)s %(asctime)s - %(message)s"
    )
    logger.info('Started')
    r = cli(sys.argv[1:])
    logger.info('Finished')
    sys.exit(0)

if __name__ == "__main__":
    main()
