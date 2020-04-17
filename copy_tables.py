"""
Example Usage
-------------

    $ python copy_tables.py 'machine_learning_dev_ttl_120d.vc_v20200124_results_*' gfw_research_precursors

This copies all tables that match the given wildcard to `0_ttl24h` dataset.
 

Caveats
-------

Annoyingly slow.

"""
import argparse
from collections import Counter
import fnmatch
import re
import sys
import subprocess

try:
    input = raw_input
except NameError:
    pass

# Non futures version, so should work with vanilla Python
from multiprocessing.pool import ThreadPool
def run_commands_in_parallel(commands, max_threads=32):
    n_threads = min(max_threads, len(commands))
    pool = ThreadPool(n_threads)
    pool.map(subprocess.check_call, commands)
    pool.close()


def list_tables(pattern, max_tables_to_match):
    if '.' in pattern:
        dataset, table_pattern = pattern.split('.')
    else:
        dataset = pattern
        table_pattern = '*'
    command = ['bq', 'ls', '-n', str(max_tables_to_match), dataset]
    try:
        raw_output = subprocess.check_output(command).decode('utf8')
    except subprocess.CalledProcessError as err:
        if err.returncode == 2:
            print("{}: {}: No such dataset or table".format(
                    sys.argv[0], pattern))
            sys.exit(2)
        raise err
    all_tables = [x.strip().split()[0] 
        for x in raw_output.strip().split('\n')[2:] if x.split()[1].endswith('TABLE')]

    return ['{}.{}'.format(dataset, x) for x in fnmatch.filter(all_tables, table_pattern)]


def copy_tables(source_tables, target_dataset):
    commands = []
    for src in source_tables:
        src_ds, src_tbl = src.split('.')
        tgt = '{}.{}'.format(target_dataset, src_tbl)
        commands.append(['bq', 'cp', '--no_clobber', src, tgt])
    run_commands_in_parallel(commands)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source_tables",  help="table names to copy (supports wildcards)")
    parser.add_argument("target_dataset", help="directory to copy")
    parser.add_argument("-N", "--max_tables_to_match", 
                        help="maximum number of tables to list (includes individual shards)",
                        default=1000000) 
    args = parser.parse_args()
    sources = list_tables(args.source_tables, args.max_tables_to_match)
    copy_tables(sources, args.target_dataset)


