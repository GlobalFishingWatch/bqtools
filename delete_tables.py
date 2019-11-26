"""
Example Usage:

    $ python delete_tables.py machine_learning_dev_ttl_30d.test_dataflow_2016_01_01_current2*
    Found the following matching tables:
    test_dataflow_2016_01_01_current20120101
    test_dataflow_2016_01_01_current20120102
    test_dataflow_2016_01_01_current20120103
    test_dataflow_2016_01_01_current20120104
    test_dataflow_2016_01_01_current20120105
    ...2166 tables
    test_dataflow_2016_01_01_current20171211
    test_dataflow_2016_01_01_current20171212
    test_dataflow_2016_01_01_current20171213
    test_dataflow_2016_01_01_current20171214
    test_dataflow_2016_01_01_current20171215
    Delete tables (y/N)?y



"""
import argparse
import fnmatch
import sys
import subprocess

try:
    input = raw_input
except NameError:
    pass

# # Note need to `pip install futures` to get the concurrent.futures module
# from concurrent.futures import ThreadPoolExecutor
# def run_commands_in_parallel(commands, max_threads=32):
#     with ThreadPoolExecutor(min(max_threads, len(commands))) as exc:
#         for cmd in commands:
#             exc.submit(subprocess.check_call, cmd)

# Non futures version, so should work with vanilla Python
from multiprocessing.pool import ThreadPool
def run_commands_in_parallel(commands, max_threads=32):
    n_threads = min(max_threads, len(commands))
    pool = ThreadPool(n_threads)
    pool.map(subprocess.check_call, commands)
    pool.close()


if __name__ == "__main__":
    # TODO: use argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", 
                        help="table name to delete (supports wildcards)")
    parser.add_argument("-f", "--force", 
                        help="delete tables without asking",
                        action="store_true")
    parser.add_argument("-q", "--quiet", 
                        help="don't list tables that match pattern",
                        action="store_true")
    parser.add_argument("-n", "--max_tables_to_list", 
                        help="maximum number of matching tables to list",
                        default=10) 
    parser.add_argument("-N", "--max_tables_to_match", 
                        help="maximum number of tables to match and delete",
                        default=1000000) 
    args = parser.parse_args()
    pattern = args.pattern
    pattern = sys.argv[1]
    dataset, table_pattern = pattern.split('.')
    command = ['bq', 'ls', '-n', str(args.max_tables_to_match), dataset]
    raw_output = subprocess.check_output(command).decode('utf8')
    all_tables = [x.strip().split()[0] 
        for x in raw_output.strip().split('\n')[2:] if x.strip().endswith('TABLE')]
    tables = fnmatch.filter(all_tables, table_pattern)
    if len(tables) == 0:
        print('No matching tables')
        raise SystemExit(1)
    if not args.quiet:
        print("Found the following matching tables:")
        if len(tables) <= args.max_tables_to_list:    
            for x in tables:
                print(x)
        else:
            n = args.max_tables_to_list // 2
            for x in tables[:n]:
                print(x)
            print('...{} tables'.format(len(tables) - args.max_tables_to_list))
            for x in tables[-(args.max_tables_to_list - n):]:
                print(x)
    if args.force or input("Delete tables (y/N)?").upper() == 'Y':
        full_names = ['{}.{}'.format(dataset, x) for x in tables]
        commands = [['bq', 'rm', '-f', x] for x in full_names]
        run_commands_in_parallel(commands)