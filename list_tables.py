"""
Example Usage
-------------

    $ python list_tables.py pipe_production_v20190502
    classify_logistic_[2883]
    encounters
    features_[2882]
    fishing_score_[2884]
    messages_scored_[2883]
    messages_segmented_[2884]
    port_events_[2883]
    port_visits_[2883]
    position_messages_[2883]
    published_events_encounters
    published_events_fishing
    published_events_gaps
    published_events_ports
    raw_encounters_[2883]
    raw_encounters_neighbors_[2883]
    raw_vbd_global
    research_aggregated_ids
    research_aggregated_segments
    research_fishing_daily
    research_ids_daily
    research_messages
    research_satellite_timing
    research_segments_daily
    research_stats
    segment_identity_daily_[2883]
    segment_info
    segment_vessel
    segment_vessel_daily_[2883]
    segments_[2884]
    spatial_measures_[1]
    vessel_info
    voyages

You can use the `-r` flag to disable combining shards, and 
basic globbing is supports. So, we can figure out what is going
on with `spatial_measures`.

    $ python list_tables.py -r pipe_production_v20190502.spatial_measures*
    spatial_measures_20181025

Looks like it doesn't conform to our usual `_Vyyyymmdd` method of versioning.


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



def normalize(x):
    x = x.strip()
    match = re.match("(.*_)\d\d\d\d\d\d\d\d$", x)
    if match:
        return match.group(1)
    # A common mispelling
    match = re.match("(.*_[vV]\d\d\d\d\d\d\d\d.?)\d\d\d\d\d\d\d\d$", x)
    if match:
        return match.group(1)
    return x + '$' 

def counted(names):
    cntr = Counter([normalize(x) for x in names])
    return cntr.most_common()
        
def list(names):
    for name, count in sorted(counted(names)):
        if name.endswith('$'):
            assert count == 1
            yield name[:-1]
        else:
            yield "{}[{}]".format(name, count)


def list_datasets(args):
    command = ['bq', 'ls', '-n', str(args.max_tables_to_match)]
    raw_output = subprocess.check_output(command).decode('utf8')
    for line in raw_output.strip().split('\n')[2:]:
        dataset = line.strip()
        print(dataset)

def list_tables(args):
    if '.' in args.pattern:
        dataset, table_pattern = args.pattern.split('.')
    else:
        dataset = args.pattern
        table_pattern = '*'
    command = ['bq', 'ls', '-n', str(args.max_tables_to_match), dataset]
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
    tables = fnmatch.filter(all_tables, table_pattern)

    if not args.raw:
        tables = list(tables)
    for x in tables:
        print(x)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", nargs='?', default=None,
                        help="table name to list (supports wildcards)")
    parser.add_argument("-r", "--raw", action='store_true',
                        help="Don't combine date sharded tables") 
    parser.add_argument("-N", "--max_tables_to_match", 
                        help="maximum number of tables to list (includes individual shards)",
                        default=1000000) 
    args = parser.parse_args()
    if args.pattern is None:
        list_datasets(args)
        sys.exit()
    else:
        list_tables(args)


