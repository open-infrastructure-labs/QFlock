#!/usr/bin/python3
import sys
import subprocess
import time

HELP = """ run spark_bench.py inside a docker.
     docker-bench.py <args>
    examples:
    - initialize (includes, generate tpc database, generate parquet, create catalog, compute stats
    - --log_level INFO recommended so you can view progress.
    ./docker-bench.py --init --log_level INFO
    ./docker-bench.py --generate --gen_parquet
    ./docker-bench.py --create_catalog --view_catalog
    ./docker-bench.py --compute_stats
    ./docker-bench.py --view_catalog
    ./docker-bench.py --view_catalog --verbose
    ./docker-bench.py --view_columns "*"
    ./docker-bench.py --view_columns "*" --verbose
    ./docker-bench.py --view_columns "web_site.*""
    ./docker-bench.py --view_columns "web_site.*"" --verbose
    ./docker-bench.py --view_columns "*.quantity"
    ./docker-bench.py --view_columns "*.name"
    ./docker-bench.py --explain --query_range 3 --log_level INFO
    ./docker-bench.py --explain --query_range "*"
    ./docker-bench.py --queries "*"
    ./docker-bench.py --queries "3" --log_level INFO
    ./docker-bench.py --queries "3,5-20,28,55"
    ./docker-bench.py --query_text "select cc_street_name,cc_city,cc_state from call_center" --verbose
    """

if __name__ == "__main__":
    if len(sys.argv) == 1 or sys.argv[1].casefold() == "-h" or sys.argv[1].casefold() == "--help":
        print(HELP)
        exit(1)
    arg_string = " ".join(sys.argv[1:])
    cmd = "docker exec -it sparklauncher-qflock ./spark_bench.py " + arg_string
    print(cmd)
    start_time = time.time()
    subprocess.call(cmd, shell=True)
    delta_time = time.time() - start_time
    print(f"total seconds: {delta_time}")