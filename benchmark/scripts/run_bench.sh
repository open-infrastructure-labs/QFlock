#!/bin/bash

echo "time docker exec -it sparklauncher-qflock ./spark_bench.py $@"
time docker exec -it sparklauncher-qflock ./spark_bench.py "$@"

# Create both the raw data file and the parquet files.
#./run_bench.sh --generate --gen_parquet
#./run_bench.sh --create_catalog --view_catalog
#./run_bench.sh --compute_stats
#./run_bench.sh --view_catalog
#./run_bench.sh --view_catalog --verbose
#./run_bench.sh --view_columns "*"
#./run_bench.sh --view_columns "*" --verbose
#./run_bench.sh --view_columns "web_site.*""
#./run_bench.sh --view_columns "web_site.*"" --verbose
#./run_bench.sh --view_columns "*.quantity"
#./run_bench.sh --view_columns "*.name"
#./run_bench.sh --explain --query_range 3 --log_level INFO
#./run_bench.sh --explain --query_range "*"
#./run_bench.sh --queries "*"
#./run_bench.sh --queries "3" --log_level INFO
#./run_bench.sh --queries "3,5-20,28,55"
#./run_bench.sh --query_text "select cc_street_name,cc_city,cc_state from call_center" --verbose
