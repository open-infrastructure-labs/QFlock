#!/bin/bash

if [ $# -ne 2 ]; then
  echo "sum_stats.sh column_name file"
  exit 1
fi
grep $1 $2 | cut -d'|' -f3 | paste -sd+ - | bc
