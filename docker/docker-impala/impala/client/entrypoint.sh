#!/bin/bash
set -euo pipefail
set -x

if [[ $# -eq 0 ]]; then
  echo "Must provide at least one argument."
  exit 1
elif [[ "$1" = "init_data" ]]; then
  echo "Loading Init Data"

  IMPALA_SHELL="impala-shell --protocol=hs2 -i ${IMPALAD:-impalad:21050}"

  # Wait until Impala comes up (it started in parallel with the data loader).
  for i in $(seq 30); do
    if ${IMPALA_SHELL} -q 'select version()'; then
      break
    fi
    echo "Waiting for impala to come up"
    sleep ${INTERVAL:-10}
  done

  ${IMPALA_SHELL} -f ${INIT_SQL}
elif [[ "$1" = "impala-shell" ]]; then
  shift
  # Execute impala-shell with any extra arguments provided.
  exec impala-shell --protocol=hs2 --history_file=/tmp/impalahistory -i ${IMPALAD:-impalad:21050} "$@"
else
  # Execute the provided input as a command
  exec "$@"
fi