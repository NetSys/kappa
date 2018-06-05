#!/usr/bin/env bash
# Runs the benchmark, varying checkpoint size.
# The first argument is the log directory for this run; pass in - to have one
# randomly generated.

set -e  # Quit on failure.

if [ "$#" -lt 2 ]; then
  echo "usage: [NUM_ROUNDS=...] [NUM_SPAWNS=...] $0 run_dir chkpt_size..."
  exit 1
fi

: "${NUM_ROUNDS:=3}"  # How many times to run this benchmark.
: "${NUM_SPAWNS:=1000}"  # Spawn this many parallel tasks per run; mustn't exceed regional AWS Lambda concurrency limit.

# Transform the handler.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../../compiler/do_transform.py < ${DIR}/spawn_throughput.py > ${DIR}/spawn_throughput_transformed.py
echo "Handler code transformed"

# Install the coordinator.
( cd ${DIR}/../../coordinator/cmd/coordinator; go install )
echo "Coordinator installed"

run_dir=$1
shift 1
if [ "$run_dir" = "-" ]; then
  # Create directory for this run.
  timestamp=$(date +%s)
  run_dir="$DIR/spawn-throughput-run-$timestamp"
  mkdir "$run_dir"
fi
cd "$run_dir"
echo "Run directory: $run_dir"

for round in `seq 1 ${NUM_ROUNDS}`; do
  for chkpt_size in $@; do
    echo "Round = $round, checkpoint size = $chkpt_size MB"

    event="{\"num_spawns\": $NUM_SPAWNS, \"chkpt_size\": $chkpt_size}"
    timestamp=$(date +%s)
    log_file="spawn_throughput-$chkpt_size-$timestamp.log"
    coordinator --platform=aws --rpc --event="$event" \
      --name="spawn_throughput-$chkpt_size" --no-logging \
      "$DIR/spawn_throughput_transformed.py" "$DIR/../../compiler/rt" \
      2>&1 | tee ${log_file}
  done
done
