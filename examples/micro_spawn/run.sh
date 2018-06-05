#!/usr/bin/env bash
set -e  # Quit on failure.

if [ "$#" -lt 1 ]; then
  echo "usage: [NUM_ROUNDS=...] [NUM_SPAWNS=...] $0 chkpt_sizes..."
  exit 1
fi

: "${NUM_ROUNDS:=3}"  # How many times to run this benchmark.
: "${NUM_SPAWNS:=100}"  # How many spawns per run.

# Transform the handler.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../../compiler/do_transform.py < ${DIR}/micro_spawn.py > ${DIR}/micro_spawn_transformed.py
echo "Handler code transformed"

# Install the coordinator.
( cd ${DIR}/../../coordinator/cmd/coordinator; go install )
echo "Coordinator installed"

# Create directory for this run.
timestamp=$(date +%s)
run_dir="run-$timestamp"
mkdir "$DIR/$run_dir"
cd "$DIR/$run_dir"
echo "Run directory created: $run_dir"

for round in `seq 1 ${NUM_ROUNDS}`; do
  for chkpt_size in $@; do
    echo "Round = $round, Checkpoint size = $chkpt_size MB"

    event="{\"chkpt_size\": $chkpt_size, \"num_spawns\": $NUM_SPAWNS}"
    coordinator --platform=aws --rpc --event="$event" \
      --name="micro-spawn-$chkpt_size" \
      "$DIR/micro_spawn_transformed.py" "$DIR/../../compiler/rt"
  done
done
