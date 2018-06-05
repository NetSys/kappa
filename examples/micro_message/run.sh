#!/usr/bin/env bash
set -e  # Quit on failure.

: "${NUM_ROUNDS:=4}"  # How many times to run this benchmark.
: "${NUM_MESSAGES:=25}"  # How many messages to send in each run.
task="micro_message"

if [ "$#" -lt 1 ]; then
  echo "usage: [NUM_ROUNDS=...] [NUM_MESSAGES=...] $0 msg_sizes..."
  exit 1
fi

# Transform the handler.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../../compiler/do_transform.py < "$DIR/$task.py" > "$DIR/${task}_transformed.py"
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
  for msg_size in $@; do
    echo "Round = $round, message size = $msg_size MB"

    event="{\"message_size\": $msg_size, \"num_messages\": $NUM_MESSAGES}"
    coordinator --platform=aws --rpc --event="$event" \
      --name="$task-$msg_size" --rpc-timeout 30 \
      "$DIR/${task}_transformed.py" "$DIR/../../compiler/rt"
  done
done
