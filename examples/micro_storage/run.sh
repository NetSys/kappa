#!/usr/bin/env bash
# For example,
#     BUCKET=my-bucket ./micro_storage.sh 0.1 1 10
# runs the benchmark for write sizes 0.1 MB, 1 MB, and 10 MB.
# WARNING: this script will CLEAR the bucket after writing files to it.

set -e  # Quit on failure.

task="micro_storage"

if [ "$#" -lt 1 ]; then
  echo "usage: BUCKET=... [NUM_ROUNDS=...] [NUM_WRITES=...] $0 write_sizes..."
  exit 1
fi

BUCKET="${BUCKET:?Need to set BUCKET}"  # S3 bucket to write files to.
: "${NUM_ROUNDS:=3}"  # How many times to run this benchmark.
: "${NUM_WRITES:=10}"  # How many writes per run.

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
  for write_size in $@; do
    echo "Round = $round, write size = $write_size MB"

    event="{\"write_size\": $write_size, \"bucket\": \"$BUCKET\", \"num_writes\": $NUM_WRITES}"
    coordinator --platform=aws --rpc --event="$event" \
      --name="$task-$write_size" --env TEMP_BUCKET="$BUCKET" \
      --rpc-timeout 30 \
      "$DIR/${task}_transformed.py" "$DIR/../../compiler/rt"

    aws s3 rm --recursive "s3://$BUCKET"
  done
done
