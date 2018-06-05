#!/usr/bin/env bash
# Runs the grep application.
# The first argument is the log directory for this run; pass in - to have one randomly generated.
# For example,
#     BUCKET=my-bucket NUM_CHUNKS=256 WORD=foo ./run.sh - 10 20 30
# does three runs of the application, using 10, 20, and 30 workers, respectively.

set -e  # Quit on failure.

if [ "$#" -lt 2 ]; then
  echo "usage: [NUM_ROUNDS=...] BUCKET=... NUM_CHUNKS=... WORD=... $0 run_dir num_worker..."
  exit 1
fi

BUCKET="${BUCKET:?Need to set BUCKET}"  # S3 bucket containing input files.
NUM_CHUNKS="${NUM_CHUNKS:?Need to set NUM_CHUNKS}"  # Number of input chunks
WORD="${WORD:?Need to set WORD}"  # Word to grep for.
# (Input chunks are S3 objects keyed 0, 1, ..., num_chunks-1 under $BUCKET).

: "${NUM_ROUNDS:=1}"  # How many times to run the program.  Useful for benchmarking.

# Transform the handler.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../../compiler/do_transform.py < ${DIR}/grep.py > ${DIR}/grep_transformed.py
echo "Handler code transformed"

# Install the coordinator.
( cd ${DIR}/../../coordinator/cmd/coordinator; go install )
echo "Coordinator installed"

run_dir=$1
shift 1
if [ "$run_dir" = "-" ]; then
  # Create directory for this run.
  timestamp=$(date +%s)
  run_dir="$DIR/grep-$timestamp"
  mkdir "$run_dir"
fi
cd "$run_dir"
echo "Run directory: $run_dir"

for round in `seq 1 ${NUM_ROUNDS}`; do
  for num_workers in $@; do
      echo "Round = $round, num_workers = $num_workers"

      event="{\"bucket\": \"$BUCKET\", \"word\": \"$WORD\", \"num_chunks\": $NUM_CHUNKS, \"num_workers\": $num_workers}"
      coordinator --platform=aws --rpc --event="$event" \
        --name="grep-$num_workers" \
        "$DIR/grep_transformed.py" "$DIR/../../compiler/rt"
  done
done
