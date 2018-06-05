#!/usr/bin/env bash

set -e  # Quit on failure.

if [ "$#" -lt 1 ]; then
  echo "usage: BUCKET=... $0 num_workers"
  exit 1
fi

BUCKET="${BUCKET:?Need to set BUCKET}"  # S3 bucket containing input files.

num_workers="$1"

num_chunks=`aws s3 ls ${BUCKET} | wc -l`
echo "# input chunks: $num_chunks"

# Transform the handler.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../../compiler/do_transform.py < ${DIR}/streaming.py > ${DIR}/streaming_transformed.py

timestamp=$(date +%s)
log_file="streaming_$timestamp.log"

# Install the coordinator.
( cd ${DIR}/../../coordinator/cmd/coordinator; go install )
echo "Coordinator installed"

# Launch!
event="{\"num_chunks\": $num_chunks, \"num_workers\": $num_workers}"
unbuffer coordinator --platform=aws --rpc --no-logging \
  --name="streaming" --event="$event" --env "BUCKET=$BUCKET"\
  ${DIR}/streaming_transformed.py ${DIR}/../../compiler/rt | tee ${log_file}


