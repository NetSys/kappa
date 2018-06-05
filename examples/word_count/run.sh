#!/usr/bin/env bash
# WARNING: this script CLEARS buckets SHUFFLE_BUCKET and OUTPUT_BUCKET.

set -e  # Quit on failure.

if [ "$#" -lt 2 ]; then
  echo "usage: INPUT_BUCKET=... SHUFFLE_BUCKET=... OUTPUT_BUCKET=... $0 num_mappers num_reducers"
  exit 1
fi

INPUT_BUCKET="${INPUT_BUCKET:?must be set}"  # S3 bucket containing input files.
# (Input chunks are S3 objects keyed 0, 1, ..., num_chunks-1 under $INPUT_BUCKET).
SHUFFLE_BUCKET="${SHUFFLE_BUCKET:?must be set}"  # S3 bucket to write intermediate results to.
OUTPUT_BUCKET="${OUTPUT_BUCKET:?must be set}"  # S3 bucket to write output files to.

num_mappers="$1"
num_reducers="$2"

num_chunks=`aws s3 ls ${INPUT_BUCKET} | wc -l`
echo "# input chunks: $num_chunks"

aws s3 rm --recursive s3://${SHUFFLE_BUCKET}
aws s3 rm --recursive s3://${OUTPUT_BUCKET}

# Transform the handler.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/../../compiler/do_transform.py < ${DIR}/word_count.py > ${DIR}/word_count_transformed.py

timestamp=$(date +%s)
log_file="word_count-$num_mappers-$num_reducers-$timestamp.log"

# Install the coordinator.
( cd ${DIR}/../../coordinator/cmd/coordinator; go install )
echo "Coordinator installed"

# Launch!
event="{\"num_chunks\": $num_chunks, \"num_mappers\": $num_mappers, \"num_reducers\": $num_reducers}"
unbuffer coordinator --platform=aws --rpc --no-logging \
  --name="word_count-$num_mappers-$num_reducers" --event="$event" \
  --env "INPUT_BUCKET=$INPUT_BUCKET" --env "SHUFFLE_BUCKET=$SHUFFLE_BUCKET" --env "OUTPUT_BUCKET=$OUTPUT_BUCKET" \
  ${DIR}/word_count_transformed.py ${DIR}/../../compiler/rt | tee ${log_file}

