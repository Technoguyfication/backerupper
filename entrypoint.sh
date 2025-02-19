#!/bin/bash
set -e

if [[ -z "$COMMAND" && -z "$DIRECTORY" && -z "$FILE" ]]; then
    echo "No COMMAND, DIRECTORY, or FILE found in environment. Exactly one of these must be set as a backup target. Exiting."
    exit 1
fi

if [[ -z "$AWS_BUCKET_NAME" ]]; then
    echo "No S3 bucket specified. Exiting."
    exit 1
fi

if [[ -z "$FILE_FORMAT" ]]; then
    FILE_FORMAT='$(date +%Y%m%d-%H%M)'
fi

OBJECT_NAME=$(eval echo "$FILE_FORMAT")

echo "Running command: $COMMAND"
echo "Uploading result to $AWS_BUCKET_NAME/$OBJECT_NAME"

$COMMAND | python3 s3upload.py --bucket $AWS_BUCKET_NAME --key $OBJECT_NAME
