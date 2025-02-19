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
    FILE_FORMAT='$(date +%Y%m%d-%H%M%S)'
fi

if [[ -n "$COMMAND" ]]; then
    TASK="$COMMAND | %s"
elif [[ -n "$DIRECTORY" ]]; then
    TASK="tar -c \"$DIRECTORY\" | %s"
elif [[ -n "$FILE" ]]; then
    TASK="%s < \"$FILE\""
fi

# create job runner
echo "#!/bin/bash" > backup.sh
echo "FILE_FORMAT=$FILE_FORMAT" >> backup.sh
echo "OBJECT_NAME=\$(eval echo \"\$FILE_FORMAT\")" >> backup.sh
echo "UPLOAD_COMMAND=\"python3 s3upload.py --bucket $AWS_BUCKET_NAME --key \$OBJECT_NAME\"" >> backup.sh
echo "EVAL_TASK=\$(printf \"$TASK\" \"\$UPLOAD_COMMAND\")" >> backup.sh
echo "echo \"\$EVAL_TASK\"" >> backup.sh
echo "eval \"\$EVAL_TASK\"" >> backup.sh
chmod +x backup.sh

if [[ -z "$SCHEDULE" ]]; then
    # no schedule defined; run the job now
    ./backup.sh
else
    # generate a crontab file and start supercronic
    echo "$SCHEDULE $PWD/backup.sh" > crontab
    /usr/local/bin/supercronic crontab
fi