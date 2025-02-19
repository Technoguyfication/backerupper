#!/usr/bin/env python3

import io
import os
import sys
import argparse
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

BUFFER_SZE = 5 * 1024 * 1024    # 5MB

def main():
    parser = argparse.ArgumentParser(
        description="Stream data from stdin to an S3 object using multipart upload"
    )
    parser.add_argument("--bucket", required=True, help="Name of the S3 bucket")
    parser.add_argument("--key", required=True, help="Key for the S3 object")
    parser.add_argument("--profile", help="AWS profile name (optional)", default=None)
    args = parser.parse_args()

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", None)
    path_style_buckets = bool(os.environ.get("AWS_USE_PATH_STYLE_BUCKETS", False))
    boto_config = Config(s3={'addressing_style': 'path' if path_style_buckets else 'auto'})

    # Optionally honor a specific AWS profile (otherwise uses default credentials).
    if args.profile:
        session = boto3.session.Session()
        s3_client = session.client("s3", profile_name=args.profile, endpoint_url=endpoint_url, config=boto_config)
    else:
        s3_client = boto3.client("s3", endpoint_url=endpoint_url, config=boto_config)

    # TransferConfig for controlling multipart parameters
    transfer_config = TransferConfig(
        multipart_threshold=BUFFER_SZE,
        multipart_chunksize=BUFFER_SZE,
        # max_concurrency=4,
        # use_threads=True
    )

    # Stream binary data from stdin to S3
    s3_client.upload_fileobj(
        Fileobj=io.BufferedReader(sys.stdin.buffer, BUFFER_SZE),
        Bucket=args.bucket,
        Key=args.key,
        Config=transfer_config
    )

    print(f"{s3_client._endpoint.host}/{args.bucket}/{args.key}")

if __name__ == "__main__":
    main()
