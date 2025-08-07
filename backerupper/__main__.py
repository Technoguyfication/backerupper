import random
import threading
import subprocess

from backerupper.uploaders.s3 import S3Uploader
from backerupper.conf import Config

def main():
    s3_client = S3Uploader(
        endpoint_url=Config.aws_endpoint_url(),
        aws_access_key_id=Config.aws_access_key_id(),
        aws_secret_access_key=Config.aws_secret_access_key(),
        region_name=Config.aws_s3_region_name(),
        bucket_name=Config.aws_s3_bucket_name()
    )




if __name__ == "__main__":
    main()
