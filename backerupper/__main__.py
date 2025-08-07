import logging

from backerupper.conf import Config
from backerupper.sources.mysqldump import MySQLSource
from backerupper.uploaders.s3 import S3Uploader

_LOGGER = logging.getLogger()

def main():
    s3_client = S3Uploader(
        endpoint_url=Config.aws_endpoint_url(),
        aws_access_key_id=Config.aws_access_key_id(),
        aws_secret_access_key=Config.aws_secret_access_key(),
        region_name=Config.aws_s3_region_name(),
        bucket_name=Config.aws_s3_bucket_name()
    )

if __name__ == "__main__":
    logging.basicConfig(level=Config.log_level())
    main()
