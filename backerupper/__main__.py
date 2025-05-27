from .s3 import S3
from .conf import Config

def main():
    s3_client = S3(
        endpoint_url=Config.aws_endpoint_url(),
        access_key=Config.aws_access_key_id(),
        secret_key=Config.aws_secret_access_key(),
        aws_region=Config.aws_s3_region_name()
    )

    buckets = s3_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(f"- {bucket.name} (Created on: {bucket.creation_date})")

if __name__ == "__main__":
    main()
