import random

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

    print("listing objects in bucket:")
    objects = s3_client.list_objects()
    for obj in objects:
        print(f"\t{obj.key} ({obj.size} bytes) - {obj.created}")

    print("listing incomplete uploads:")
    uploads = s3_client.list_incomplete_uploads()
    for upload in uploads:
        print(f"\t{obj.key} - {obj.created}")

    upload = s3_client.create_streaming_upload(f'test-upload-{random.randint(100, 1000)}')
    for i in range(10):
        # create 10MB chunk of data
        data = bytes([i] * int(10e6))
        part_num = upload.upload_chunk(data)
        print(f"part {part_num} uploaded")
    
    completed = upload.complete_upload()
    print(f"Completed multipart upload of {completed.key}")

if __name__ == "__main__":
    main()
