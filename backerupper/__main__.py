import random

from backerupper.s3 import S3
from backerupper.conf import Config

def main():
    s3_client = S3(
        endpoint_url=Config.aws_endpoint_url(),
        access_key=Config.aws_access_key_id(),
        secret_key=Config.aws_secret_access_key(),
        aws_region=Config.aws_s3_region_name()
    )

    # buckets = s3_client.list_buckets()
    # print("Buckets:")
    # for bucket in buckets:
    #     print(f"- {bucket.name} (Created on: {bucket.creation_date})")

    # objects = s3_client.list_objects(bucket_name=Config.aws_s3_bucket_name())
    # for obj in objects:
    #     print(f"Object: {obj}")

    # load object from disk an upload to s3

    # with open(r"C:\Users\Hayden\Downloads\Baked-Beans-22.png", 'rb') as file:
    #     all_bytes = file.read()
    #     s3_client.put_object(Config.aws_s3_bucket_name(), 'beans.png', all_bytes)

    # s3_client.delete_object(Config.aws_s3_bucket_name(), 'beans.png')

    upload = s3_client.create_multipart_upload(Config.aws_s3_bucket_name(), f'test-upload-{random.randint(100, 1000)}')
    for i in range(10):
        # create 10MB chunk of data
        data = bytes([i] * int(10e6))
        part_num, etag = upload.upload_part(data)
        print(f"part {part_num} uploaded, etag: {etag}")
    
    completed = upload.complete()
    print(f"Completed multipart upload, etag: {completed.etag}")

if __name__ == "__main__":
    main()
