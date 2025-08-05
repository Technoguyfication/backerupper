from datetime import datetime
import boto3
import threading

from backerupper.uploaders.abstract import (
    AbstractUploader,
    AbstractStreamingUpload,
    ObjectData,
    IncompleteStreamingUpload
)

class S3Uploader(AbstractUploader):
    def __init__(
            self, 
            bucket_name: str, 
            region_name: str = None, 
            aws_access_key_id: str | None = None, 
            aws_secret_access_key: str | None = None,
            endpoint_url: str | None = None
        ):
        
        self.bucket = bucket_name
        self._s3 = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url
        )
    
    @property
    def supports_streaming_upload(self):
        return True

    def create_object(self, key: str, data: bytes):
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        return ObjectData(key=key, created=datetime.now(), size=len(data))

    def delete_object(self, key: str):
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def list_objects(self) -> list[ObjectData]:
        paginator = self._s3.get_paginator("list_objects_v2")
        objects = []

        for page in paginator.paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                objects.append(ObjectData(
                    key=obj["Key"],
                    created=obj["LastModified"],
                    size=obj["Size"]
                ))
        return objects

    def list_incomplete_uploads(self):
        uploads = self._s3.list_multipart_uploads(Bucket=self.bucket).get("Uploads", [])
        return [
            IncompleteStreamingUpload(
                key=upload["Key"],
                created=upload["Initiated"]
            )
            for upload in uploads
        ]

    def create_streaming_upload(self, key: str):
        response = self._s3.create_multipart_upload(Bucket=self.bucket, Key=key)
        upload_id = response["UploadId"]
        return S3StreamingUpload(self, upload_id, key)


class S3StreamingUpload(AbstractStreamingUpload):
    def __init__(self, uploader: S3Uploader, upload_id: str, key: str):
        super().__init__(uploader, key)

        self.upload_id = upload_id
        self._s3 = uploader._s3
        self.bucket = uploader.bucket

        self._lock = threading.Lock()   # lock used for concurrent access to parts
        self.part_number = 1
        self.parts = []
        self.total_size = 0

    def upload_chunk(self, data: bytes):
        super().upload_chunk(data)

        with self._lock:
            part_number = self.part_number
            self.part_number += 1
            self.total_size += len(data)

        response = self._s3.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=part_number,
            UploadId=self.upload_id,
            Body=data
        )

        part_info = {
            "ETag": response["ETag"],
            "PartNumber": part_number
        }

        with self._lock:
            self.parts.append(part_info)

        return part_number

    def complete_upload(self) -> ObjectData:
        super().complete_upload()

        with self._lock:
            sorted_parts = sorted(self.parts, key=lambda p: p["PartNumber"])

        self._s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": sorted_parts}
        )

        return ObjectData(key=self.key, created=datetime.now(), size=self.total_size)

    def abort_upload(self):
        super().abort_upload()

        self._s3.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id
        )
