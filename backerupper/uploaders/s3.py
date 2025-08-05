from datetime import datetime
import boto3
import threading

from backerupper.uploaders.abstract import (
    AbstractUploader,
    AbstractStreamingUpload,
    ObjectMetadata,
    IncompleteStreamingUpload
)

class S3Uploader(AbstractUploader):
    def __init__(
            self, 
            bucket_name: str, 
            region_name: str  | None = None, 
            aws_access_key_id: str | None = None, 
            aws_secret_access_key: str | None = None,
            endpoint_url: str | None = None
        ):
        
        self.bucket = bucket_name
        self._s3 = boto3.client(
            's3', # type: ignore
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url
        )
    
    @property
    def supports_streaming_upload(self):
        return True

    def create_object(self, key, data):
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        return ObjectMetadata(key=key, created=datetime.now(), size=len(data))

    def delete_object(self, key):
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def list_objects(self) -> list[ObjectMetadata]:
        paginator = self._s3.get_paginator("list_objects_v2")
        objects = []

        for page in paginator.paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                objects.append(ObjectMetadata(
                    key=obj["Key"], # type: ignore
                    created=obj["LastModified"], # type: ignore
                    size=obj["Size"] # type: ignore
                ))
        return objects

    def list_incomplete_uploads(self):
        uploads = self._s3.list_multipart_uploads(Bucket=self.bucket).get("Uploads", [])
        return [
            IncompleteStreamingUpload(
                key=upload["Key"], # type: ignore
                id=upload["UploadId"], # type: ignore
                created=upload["Initiated"] # type: ignore
            )
            for upload in uploads
        ]

    def create_streaming_upload(self, key):
        response = self._s3.create_multipart_upload(Bucket=self.bucket, Key=key)
        upload_id = response["UploadId"]
        return S3StreamingUpload(self, upload_id, key)

    def abort_incomplete_upload(self, upload):
        self._s3.abort_multipart_upload(Bucket=self.bucket, UploadId=upload.id, Key=upload.key)


class S3StreamingUpload(AbstractStreamingUpload):
    def __init__(self, uploader: S3Uploader, upload_id: str, key: str):
        super().__init__()

        self.uploader = uploader
        self.upload_id = upload_id
        self.key = key

        self._lock = threading.Lock()   # lock used for concurrent access to parts
        self._part_number = 1
        self._parts = []
        self._incomplete_parts = []
        self._total_size = 0
    
    @property
    def bucket(self):
        return self.uploader.bucket
    
    @property
    def _s3(self):
        return self.uploader._s3

    def upload_chunk(self, data):
        with self._lock:
            super().upload_chunk(data)  # super checks that we can still upload a chunk
            part_number = self._part_number
            self._part_number += 1
            self._incomplete_parts.append(part_number)
            self._total_size += len(data)

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
            self._parts.append(part_info)
            self._incomplete_parts.remove(part_number)

        return part_number

    def complete_upload(self) -> ObjectMetadata:
        with self._lock:
            if len(self._incomplete_parts) > 0:
                raise RuntimeError("Cannot complete upload until all parts are uploaded!")
            
            super().complete_upload()
            sorted_parts = sorted(self._parts, key=lambda p: p["PartNumber"])

        self._s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": sorted_parts}
        )

        return ObjectMetadata(key=self.key, created=datetime.now(), size=self._total_size)

    def abort_upload(self):
        with self._lock:
            super().abort_upload()

        self._s3.abort_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)
