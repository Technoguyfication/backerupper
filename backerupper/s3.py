from __future__ import annotations

from urllib.parse import urljoin
from dataclasses import dataclass
import xml.etree.ElementTree as ET
import requests
from requests_aws4auth import AWS4Auth

class S3:
    S3NS = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, aws_region: str):
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._aws_region = aws_region
        self.auth = AWS4Auth(access_key, secret_key, aws_region, 's3')

        # set up session
        self._session = requests.Session()
    
    def _request(self, method: str, path: str, params: dict | None = None, data: bytes | None = None):
        url = urljoin(self._endpoint_url, path)

        response = self._session.request(method, url, params=params, data=data, auth=self.auth)
        response.raise_for_status()
        return response
    
    def _request_parse_xml(self, method: str, path: str, params: dict | None = None, data: bytes | None = None):
        response = self._request(method, path, params, data)
        if not response.headers.get('Content-Type') == 'application/xml':
            raise RuntimeError("Did not receive XML response")
        
        return ET.fromstring(response.content)

    def list_buckets(self) -> list[Bucket]:
        response = self._request_parse_xml('GET', '/')
        buckets = []
        for bucket in response.findall('.//s3:Bucket', self.S3NS):
            name = bucket.find('s3:Name', self.S3NS).text
            creation_date = bucket.find('s3:CreationDate', self.S3NS).text
            buckets.append(Bucket(name=name, creation_date=creation_date))
        return buckets

    def list_objects(self, bucket_name: str, continuation_token: str | None = None) -> list[str]:
        params = {
            'list-type': 2,
        }

        if continuation_token:
            params['continuation-token'] = continuation_token

        path = f'/{bucket_name}'
        response = self._request_parse_xml('GET', path, params)

        objects = []
        for obj in response.findall('.//s3:Contents', self.S3NS):
            key = obj.find('s3:Key', self.S3NS).text
            size = int(obj.find('s3:Size', self.S3NS).text)
            last_modified = obj.find('s3:LastModified', self.S3NS).text
            etag = obj.find('s3:ETag', self.S3NS).text.strip('"')
            storage_class = obj.find('s3:StorageClass', self.S3NS).text
            objects.append(Object(key=key, size=size, last_modified=last_modified, etag=etag, storage_class=storage_class))

        # recursively fetch remaining objects
        is_truncated = response.find('.//s3:IsTruncated', self.S3NS).text.lower() == 'true'
        if is_truncated:
            continuation_token = response.find('.//s3:NextContinuationToken', self.S3NS).text
            objects.extend(self.list_objects(bucket_name, continuation_token))
        
        return objects

    def put_object(self, bucket_name: str, key: str, data: bytes) -> None:
        path = f'/{bucket_name}/{key}'
        self._request('PUT', path, data=data)

    def get_object_url(self, bucket_name: str, key: str) -> str:
        path = f'/{bucket_name}/{key}'
        return urljoin(self._endpoint_url, path)
    
    def delete_object(self, bucket_name: str, key: str) -> None:
        path = f'/{bucket_name}/{key}'
        self._request('DELETE', path)

    def create_multipart_upload(self, bucket_name: str, key: str) -> MultipartUploadStream:
        response = self._request_parse_xml('POST', f'/{bucket_name}/{key}?uploads')
        upload_id = response.find('s3:UploadId', S3.S3NS).text

        return MultipartUploadStream(
            self,
            bucket_name,
            key,
            upload_id
        )

class MultipartUploadStream():
    def __init__(self, s3_client: S3, bucket: str, key: str, id: str):
        self._client = s3_client
        self.bucket = bucket
        self.key = key
        self.id = id
        self._part_number = 0
        self._completed = False

        self._parts: list[str] = []

    def upload_part(self, data: bytes) -> tuple[int, str]:
        if self._completed:
            raise RuntimeError("Cannot upload part to a completed multipart upload!")
        
        part_number = self._part_number
        self._part_number += 1
        params = {
            'partNumber': part_number,
            'uploadId': self.id
        }

        response = self._client._request('PUT', f'/{self.bucket}/{self.key}', params=params, data=data)
        etag = response.headers.get("ETag")
        if not etag:
            raise RuntimeError("ETag not returned from UploadPart!")
        
        self._parts.insert(part_number, etag)
        return (part_number, etag)
    
    def complete(self):
        self._completed = True

        # build CompleteMultipartUpload payload
        root = ET.Element('CompleteMultipartUpload')
        for part, etag in enumerate(self._parts):
            part_elem = ET.SubElement(root, 'Part')
            part_number_elem = ET.SubElement(part_elem, 'PartNumber')
            part_number_elem.text = part
            
            etag_elem = ET.SubElement(part_elem, 'ETag')
            etag_elem.text = etag
        
        xml = ET.tostring(root, encoding='unicode', xml_declaration=True)

        params = {
            'uploadId': self.id
        }

        response = self._client._request_parse_xml('POST', f'/{self.bucket}/{self.key}', params=params, data=xml.encode())
        location = response.find('s3:Location', S3.S3NS).text
        etag = response.find('s3:ETag', S3.S3NS).text

        return CompleteMultipartUploadResult(
            location,
            self.bucket,
            self.key,
            etag
        )


    def abort(self):
        self._completed = True
        ...

@dataclass
class CompleteMultipartUploadResult:
    location: str
    bucket: str
    key: str
    etag: str

@dataclass
class Bucket:
    name: str
    creation_date: str

@dataclass
class Object:
    key: str
    size: int
    last_modified: str
    etag: str
    storage_class: str
