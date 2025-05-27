from __future__ import annotations

from urllib.parse import urljoin
from dataclasses import dataclass
import xml.etree.ElementTree as ET
import requests

from backerupper.aws import AWS

class S3:
    S3NS = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, aws_region: str):
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._aws_region = aws_region

        # set up session
        self._session = requests.Session()
        self._session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _request(self, method: str, path: str, params=None, data=None):
        url = urljoin(self._endpoint_url, path)
        auth_headers = AWS.generate_aws_auth_header(
            access_key=self._access_key,
            secret_key=self._secret_key,
            region=self._aws_region,
            service='s3',
            method=method,
            endpoint=self._endpoint_url,
            uri=path,
            query_string=params if params else '',
            payload=data if data else ''
        )
        response = self._session.request(method, url, params=params, json=data, headers=auth_headers)
        response.raise_for_status()
        if response.headers.get('Content-Type') == 'application/xml':
            return ET.fromstring(response.content)
        else:
            raise ValueError("Unexpected content type: {}".format(response.headers.get('Content-Type')))

    def list_buckets(self) -> list[Bucket]:
        response = self._request('GET', '/')
        buckets = []
        for bucket in response.findall('.//s3:Bucket', self.S3NS):
            name = bucket.find('s3:Name', self.S3NS).text
            creation_date = bucket.find('s3:CreationDate', self.S3NS).text
            buckets.append(Bucket(name=name, creation_date=creation_date))
        return buckets

@dataclass
class MultipartUpload:
    ...

@dataclass
class Bucket:
    name: str
    creation_date: str
