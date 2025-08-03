import hmac
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, quote

class AWS:

    @classmethod
    def _sign(cls, key: str, msg: str):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    @classmethod
    def _get_signature_key(cls, key: str, date_stamp: str, region_name: str, service_name: str):
        k_date = cls._sign(('AWS4' + key).encode('utf-8'), date_stamp)
        k_region = cls._sign(k_date, region_name)
        k_service = cls._sign(k_region, service_name)
        k_signing = cls._sign(k_service, 'aws4_request')
        return k_signing

    def aws_encode_query(params):
        return '&'.join(
            f'{quote(k, safe="-_.~")}={quote(str(v), safe="-_.~")}'
            for k, v in sorted(params.items())
        )

    @classmethod
    def generate_aws_auth_header(cls, access_key: str, secret_key: str, region: str, service: str, method: str, endpoint: str, uri: str, query_params: dict | None, payload: str):
        t = datetime.now(tz=timezone.utc)
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')

        parsed_url = urlparse(endpoint)
        host = parsed_url.netloc

        canonical_headers = f'host:{host}\n' + f'x-amz-date:{amz_date}\n'
        signed_headers = 'host;x-amz-date'
        payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()

        query_string = cls.aws_encode_query(query_params) if query_params else ''

        canonical_request = '\n'.join([
            method,
            uri,
            query_string,
            canonical_headers,
            signed_headers,
            payload_hash
        ])

        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f'{date_stamp}/{region}/{service}/aws4_request'
        string_to_sign = '\n'.join([
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        ])

        # Signature
        signing_key = cls._get_signature_key(secret_key, date_stamp, region, service)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

        # Authorization header
        authorization_header = (
            f'{algorithm} '
            f'Credential={access_key}/{credential_scope}, '
            f'SignedHeaders={signed_headers}, '
            f'Signature={signature}'
        )

        return {
            'Authorization': authorization_header,
            'x-amz-date': amz_date
        }   
