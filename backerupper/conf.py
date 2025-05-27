import os

class Config:

    @staticmethod
    def aws_access_key_id():
        if (key := os.getenv('AWS_ACCESS_KEY_ID')):
            return key
        raise ValueError("AWS_ACCESS_KEY_ID is not set")

    @staticmethod
    def aws_secret_access_key():
        if (key := os.getenv('AWS_SECRET_ACCESS_KEY')):
            return key
        raise ValueError("AWS_SECRET_ACCESS_KEY is not set")
    
    @staticmethod
    def aws_endpoint_url():
        if (url := os.getenv('AWS_ENDPOINT_URL')):
            return url
        raise ValueError("AWS_ENDPOINT_URL is not set")
    
    @staticmethod
    def aws_s3_bucket_name():
        if (bucket := os.getenv('AWS_S3_BUCKET_NAME')):
            return bucket
        raise ValueError("AWS_S3_BUCKET_NAME is not set")
    
    @staticmethod
    def aws_s3_region_name():
        return os.getenv('AWS_S3_REGION_NAME', 'us-east-1')
