FROM python:3.13-alpine

RUN apk update && apk add --no-cache gzip bash
RUN pip3 install --no-cache-dir boto3 && rm -rf /root/.cache/pip

WORKDIR /app

COPY entrypoint.sh .
COPY s3upload.py .

ENTRYPOINT ["bash", "entrypoint.sh"]
