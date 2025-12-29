import json
import boto3
import os
from io import BytesIO
from PIL import Image, ExifTags
from botocore.exceptions import ClientError
from datetime import datetime

s3 = boto3.client("s3")
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]

def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            message = json.loads(record["body"])
            bucket = message["bucket"]
            key = message["key"]
            etag = message.get("etag")

            metadata_key = (f'{OUTPUT_PREFIX}/{key}.json')

            # -------------------------------
            # Idempotency check
            # -------------------------------
            if metadata_exists(bucket, metadata_key):
                print(f"Metadata already exists, skipping: {metadata_key}")
                continue

            # -------------------------------
            # Download image
            # -------------------------------
            image_obj = s3.get_object(Bucket=bucket, Key=key)
            image_bytes = image_obj["Body"].read()
            file_size = image_obj["ContentLength"]

            image = Image.open(BytesIO(image_bytes))

            # -------------------------------
            # Extract metadata
            # -------------------------------
            metadata = {
                "bucket": bucket,
                "key": key,
                "etag": etag,
                "format": image.format,
                "width": image.width,
                "height": image.height,
                "file_size": file_size,
                "exif": extract_exif(image),
                "processed_at": datetime.utcnow().isoformat()
            }

            # -------------------------------
            # Write metadata JSON to S3
            # -------------------------------
            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json"
            )

            print(f"Metadata written to: {metadata_key}")

        except Exception as e:
            print("Failed processing message")
            raise e

    return {"statusCode": 200}

def metadata_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise e

def extract_exif(image):
    try:
        exif_raw = image._getexif()
        if not exif_raw:
            return None

        return {
            ExifTags.TAGS.get(tag, tag): value
            for tag, value in exif_raw.items()
        }
    except Exception:
        return None

