import json
import os
import urllib.parse
import boto3
from datetime import datetime

sqs = boto3.client("sqs")

QUEUE_URL = os.environ["QUEUE_URL"]

def lambda_handler(event, context):
  for record in event["Records"]:
    bucket = record["s3"]["bucket"]["name"]

    key = urllib.parse.unquote_plus(
      record["s3"]["object"]["key"]
    )

    etag = record["s3"]["object"].get("eTag")

    message = {
      "bucket": bucket,
      "key": key,
      "etag": etag,
      "timestamp": datetime.utcnow().isoformat()
    }

    sqs.send_message(
      QueueUrl=QUEUE_URL,
      MessageBody=json.dumps(message)
    )

    print("Message sent to SQS:", message)

  return {
    "statusCode": 200,
    "body": "Messages sent successfully"
  }
