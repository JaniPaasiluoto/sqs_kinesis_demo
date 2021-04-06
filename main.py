import base64
import json
import os
import uuid
import boto3
from botocore.config import Config
from datetime import datetime, timezone

# define the required variables
LOCAL_ENDPOINT_URL = os.environ.get("SQS_ENDPOINT_URL", "http://localhost:4566")
FLEET_SIZE = int(os.environ.get("FLEET_SIZE", 10))
STREAM_NAME = 'stream_name'
REGION = 'aws_region'
QUEUE_URL = 'url'
MESSAGE_ATTRIBUTE_NAMES = 'att_names'
# configuration for Kinesis and SQS clients
config = Config(
    region_name=REGION,
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    }
)


# function to receive message from SQS
# maxNumberOfMessages and visibilityTimeout have default value bit can be changed
# if maxNumberOfMessages is not between 1-10, the default value 1 is used
def receive_message(sqs_client, d_id, url, maxNumberOfMessages=1, visibilityTimeout=10):
    if maxNumberOfMessages < 1 or maxNumberOfMessages > 10:
        maxNumberOfMessages = 1
    return sqs_client.receive_message(
        QueueUrl=QUEUE_URL,
        AttributeNames=['All'],
        MessageAttributeNames=[
            MESSAGE_ATTRIBUTE_NAMES,
        ],
        MaxNumberOfMessages=maxNumberOfMessages,
        VisibilityTimeout=visibilityTimeout,
    )


# function to publish the event into kinesis stream
def put_to_stream(event_type, device_id, timestamp, submission_id, kinesis_client):
    payload = {
        'event_type': str(event_type),
        'timestamp': str(timestamp),
        'device_id': device_id,
        'uuid': str(uuid.uuid4())
    }
    print(payload)
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(payload),
        PartitionKey=submission_id)


# function to delete a message from SQS
def delete_message_from_stream(sqs_client, url, receiptHandle):
    sqs_client.delete_message(
        QueueUrl=url,
        ReceiptHandle=receiptHandle
    )


def run_preprocessor():
    # Initialize SQS client
    sqs_client = boto3.client('sqs', config=config, endpoint_url=LOCAL_ENDPOINT_URL, verify=False)
    # Initialize kinesis client
    kinesis_client = boto3.client('kinesis', config=config, aws_access_key_id='', aws_secret_access_key='',
                                  endpoint_url=LOCAL_ENDPOINT_URL)

    device_ids = [str(uuid.uuid4()) for _ in range(FLEET_SIZE)]

    # loop until terminated
    while True:
        for d_id in device_ids:
            # retrieve a message from SQS
            response = receive_message(sqs_client, d_id, LOCAL_ENDPOINT_URL)
            # catch any errors and continue looping
            try:
                messageBody = response["Messages"][0]["Body"]
                message_bytes = base64.b64decode(messageBody)
                message = message_bytes.decode('ascii')
                # jsonify the string and get the data needed from it
                message_json = json.loads(message)
                events = message_json["events"]
                device_id = message_json["device_id"]
                submission_id = message_json["submission_id"]
                receiptHandle = response["Messages"][0]["ReceiptHandle"]
                # loop through the events
                for event in events:
                    # create a timestamp
                    now_utc = datetime.now(timezone.utc)
                    # publish the event into the `events` Kinesis stream
                    put_to_stream(str(event), device_id, now_utc, submission_id, kinesis_client)
                    # after publishment, delete the message from the SQS
                    delete_message_from_stream(sqs_client, LOCAL_ENDPOINT_URL, receiptHandle)
            except Exception as e:
                print(e)
                pass


if __name__ == '__main__':
    run_preprocessor()
