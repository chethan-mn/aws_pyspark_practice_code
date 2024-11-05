# Write a Python program using Boto3 to list all SQS queues in your AWS account and print their URLs.
import boto3

# Create an SQS client
sqs_client = boto3.client('sqs', region_name='us-east-1')  # Replace with your desired region

try:
    # List all SQS queues
    response = sqs_client.list_queues()

    # Get the list of queue URLs
    queue_urls = response.get('QueueUrls', [])

    if queue_urls:
        print("SQS Queues in your account:")
        for queue_url in queue_urls:
            print(f"Queue URL: {queue_url}")
    else:
        print("No SQS queues found in your account.")

except Exception as e:
    print(f"An error occurred: {e}")

