# Write a Python script to send a message to a specific SQS queue and print the message ID.
import boto3

# Create an SQS client
sqs_client = boto3.client('sqs', region_name='us-east-1')  # Replace with your desired region

# Replace with your queue URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/863518435568/sqs_practice'  # Update with your queue URL

# The message you want to send
message_body = 'This is a test message from boto3.'

try:
    # Send the message to the specified queue
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body
    )

    # Print the message ID
    print(f"Message sent successfully! Message ID: {response['MessageId']}")

except Exception as e:
    print(f"An error occurred: {e}")
