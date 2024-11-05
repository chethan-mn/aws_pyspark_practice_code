# Write a Python script to publish a message to an SNS topic by specifying the topic ARN and a custom message.
import boto3

# Create an SNS client
sns_client = boto3.client('sns', region_name='us-east-1')  # Replace with your desired region

# Replace with your FIFO topic ARN
topic_arn = 'arn:aws:sns:us-east-1:863518435568:sns_practice.fifo'  # Update with your topic ARN

# The message you want to publish
message = 'This is a test message published from boto3 to a FIFO SNS topic.'

# The subject of the message (optional)
subject = 'Test FIFO SNS Message'

# The MessageGroupId is required for FIFO topics
message_group_id = '3b08b0e1-3306-5311-a57f-ef456f52d4b5'  # Replace with your appropriate group ID (e.g., "group1", "user1")

try:
    # Publish the message to the SNS topic
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject,
        MessageGroupId=message_group_id  # FIFO topics require this parameter
    )

    # Print the message ID returned from SNS
    print(f"Message sent successfully! Message ID: {response['MessageId']}")

except Exception as e:
    print(f"An error occurred: {e}")

