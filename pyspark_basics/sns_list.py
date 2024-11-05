# Write a Python program to list all SNS topics in your AWS account and print their ARNs.
import boto3

# Create an SNS client
sns_client = boto3.client('sns', region_name='us-east-1')  # Replace with your desired region

try:
    # List all SNS topics
    response = sns_client.list_topics()

    # Get the list of topic ARNs
    topics = response.get('Topics', [])

    if topics:
        print("SNS Topics in your account:")
        for topic in topics:
            topic_arn = topic['TopicArn']
            print(f"Topic ARN: {topic_arn}")
    else:
        print("No SNS topics found in your account.")

except Exception as e:
    print(f"An error occurred: {e}")
