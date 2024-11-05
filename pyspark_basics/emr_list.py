# Write a Python program using Boto3 to list all EMR clusters in your AWS account and print their cluster IDs and statuses.
# Write a Python script to start a new EMR cluster with a specific configuration and print the cluster ID.
import boto3

# Create an EMR client
emr_client = boto3.client('emr', region_name='us-east-1')  # Replace with your desired region

try:
    # List all EMR clusters
    response = emr_client.list_clusters()

    # Get the list of clusters
    clusters = response.get('Clusters', [])

    if clusters:
        print("EMR Clusters in your account:")
        for cluster in clusters:
            cluster_id = cluster['Id']
            cluster_status = cluster['Status']['State']
            print(f"Cluster ID: {cluster_id}, Status: {cluster_status}")
    else:
        print("No EMR clusters found in your account.")

except Exception as e:
    print(f"An error occurred: {e}")
