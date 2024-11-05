import boto3
eventbridge_client = boto3.client('events', region_name='us-east-1')
try:
   response = eventbridge_client.list_event_buses()
   print("EventBridge Event Buses:")
   for bus in response['EventBuses']:
      print("Name:", bus['Name'])
except Exception as e:
   print("Error listing EventBridge event buses:", e)