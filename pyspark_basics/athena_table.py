import boto3
import time
 
def create_athena_table(database, table_name, s3_location):
    # Initialize the Athena client
    athena_client = boto3.client('athena')
 
    # Define the schema for the table
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
        id INT,
        name STRING,
        age INT,
        email STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
        'serialization.format' = ','
    )
    LOCATION '{s3_location}'
    TBLPROPERTIES ('has_encrypted_data'='false');
    """
 
    # Specify the S3 location for query results
    result_location = 's3://output-bucket-010/folder/'
 
    # Start the query execution to create the table
    response = athena_client.start_query_execution(
        QueryString=create_table_query,
        ResultConfiguration={'OutputLocation': result_location}
    )
 
    # Capture the query execution ID
    query_execution_id = response['QueryExecutionId']
 
    # Wait until the query execution completes
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
 
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
 
    # Check if the table creation was successful
    if status == 'SUCCEEDED':
        print(f"Table {table_name} created successfully in database {database}.")
    else:
        reason = query_status['QueryExecution']['Status']['StateChangeReason']
        print(f"Failed to create table {table_name}. Reason: {reason}")
 
# Replace these variables with your own values
database = 'my_database'
table_name = 'my_table'
s3_location = 's3://demo-buckk/data-folder/'
 
create_athena_table(database, table_name, s3_location)
 