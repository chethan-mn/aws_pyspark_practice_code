# RetailMart wants to identify inactive customers who haven’t made recent purchases. Load customer data from DynamoDB 
# and transaction data from S3, and calculate the last purchase date for each customerusing PySpark.
# Flag customers as inactive if their last purchase was more than six months ago, then update this status in DynamoDB.
import boto3
from pyspark.sql import Row
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
# import os
# os.environ["PYSPARK_PYTHON"] = "C:\\Users\\CNagaraj\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"
# Initialize Spark session
spark = SparkSession.builder \
    .appName("IdentifyInactiveCustomers") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", 8) \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('Customer_details')

customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])

transaction_df = spark.read.option("header", "true").csv("s3a://script-csv-bkt/transaction_data.csv")
transaction_df = transaction_df.withColumn("timestamp", F.to_timestamp("transaction_date", "dd-MM-yyyy"))

joined_df = transaction_df.join(customer_df, "customer_id", "inner")
print(joined_df)
last_purchase_df = joined_df.groupBy("customer_id").agg(F.max("timestamp").alias("last_purchase_date"))
six_months_ago = datetime.now() - timedelta(days=6*30) 
six_months_ago_str = six_months_ago.strftime('%Y-%m-%d')
inactive_customers_df = last_purchase_df.withColumn(
    "inactive", 
    F.when(F.col("last_purchase_date") < F.lit(six_months_ago_str), "inactive").otherwise("active")
)

# Show inactive customers
# inactive_customers_df.show()
print(inactive_customers_df)
for row in inactive_customers_df.collect():
    customer_id = row['customer_id']
    status = row['inactive']
    customer_table.update_item(
        Key={'customer_id': customer_id},
        UpdateExpression="set #status = :s",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":s": status}
    )
spark.stop()
