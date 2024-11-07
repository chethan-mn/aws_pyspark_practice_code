# RetailMart wants to track customer loyalty by identifying repeat purchases. Load customer details from 
# DynamoDB and transaction data from S3, then join them on customer_id
# Use PySpark to count repeat purchases per customer, identifying top repeat customers for loyalty program targeting.

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("RetailMart_LoyaltyTracking") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Initialize Boto3 DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Customer_details')

# Scan table and load data into a Spark DataFrame
items = table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in items])

# Load transaction data from S3
transaction_df = spark.read.option("header", "true").csv("s3a://script-csv-bkt/transaction_data.csv")
transaction_df.show(5)
# Join customer and transaction data on customer_id
joined_df = customer_df.join(transaction_df, "customer_id", "inner").select("customer_id", "transaction_id")

# Count repeat purchases per customer
repeat_purchases_df = joined_df.groupBy("customer_id").agg(count("transaction_id").alias("purchase_count"))

# Filter customers with more than one purchase (repeat customers)
repeat_customers_df = repeat_purchases_df.filter(col("purchase_count") > 1)

top_repeat_customers_df = repeat_customers_df.orderBy(col("purchase_count").desc())
top_repeat_customers_df.show()
spark.stop()
