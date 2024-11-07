# RetailMart wants to analyze churn by monitoring inactive accounts. Load customer profiles from DynamoDB and 
# transaction history from S3, then use PySpark to identify customers withno transactions 
# in the last year and calculate the monthly churn rate, helping inform customer retention strategies.
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, month, count
from datetime import datetime, timedelta

    
spark = SparkSession.builder \
    .appName("MonitorFraudulentTransactions") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .appName("RetailMart_Churn_Analysis") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

dynamodb_table = "Customers_Details"
s3_path = "s3a://script-csv-bkt/transaction_data.csv"

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('Customer_details')
customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])
print(customer_items[:2])
transaction_df = spark.read.csv(s3_path, header=True, inferSchema=True)

# customer_df.show(5)
print(customer_df)
print(transaction_df)
# transaction_df.show(5)

one_year_ago = datetime.now() - timedelta(days=365)
transaction_last_year_df = transaction_df.filter(col("transaction_date") >= one_year_ago)

active_customers_df = transaction_last_year_df.select("customer_id").distinct()

inactive_customers_df = customer_df.join(active_customers_df, on="customer_id", how="left_anti")

total_customers = customer_df.count()

inactive_by_month_df = inactive_customers_df.withColumn("inactive_month", month("profile_update_date")) \
    .groupBy("inactive_month").agg(count("customer_id").alias("inactive_customers"))

churn_rate_df = inactive_by_month_df.withColumn("churn_rate", col("inactive_customers") / total_customers * 100)
churn_rate_df.show()
spark.stop()
