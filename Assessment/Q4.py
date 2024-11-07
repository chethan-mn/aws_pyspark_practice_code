# To better understand purchasing behavior, load customer data from DynamoDB and join it with sales data from S3.
# Use PySpark to calculate the time intervals between each purchase for individual customers, 
# then find the average transaction interval to identify high-engagement customers.
import boto3
from pyspark.sql import Row
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("CustomerEngagementAnalysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Initialize Boto3 DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('Customer_details')

# Load customer data from DynamoDB
customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])

# Load transaction data from S3 (ensure the CSV file includes 'customer_id' and 'transaction_date' columns)
transaction_df = spark.read.option("header", "true").csv("s3a://script-csv-bkt/transaction_data.csv")
transaction_df.show(5)
# Convert 'transaction_date' column to a proper timestamp type if necessary
transaction_df = transaction_df.withColumn("timestamp", F.to_timestamp("transaction_date", "dd-MM-yyyy"))

# Join the customer data with transaction data on 'customer_id'
joined_df = transaction_df.join(customer_df, "customer_id", "inner")

# Sort transactions by 'customer_id' and 'timestamp' to calculate the time difference
joined_df = joined_df.orderBy("customer_id", "timestamp")

# Calculate time interval between consecutive transactions for each customer
window_spec = Window.partitionBy("customer_id").orderBy("timestamp")
joined_df = joined_df.withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))

# Calculate the time difference in seconds
joined_df = joined_df.withColumn("time_diff", F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp"))

# Filter out rows with null time_diff (first purchase for each customer will have null)
joined_df = joined_df.filter(joined_df.time_diff.isNotNull())

# Calculate average transaction interval for each customer
avg_interval_df = joined_df.groupBy("customer_id").agg(F.avg("time_diff").alias("avg_transaction_interval"))

# Convert time difference from seconds to a more readable format (e.g., days)
avg_interval_df = avg_interval_df.withColumn("avg_transaction_interval_days", avg_interval_df["avg_transaction_interval"] / (60 * 60 * 24))
avg_interval_df.show()
high_engagement_customers_df = avg_interval_df.filter(avg_interval_df["avg_transaction_interval_days"] < 7)
high_engagement_customers_df.show()
spark.stop()
