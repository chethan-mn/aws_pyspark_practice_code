# RetailMart wants to add insights like "favorite product category" to each customer profile. Load transactions
# from S3 and customer data from DynamoDB, use PySpark to analyze purchases
# per category, determine the most frequently bought category for each customer, and write this back to DynamoDB.
import boto3
from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
spark = SparkSession.builder \
    .appName("RetailMartCustomerInsights") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('Customer_details')
customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])
print(customer_items[:2])
transaction_df = spark.read.option("header", "true").csv("s3a://script-csv-bkt/td.csv")

category_counts_df = transaction_df.groupBy("customer_id", "category").agg(
    F.count("transaction_id").alias("purchase_count")
)

window_spec = Window.partitionBy("customer_id").orderBy(F.col("purchase_count").desc())
category_counts_df = category_counts_df.withColumn("rank", F.row_number().over(window_spec))
favorite_category_df = category_counts_df.filter(category_counts_df.rank == 1)
customer_with_favorite_category_df = customer_df.join(favorite_category_df, "customer_id", "left")
customer_with_favorite_category_df.printSchema()
print(customer_with_favorite_category_df)
for row in customer_with_favorite_category_df.collect():
    customer_table.update_item(
        Key={'customer_id': row['customer_id']},
        UpdateExpression="SET favorite_category = :val1",
        ExpressionAttributeValues={":val1": row['category']}
    )
spark.stop()
