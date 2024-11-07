# RetailMart wants to monitor unusual spending patterns as part of fraud detection. Load sales transactions 
# from S3 and join with customer data from DynamoDB. Calculate the average spending per transaction and 
# flag any transactions that exceed a certain threshold as anomalies, then log these flags in DynamoDB.
import boto3
from pyspark.sql import Row
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("MonitorFraudulentTransactions") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
customer_table = dynamodb.Table('Customer_details')

customer_items = customer_table.scan()['Items']
customer_df = spark.createDataFrame([Row(**item) for item in customer_items])

transaction_df = spark.read.option("header", "true").csv("s3a://script-csv-bkt/sales_transactions.csv")
transaction_df = transaction_df.withColumn("transaction_date", F.to_timestamp("transaction_date", "yyyy-MM-dd HH:mm:ss"))
joined_df = transaction_df.join(customer_df, "customer_id", "inner")
avg_spending_df = joined_df.groupBy("customer_id").agg(F.avg("transaction_amount").alias("avg_spending"))
joined_df = joined_df.join(avg_spending_df, "customer_id")
threshold_multiplier = 2
joined_df = joined_df.withColumn("anomaly_flag", 
                                 F.when(F.col("transaction_amount") > threshold_multiplier * F.col("avg_spending"), "anomaly")
                                  .otherwise("normal"))

anomalies_df = joined_df.filter(joined_df.anomaly_flag == "anomaly")
print(anomalies_df)

for row in anomalies_df.collect():
    customer_id = row['customer_id']
    anomaly_flag = row['anomaly_flag']
    transaction_id = row['transaction_id']
    amount = row['transaction_amount']
    customer_table.update_item(
        Key={'customer_id': customer_id},
        UpdateExpression="set anomaly_flag = :flag, transaction_id = :tid, amount = :amt",
        ExpressionAttributeValues={
            ":flag": anomaly_flag,
            ":tid": transaction_id,
            ":amt": amount
        }
    )
spark.stop()
