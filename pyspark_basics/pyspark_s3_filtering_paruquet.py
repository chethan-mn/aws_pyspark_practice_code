# Write a PySpark script to load a CSV file from an S3 bucket, filter rows based 
# on a condition (e.g., age > 30), and save the filtered data back to another S3 bucket in Parquet format.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with Hadoop AWS package
spark = SparkSession.builder \
    .appName("FilterCSVData") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA4SDNVVDYC7X63ZDY")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

# Define S3 input and output paths
input_path = "s3a://pyspark-s3-bucket/input_data.csv" 
output_path = "s3a://pyspark-s3-out/filtered_data" 

# Load CSV data from S3
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Filter rows where age > 30
filtered_df = df.filter(col("age") > 30)

# Save the filtered data to S3 in Parquet format
filtered_df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
