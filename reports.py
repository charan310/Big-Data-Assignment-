from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, weekofyear, month, current_date

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.iceberg:iceberg-spark3-runtime:0.12.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.sql.catalog.event.type", "hive") \
    .config("spark.sql.catalog.event.uri", "thrift://localhost:9083") \
    .config("spark.sql.catalog.event.clients", "10") \
    .config("spark.sql.catalog.event.warehouse.dir", "/user/hive/warehouse") \
    .getOrCreate()

# Read data from Kafka
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "new_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka data to DataFrame
df = df.selectExpr("CAST(value AS STRING)")

# Define schema for DataFrame
schema = "Event_Name STRING, Event_Type STRING, Event_Value STRING, Event_Timestamp STRING, Event_Page_Source STRING, Event_Page_URL STRING, Event_Component_ID STRING, user_id STRING, Event_Date STRING"

# Apply schema and split data into columns
df = df.selectExpr("split(value, ',') as row").selectExpr(*[f"row[{i}] as {col_name}" for i, col_name in enumerate(schema.split(", "))])

# Convert timestamp string to timestamp type
df = df.withColumn("Event_Timestamp", col("Event_Timestamp").cast("timestamp"))

# Write DataFrame to Hive table
df.write.mode("overwrite").saveAsTable("default.kevent")

# Generate daily/weekly/monthly reports
daily_report = df.groupBy("Event_Date").agg(count("*").alias("count")).orderBy("Event_Date")

weekly_report = df.groupBy(weekofyear("Event_Timestamp").alias("Week")).agg(count("*").alias("count")).orderBy("Week")

monthly_report = df.groupBy(month("Event_Timestamp").alias("Month")).agg(count("*").alias("count")).orderBy("Month")

# Top 10 events for a given date
top_events = df.filter(col("Event_Date") == "05-01-2024").groupBy("Event_Name").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10)

# Returning users (assuming returning users are those who have multiple events)
returning_users = df.groupBy("user_id").agg(count("*").alias("Event_Count")).filter(col("Event_Count") > 1).count()

# Active users (assuming active users are those who have events in the past X days)
active_users = df.filter(col("Event_Timestamp") >= (current_date() - 30)).select("user_id").distinct().count()

# Churn (assuming churned users are those who had events before but not in the past X days)
churn_users = df.filter(col("Event_Timestamp") < (current_date() - 30)).select("user_id").distinct().count()

# Store reports in Iceberg tables
daily_report.write.mode("overwrite").saveAsTable("icberg.default.daily_report")
weekly_report.write.mode("overwrite").saveAsTable("icberg.default.weekly_report")
monthly_report.write.mode("overwrite").saveAsTable("icberg.default.monthly_report")
top_events.write.mode("overwrite").saveAsTable("icberg.default.top_events")

