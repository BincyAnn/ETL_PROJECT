import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .config("spark.jars.repositories", "https://repo1.maven.org/maven2/") \
    .config("spark.jars", "/home/bincy/Downloads/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for the JSON data
weather_schema = StructType([
    StructField("location", StructType([
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("tz_id", StringType(), True),
        StructField("localtime_epoch", IntegerType(), True),
        StructField("localtime", StringType(), True)
    ]), True),
    StructField("current", StructType([
        StructField("last_updated_epoch", IntegerType(), True),
        StructField("last_updated", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("condition", StructType([
            StructField("text", StringType(), True),
            StructField("icon", StringType(), True),
            StructField("code", IntegerType(), True)
        ]), True),
        # ... (add other fields as needed)
    ]), True)
])

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_data") \
    .load()

# Parse the JSON data in the "value" column
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), weather_schema).alias("data"),
    "topic",
    "partition",
    "offset",
    "timestamp"
)

# Select the desired columns from the parsed data
weather_df = parsed_df.select(
    "data.location.name",
    "data.location.region",
    "data.location.country",
    "data.current.temp_c",
    "data.current.condition.text",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

# Write the data to PostgreSQL using foreachBatch
query = weather_df \
    .writeStream \
    .foreachBatch(lambda batch_df, batch_id: 
                  batch_df.select("*") \
                  .write \
                  .format("jdbc") \
                  .option("url", "jdbc:postgresql://data11.cdeams0isk9w.ap-south-1.rds.amazonaws.com:5432/weather_dataa") \
                  .option("dbtable", "weather_data") \
                  .option("user", "datauser") \
                  .option("password", "Datauser1234") \
                  .option("driver", "org.postgresql.Driver") \
                  .mode("append") \
                  .save()) \
    .start()

# Wait for the query to terminate
query.awaitTermination()