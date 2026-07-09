from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("=== Starting Bronze Layer Ingestion (Raw Data) ===")

# 1. Define Spark Session with Kafka and Iceberg configurations
spark = (
    SparkSession.builder.appName("BronzeLayerIngestion")
    .config(
        "spark.jars",
        "/home/jovyan/.ivy2/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/home/jovyan/.ivy2/jars/kafka-clients-3.4.1.jar,/home/jovyan/.ivy2/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/home/jovyan/.ivy2/jars/commons-pool2-2.11.1.jar",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.rest_prod", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest_prod.type", "rest")
    .config("spark.sql.catalog.rest_prod.uri", "http://iceberg-rest:8181")
    .config(
        "spark.sql.catalog.rest_prod.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO",
    )
    .config(
        "spark.sql.catalog.rest_prod.warehouse",
        "hdfs://namenode:9000/user/iceberg/warehouse",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Read streaming data from Kafka broker
raw_kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "news-raw")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 3. Process Bronze Layer (Store raw payload, ingestion timestamp, and source type only)
bronze_stream = (
    raw_kafka_stream.selectExpr("CAST(value AS STRING) as raw_payload")
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_type", F.lit("Stream"))
)

# 4. Write continuous stream into Iceberg Bronze table
query_bronze = (
    bronze_stream.writeStream.format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/user/iceberg/checkpoints/bronze_layer",
    )
    .trigger(availableNow=True)
    .toTable("rest_prod.bronze_layer.news_bronze")
)

query_bronze.awaitTermination()
print("=== Bronze Layer Ingestion Completed Successfully ===")