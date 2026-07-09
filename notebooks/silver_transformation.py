"""
Production-ready Silver Layer Transformation Pipeline.

Reads raw JSON payloads from the Bronze Iceberg table, parses and cleans the data,
applies categorization and sentiment scoring, filters out records with empty titles,
and writes the cleaned data to the Silver Iceberg table.
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, current_timestamp, from_json, lit, regexp_extract,
    to_date, when
)
from pyspark.sql.types import StringType, StructField, StructType

# ------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SilverTransformation")

# ------------------------------------------------------------------
# Environment Variables (with defaults)
# ------------------------------------------------------------------
BRONZE_TABLE = os.getenv("BRONZE_TABLE", "rest_prod.bronze_layer.news_bronze")
SILVER_TABLE = os.getenv("SILVER_TABLE", "rest_prod.silver_layer.news_silver")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "hdfs://namenode:9000/user/iceberg/checkpoints/silver_layer"
)
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
HDFS_WAREHOUSE = os.getenv(
    "HDFS_WAREHOUSE",
    "hdfs://namenode:9000/user/iceberg/warehouse"
)
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")  # Override if needed

# ------------------------------------------------------------------
# Schema Definition for JSON Payload
# ------------------------------------------------------------------
news_schema = StructType([
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("author", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField(
        "source",
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]),
        True,
    ),
])


def create_spark_session() -> SparkSession:
    """Build and return a Spark session configured for Iceberg and the REST catalog."""
    return (
        SparkSession.builder
        .appName("SilverTransformationCleanSourceNames")
        .master(SPARK_MASTER)
        .config(
            "spark.jars",
            "/home/jovyan/.ivy2/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
            "/home/jovyan/.ivy2/jars/kafka-clients-3.4.1.jar,"
            "/home/jovyan/.ivy2/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
            "/home/jovyan/.ivy2/jars/commons-pool2-2.11.1.jar"
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest_prod", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest_prod.type", "rest")
        .config("spark.sql.catalog.rest_prod.uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog.rest_prod.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.catalog.rest_prod.warehouse", HDFS_WAREHOUSE)
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.rpc.askTimeout", "600s")
        .getOrCreate()
    )


def main() -> None:
    """Main entry point for the Silver Layer transformation."""
    spark = None
    try:
        logger.info("Initializing Spark session...")
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully.")

        # ------------------------------------------------------------------
        # 1. Read streaming data from Bronze table
        # ------------------------------------------------------------------
        logger.info(f"Reading streaming data from {BRONZE_TABLE}...")
        bronze_df = (
            spark.readStream
            .format("iceberg")
            .option("streaming-skip-delete-snapshots", "true")
            .load(BRONZE_TABLE)
        )

        # ------------------------------------------------------------------
        # 2. Parse JSON payload and extract fields
        # ------------------------------------------------------------------
        raw_parsed_df = bronze_df.select(
            from_json(col("raw_payload"), news_schema).alias("data"),
            col("source_type"),
            col("ingested_at").alias("bronze_ingested_at")
        ).select(
            col("data.title").alias("title"),
            coalesce(col("data.content"), col("data.description"), lit("No content available")).alias("content"),
            col("data.author").alias("author_raw"),
            coalesce(col("data.published_at"), col("data.publishedAt")).alias("published_at_raw"),
            col("source_type"),
            coalesce(col("data.source.name"), col("data.source_name"), lit("Unknown Source")).alias("raw_source_name"),
            "bronze_ingested_at"
        )

        # ------------------------------------------------------------------
        # 3. Clean and enrich the data
        # ------------------------------------------------------------------
        parsed_df = (
            raw_parsed_df
            .withColumn(
                "source_name",
                when(
                    col("raw_source_name").like("Web Scraper (%)"),
                    regexp_extract(col("raw_source_name"), r"Web Scraper \((.*?)\)", 1)
                )
                .when(
                    col("raw_source_name").like("Batch API (%)"),
                    regexp_extract(col("raw_source_name"), r"Batch API \((.*?)\)", 1)
                )
                .otherwise(
                    when(
                        col("raw_source_name").isNull() | (col("raw_source_name") == ""),
                        lit("Unknown Source")
                    ).otherwise(col("raw_source_name"))
                )
            )
            .withColumn(
                "author",
                when(
                    col("author_raw").isin("Unknown Author", "EMPTY", "", None),
                    lit("Unknown Author")
                ).otherwise(col("author_raw"))
            )
            .withColumn(
                "published_at",
                coalesce(col("published_at_raw").cast("timestamp"), col("bronze_ingested_at"))
            )
            .withColumn(
                "category",
                when(
                    col("title").rlike("(?i)(AI Core|Quantum|Processor|Chip|Machine Learning)"),
                    lit("Hardware & AI")
                )
                .when(
                    col("title").rlike("(?i)(iPhone|MacBook|iOS|Apple)"),
                    lit("Apple Ecosystem")
                )
                .when(col("source_type") == "Web-Scraper", lit("Tech Community"))
                .otherwise(lit("General Tech"))
            )
            .withColumn(
                "sentiment",
                when(
                    col("title").rlike("(?i)(Disrupts|Upgrades|Reveals|Launches|New|Top|Best)"),
                    lit("Positive")
                )
                .when(
                    col("title").rlike("(?i)(Error|Fail|Bug|Leak|Crisis|Crash)"),
                    lit("Negative")
                )
                .otherwise(lit("Neutral"))
            )
        )

        # ------------------------------------------------------------------
        # 4. Select final columns, filter out empty titles, and deduplicate
        # ------------------------------------------------------------------
        final_silver_df = (
            parsed_df.select(
                col("title"),
                col("content"),
                col("source_name"),
                col("source_type"),
                col("author"),
                col("published_at"),
                to_date(col("published_at")).alias("published_date"),
                col("category"),
                col("sentiment"),
                current_timestamp().alias("ingested_at")
            )
            # Data quality rule: reject records with NULL or empty title
            .filter(col("title").isNotNull() & (col("title") != ""))
            .withWatermark("published_at", "10 minutes")
            .dropDuplicates(["title"])
        )

        # ------------------------------------------------------------------
        # 5. Write the stream to the Silver Iceberg table
        # ------------------------------------------------------------------
        logger.info(f"Writing cleaned data to {SILVER_TABLE}...")
        query = (
            final_silver_df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .partitionBy("source_type", "published_date")
            .trigger(availableNow=True)   # Processes all available data and stops
            .toTable(SILVER_TABLE)
        )

        query.awaitTermination()
        logger.info("Silver Layer transformation completed successfully.")

    except Exception as e:
        logger.error(f"Silver transformation failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()