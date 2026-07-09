"""
Production-ready batch ingestion script for the Bronze layer.
Generates mock news articles with randomized NULL/empty values to test
data quality and transformation resilience in the Silver layer.
"""

import os
import sys
import json
import logging
import random
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit

# ------------------------------------------------------------------
# 1. Logging Configuration
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BatchIngestion")

# ------------------------------------------------------------------
# 2. Environment Variables (Production Configuration)
# ------------------------------------------------------------------
TARGET_BRONZE_TABLE = os.getenv("BRONZE_TABLE", "rest_prod.bronze_layer.news_bronze")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
HDFS_WAREHOUSE = os.getenv(
    "HDFS_WAREHOUSE", "hdfs://namenode:9000/user/iceberg/warehouse"
)

# ------------------------------------------------------------------
# 3. Data Generation Templates
# ------------------------------------------------------------------
TECH_TOPICS = [
    "AI Core",
    "iOS",
    "MacBook Pro",
    "iPhone 18",
    "Tech Future",
    "Apple",
]
ACTIONS = ["Upgrades", "Reveals Quantum", "Launches New", "Disrupts the Market with"]
FEATURES = [
    "Next-Gen Processor",
    "AI Engine",
    "Smart Connectivity",
    "Display Technology",
]
AUTHORS = ["Aimen Al-Faqih", "Sarah Jenkins", "Tech Radar Staff", "Unknown Author"]
SOURCES = [
    "Batch API (TechCrunch)",
    "Batch API (News)",
    "Batch API (The Verge)",
    "Unknown Source",
]


def generate_mock_articles(count: int) -> list:
    """
    Generate a list of mock article dictionaries.
    Injects NULL/empty values randomly to test data quality rules:
    - 30% chance for NULL or empty title
    - 40% chance for NULL or empty content
    - 20% chance for NULL or empty source_name
    - 25% chance for NULL or empty author
    - 15% chance for NULL or empty published_at
    """
    articles = []
    for _ in range(count):
        # ----- 1. Title (30% chance of being NULL or empty) -----
        if random.random() < 0.3:
            title = None if random.random() < 0.5 else ""
        else:
            topic = random.choice(TECH_TOPICS)
            action = random.choice(ACTIONS)
            feature = random.choice(FEATURES)
            title = f"{topic} {action} {feature}"

        # ----- 2. Content (40% chance of being NULL or empty) -----
        if random.random() < 0.4:
            content = None if random.random() < 0.5 else ""
        else:
            content_chance = random.random()
            if content_chance < 0.4:
                content = f"Full content for {title if title else 'Untitled'}"
            elif content_chance < 0.7:
                content = f"Mock description for {title if title else 'Untitled'}"
            else:
                content = "No content available"

        # ----- 3. Source Name (20% chance of being NULL or empty) -----
        if random.random() < 0.2:
            source_name = None if random.random() < 0.5 else ""
        else:
            source_name = random.choice(SOURCES)

        # ----- 4. Author (25% chance of being NULL or empty) -----
        if random.random() < 0.25:
            author = None if random.random() < 0.5 else ""
        else:
            author = random.choice(AUTHORS)

        # ----- 5. Published At (15% chance of being NULL) -----
        if random.random() < 0.15:
            published_at = None
        else:
            published_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        articles.append(
            {
                "title": title,
                "content": content,
                "source_name": source_name,
                "author": author,
                "published_at": published_at,
            }
        )

    # Log null/empty statistics for observability
    null_titles = sum(1 for a in articles if a["title"] in (None, ""))
    null_sources = sum(1 for a in articles if a["source_name"] in (None, ""))
    null_authors = sum(1 for a in articles if a["author"] in (None, ""))
    logger.info(
        f"Generated {len(articles)} mock articles with injected nulls: "
        f"Titles={null_titles}, Sources={null_sources}, Authors={null_authors}"
    )
    return articles


def create_spark_session() -> SparkSession:
    """Initialize and return a Spark session with Iceberg REST catalog."""
    return (
        SparkSession.builder.appName("BatchDynamicSimulationIngestion")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.rest_prod", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest_prod.type", "rest")
        .config("spark.sql.catalog.rest_prod.uri", ICEBERG_REST_URI)
        .config(
            "spark.sql.catalog.rest_prod.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .config("spark.sql.catalog.rest_prod.warehouse", HDFS_WAREHOUSE)
        .getOrCreate()
    )


def main() -> None:
    """Main execution entry point."""
    spark = None
    try:
        # Step 1: Initialize Spark
        logger.info("Initializing Spark session...")
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully.")

        # Step 2: Generate mock data with nulls
        run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        logger.info(f"Generating {BATCH_SIZE} mock articles (Run ID: {run_id})...")
        articles = generate_mock_articles(BATCH_SIZE)

        if not articles:
            logger.warning("No articles generated. Exiting.")
            return

        # Step 3: Convert to DataFrame and ingest to Bronze
        json_strings = [json.dumps(art, default=str) for art in articles]
        raw_rdd = spark.sparkContext.parallelize(json_strings)
        batch_df = spark.read.json(raw_rdd)

        bronze_batch_df = batch_df.withColumn(
            "source_type", lit("Batch-Simulation")
        ).select(
            expr(
                "to_json(named_struct("
                + ", ".join([f"'{c}', `{c}`" for c in batch_df.columns])
                + "))"
            ).alias("raw_payload"),
            col("source_type"),
            current_timestamp().alias("ingested_at"),
        )

        logger.info(f"Writing {bronze_batch_df.count()} records to {TARGET_BRONZE_TABLE}...")
        (
            bronze_batch_df.write.format("iceberg")
            .mode("append")
            .saveAsTable(TARGET_BRONZE_TABLE)
        )

        logger.info(
            f"Run ID: {run_id} - Batch successfully ingested into {TARGET_BRONZE_TABLE}"
        )

    except Exception as e:
        logger.error(f"Batch ingestion failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()