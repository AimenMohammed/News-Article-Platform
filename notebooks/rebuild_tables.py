from pyspark.sql import SparkSession

# Initialize Spark Session with Unified Iceberg Catalog configurations
spark = SparkSession.builder \
    .appName('AutomatedIcebergTablesRebuild') \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config('spark.sql.catalog.rest_prod', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.rest_prod.type', 'rest') \
    .config('spark.sql.catalog.rest_prod.uri', 'http://iceberg-rest:8181') \
    .config('spark.sql.catalog.rest_prod.warehouse', 'hdfs://namenode:9000/user/iceberg/warehouse') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting the creation of databases and unified tables...")

# Create Iceberg Architecture Databases
spark.sql('CREATE DATABASE IF NOT EXISTS rest_prod.bronze_layer')
spark.sql('CREATE DATABASE IF NOT EXISTS rest_prod.silver_layer')
spark.sql('CREATE DATABASE IF NOT EXISTS rest_prod.gold_layer')


# ==========================================
# 1. BRONZE & SILVER LAYERS DEFINITIONS
# ==========================================

# Bronze Layer Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.bronze_layer.news_bronze (
        raw_payload STRING,
        ingested_at TIMESTAMP,
        source_type STRING
    )
    USING iceberg
    PARTITIONED BY (days(ingested_at))
''')

# Silver Layer Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.silver_layer.news_silver (
        title STRING,
        content STRING,
        source_name STRING,
        source_type STRING,  -- Ingestion Source Classification (Web Scraper, Batch, Stream)
        author STRING,
        published_at TIMESTAMP,
        ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (source_type, days(ingested_at)) -- Composite partitioning for faster filtering and reporting
''')


# ==========================================
# 2. GOLD LAYER DATA MARTS (Iceberg)
# ==========================================

# Base Daily Stats Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.news_daily_stats (
        report_date DATE,
        source_name STRING,
        article_count BIGINT
    )
    USING iceberg
''')

# Enriched Granular Latest News Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_latest_news (
        article_id STRING,
        title STRING,
        source_name STRING,
        category STRING,
        published_at TIMESTAMP,
        sentiment STRING,
        url STRING
    )
    USING iceberg
''')

# Daily Categorized Trends Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_daily_trends (
        report_date DATE,
        source_name STRING,
        category STRING,
        article_count BIGINT,
        avg_sentiment_score FLOAT
    )
    USING iceberg
''')

# Top Keywords Extraction Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_top_keywords (
        report_date DATE,
        keyword STRING,
        mention_count BIGINT
    )
    USING iceberg
''')

# Media Source Reliability Metrics Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_source_reliability (
        report_month DATE,
        source_name STRING,
        total_articles_published BIGINT,
        avg_title_length FLOAT,
        distinct_categories_covered INT,
        last_updated_at TIMESTAMP
    )
    USING iceberg
''')

# Breaking News Alerts and Anomalies Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_breaking_news_alerts (
        alert_timestamp TIMESTAMP,
        trigger_entity STRING,
        normal_hourly_avg FLOAT,
        current_hourly_count BIGINT,
        spike_percentage FLOAT,
        status STRING
    )
    USING iceberg
''')

# Hourly Traffic Density Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_hourly_traffic_density (
        day_of_week INT,
        hour_of_day INT,
        category STRING,
        processed_articles_count BIGINT
    )
    USING iceberg
''')

# Cross-Source Story Clustered Matching Gold Table
spark.sql('''
    CREATE TABLE IF NOT EXISTS rest_prod.gold_layer.gold_cross_source_matching (
        report_date DATE,
        clustered_story_topic STRING,
        number_of_sharing_sources INT,
        total_coverage_volume BIGINT
    )
    USING iceberg
''')

print("Table catalog structure deployment completed successfully.")