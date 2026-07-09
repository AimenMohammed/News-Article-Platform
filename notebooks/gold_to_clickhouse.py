import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================
# 1. SPARK SESSION CONFIGURATION
# ==========================================
print("=== Starting Gold Layer to ClickHouse Complete Migration ===")

spark = (
    SparkSession.builder.appName("GoldToClickHouseComplete")
    .config(
        "spark.jars",
        "/home/jovyan/.ivy2/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
        "/home/jovyan/.ivy2/jars/kafka-clients-3.4.1.jar,"
        "/home/jovyan/.ivy2/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
        "/home/jovyan/.ivy2/jars/commons-pool2-2.11.1.jar",
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


# ==========================================
# 2. CLICKHOUSE SCHEMA INITIALIZATION
# ==========================================
def initialize_clickhouse_schemas(client):
    """Creates the target database and all 8 data mart tables if they do not exist."""
    client.command("CREATE DATABASE IF NOT EXISTS news_analytics")

    schemas_creation = [
    # Table 1: Base Daily Statistics
    "CREATE TABLE IF NOT EXISTS news_analytics.daily_stats "
    "(report_date Date, source_name String, article_count UInt32) "
    "ENGINE = MergeTree() ORDER BY (report_date, source_name)",
    
    # Table 2: Granular Detailed Latest News
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_latest_news "
    "(article_id String, title String, source_name String, category String, "
    "published_at DateTime, sentiment String, url String) "
    "ENGINE = MergeTree() ORDER BY (published_at, source_name)",
    
    # Table 3: Daily Categorized Trends with Sentiment
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_daily_trends "
    "(report_date Date, source_name String, category String, "
    "article_count UInt32, avg_sentiment_score Float32) "
    "ENGINE = MergeTree() ORDER BY (report_date, source_name, category)",
    
    # Table 4: Trending Keywords
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_top_keywords "
    "(report_date Date, keyword String, mention_count UInt32) "
    "ENGINE = MergeTree() ORDER BY (report_date, keyword)",
    
    # Table 5: Media Source Reliability KPI Metrics
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_source_reliability "
    "(report_month Date, source_name String, total_articles_published UInt32, "
    "avg_title_length Float32, distinct_categories_covered UInt32, last_updated_at DateTime) "
    "ENGINE = MergeTree() ORDER BY (report_month, source_name)",
    
    # Table 6: Breaking News Alerts and Anomaly Records
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_breaking_news_alerts "
    "(alert_timestamp DateTime, trigger_entity String, normal_hourly_avg Float32, "
    "current_hourly_count UInt32, spike_percentage Float32, status String) "
    "ENGINE = MergeTree() ORDER BY (alert_timestamp, trigger_entity)",
    
    # Table 7: Hourly Traffic Density for Heatmaps
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_hourly_traffic_density "
    "(day_of_week UInt32, hour_of_day UInt32, category String, processed_articles_count UInt32) "
    "ENGINE = MergeTree() ORDER BY (day_of_week, hour_of_day, category)",
    
    # Table 8: Cross-Source Topic Clusters and Coverage Spread
    "CREATE TABLE IF NOT EXISTS news_analytics.gold_cross_source_matching "
    "(report_date Date, clustered_story_topic String, number_of_sharing_sources UInt32, total_coverage_volume UInt32) "
    "ENGINE = MergeTree() ORDER BY (report_date, clustered_story_topic)"
]

    for create_sql in schemas_creation:
        client.command(create_sql)
    print("ClickHouse database and schemas initialized successfully.")


# ==========================================
# 3. MIGRATION HELPER FUNCTION
# ==========================================
def migrate_table(
    client,
    iceberg_table_name,
    clickhouse_table_name,
    columns,
    date_cols=None,
    timestamp_cols=None,
):
    """Loads a table from Iceberg Gold Layer, casts formats, and inserts into ClickHouse."""
    print(f"Reading data from Gold Layer ({iceberg_table_name})...")
    df = spark.read.format("iceberg").load(iceberg_table_name)

    if date_cols:
        for c in date_cols:
            df = df.withColumn(c, F.col(c).cast("date"))
    if timestamp_cols:
        for c in timestamp_cols:
            df = df.withColumn(c, F.col(c).cast("timestamp"))

    rows = df.select(columns).collect()
    data_to_insert = [tuple(row[c] for c in columns) for row in rows]

    print(f"Loaded {len(data_to_insert)} rows from Iceberg.")

    if len(data_to_insert) > 0:
        print(f"Inserting data into ClickHouse ({clickhouse_table_name})...")
        client.insert(clickhouse_table_name, data_to_insert, column_names=columns)
        print(f"Migration for {clickhouse_table_name} finished.")
    else:
        print(f"No data found for {clickhouse_table_name}. Skipping insert.")


# ==========================================
# 4. EXECUTION PIPELINE
# ==========================================
try:
    # Connect to ClickHouse
    ch_client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="clickhouse",
        password="clickhouse",
    )
    print("Successfully connected to ClickHouse server.")

    # Initialize Schemas
    initialize_clickhouse_schemas(ch_client)

    # Migrate Tables Sequentially
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.news_daily_stats",
        "news_analytics.daily_stats",
        ["report_date", "source_name", "article_count"],
        ["report_date"],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_latest_news",
        "news_analytics.gold_latest_news",
        [
            "article_id",
            "title",
            "source_name",
            "category",
            "published_at",
            "sentiment",
            "url",
        ],
        None,
        ["published_at"],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_daily_trends",
        "news_analytics.gold_daily_trends",
        [
            "report_date",
            "source_name",
            "category",
            "article_count",
            "avg_sentiment_score",
        ],
        ["report_date"],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_top_keywords",
        "news_analytics.gold_top_keywords",
        ["report_date", "keyword", "mention_count"],
        ["report_date"],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_source_reliability",
        "news_analytics.gold_source_reliability",
        [
            "report_month",
            "source_name",
            "total_articles_published",
            "avg_title_length",
            "distinct_categories_covered",
            "last_updated_at",
        ],
        ["report_month"],
        ["last_updated_at"],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_breaking_news_alerts",
        "news_analytics.gold_breaking_news_alerts",
        [
            "alert_timestamp",
            "trigger_entity",
            "normal_hourly_avg",
            "current_hourly_count",
            "spike_percentage",
            "status",
        ],
        None,
        ["alert_timestamp"],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_hourly_traffic_density",
        "news_analytics.gold_hourly_traffic_density",
        [
            "day_of_week",
            "hour_of_day",
            "category",
            "processed_articles_count",
        ],
    )
    migrate_table(
        ch_client,
        "rest_prod.gold_layer.gold_cross_source_matching",
        "news_analytics.gold_cross_source_matching",
        [
            "report_date",
            "clustered_story_topic",
            "number_of_sharing_sources",
            "total_coverage_volume",
        ],
        ["report_date"],
    )

    print(
        "All Gold Layer data marts migration to ClickHouse completed successfully."
    )

except Exception as e:
    print(f"An error occurred during data migration: {e}")

finally:
    spark.stop()
    print("Spark session stopped.")