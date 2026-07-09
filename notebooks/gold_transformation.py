from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, current_timestamp, date_format, dayofweek, explode, hour, length, lit, split, to_date, when

print("=== Starting Gold Layer Transformation Pipeline ===")

# Initialize production-grade Spark Session with Iceberg REST catalog configurations
spark = (
    SparkSession.builder.appName("GoldTransformationEnriched")
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
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.rpc.askTimeout", "600s")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Load Data from Cleaned Silver Iceberg Table
silver_df = spark.read.format("iceberg").load("rest_prod.silver_layer.news_silver")

# Basic transformations and safe date/time components parsing
# Removed date_format string tokens entirely for day_of_week and swapped to native Spark function
enriched_silver = (
    silver_df.withColumn("report_date", to_date(col("published_at")))
    .withColumn(
        "report_month", date_format(col("published_at"), "yyyy-MM-01").cast("date")
    )
    .withColumn("hour_of_day", hour(col("published_at")))
    .withColumn("day_of_week", dayofweek(col("published_at")))
)

# Apply default column schema fallback configurations if fields are absent
if "category" not in enriched_silver.columns:
    enriched_silver = enriched_silver.withColumn("category", lit("General"))
if "sentiment" not in enriched_silver.columns:
    enriched_silver = enriched_silver.withColumn("sentiment", lit("Neutral"))
if "article_id" not in enriched_silver.columns:
    enriched_silver = enriched_silver.withColumn("article_id", lit("N/A"))
if "url" not in enriched_silver.columns:
    enriched_silver = enriched_silver.withColumn("url", lit("N/A"))


# ==========================================
# 2. PROCESSING GOLD DATA MARTS
# ==========================================

# Table 1: news_daily_stats (Base Aggregated Volume)
stats_df = enriched_silver.groupBy("report_date", "source_name").agg(
    count("*").alias("article_count")
)

# Table 2: gold_latest_news (Granular Presentation Layer Feed)
latest_news_df = enriched_silver.select(
    "article_id",
    "title",
    "source_name",
    "category",
    "published_at",
    "sentiment",
    "url",
)

# Table 3: gold_daily_trends (Dynamic Categorized Trends & Sentiment Metrics)
sentiment_numeric = (
    when(col("sentiment") == "Positive", 1.0)
    .when(col("sentiment") == "Negative", -1.0)
    .otherwise(0.0)
)

daily_trends_df = (
    enriched_silver.withColumn("sentiment_score", sentiment_numeric)
    .groupBy("report_date", "source_name", "category")
    .agg(
        count("*").alias("article_count"),
        avg("sentiment_score").cast("float").alias("avg_sentiment_score"),
    )
)

# Table 4: gold_top_keywords (Token Extraction Metrics)
top_keywords_df = (
    enriched_silver.withColumn("keyword", explode(split(col("title"), " ")))
    .filter(length(col("keyword")) > 4)
    .groupBy("report_date", "keyword")
    .agg(count("*").alias("mention_count"))
)

# Table 5: gold_source_reliability (Source Ingestion KPIs Profiles)
source_reliability_df = (
    enriched_silver.groupBy("report_month", "source_name")
    .agg(
        count("*").alias("total_articles_published"),
        avg(length(col("title"))).cast("float").alias("avg_title_length"),
        countDistinct("category").cast("int").alias("distinct_categories_covered"),
    )
    .withColumn("last_updated_at", current_timestamp())
)

# Table 6: gold_breaking_news_alerts (Anomaly Detection Simulators)
breaking_news_df = (
    enriched_silver.groupBy("published_at", "category")
    .agg(count("*").alias("current_hourly_count"))
    .withColumn("alert_timestamp", col("published_at"))
    .withColumn("trigger_entity", col("category"))
    .withColumn("normal_hourly_avg", lit(1.5).cast("float"))
    .withColumn("spike_percentage", lit(100.0).cast("float"))
    .withColumn("status", lit("ACTIVE"))
    .select(
        "alert_timestamp",
        "trigger_entity",
        "normal_hourly_avg",
        "current_hourly_count",
        "spike_percentage",
        "status",
    )
)

# Table 7: gold_hourly_traffic_density (Heatmap Visualization Dataset)
traffic_density_df = enriched_silver.groupBy(
    "day_of_week", "hour_of_day", "category"
).agg(count("*").alias("processed_articles_count"))

# Table 8: gold_cross_source_matching (Cross-Media Coverage Clustering Index)
cross_source_df = (
    enriched_silver.groupBy("report_date", "title")
    .agg(
        countDistinct("source_name")
        .cast("int")
        .alias("number_of_sharing_sources"),
        count("*").alias("total_coverage_volume"),
    )
    .withColumnRenamed("title", "clustered_story_topic")
)


# ==========================================
# 3. WRITING DATA TO ICEBERG GOLD TABLES
# ==========================================

print("Writing processed data frames into the Gold Layer Data Marts...")

stats_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.news_daily_stats"
)
latest_news_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_latest_news"
)
daily_trends_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_daily_trends"
)
top_keywords_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_top_keywords"
)
source_reliability_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_source_reliability"
)
breaking_news_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_breaking_news_alerts"
)
traffic_density_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_hourly_traffic_density"
)
cross_source_df.write.format("iceberg").mode("overwrite").saveAsTable(
    "rest_prod.gold_layer.gold_cross_source_matching"
)

print("All Gold Layer analytical tables have been populated successfully.")