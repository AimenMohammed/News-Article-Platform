# 📊 News Article Categorization & Trend Analysis Platform

> A production-grade data platform for ingesting, categorizing, and analyzing news articles to track emerging trends, measure sentiment, and monitor media coverage across multiple sources.

---

## 📋 Overview

This project implements a **complete data engineering pipeline** following the **Medallion Architecture** (Bronze, Silver, Gold). It serves as a comprehensive solution for media monitoring, content recommendation, and market intelligence applications.

**Key Capabilities:**
- **Multi-Source Ingestion**: Automated web scraping via NiFi → Kafka, direct web scraping (Hacker News), and batch simulation.
- **Data Quality & Enrichment**: Automated cleaning, deduplication, sentiment analysis, and category classification.
- **Analytical Data Marts**: 8 pre-built tables for trend analysis, source reliability, and anomaly detection.
- **Serving & Visualization**: High-performance OLAP queries via ClickHouse and interactive dashboards in Grafana.
- **Production Orchestration**: Reliable scheduling and execution via Apache Airflow with automatic retries.

---

## 🏗️ Architecture

```mermaid
flowchart LR
    subgraph Sources["Data Sources"]
        WS[External Websites / News APIs] --> NF[Apache NiFi]
        NF --> K[Kafka]
        K --> B[Bronze Layer]
        
        C[Web Scraper] --> B
        D[Batch Simulator] --> B
    end

    subgraph Processing["Processing Layers"]
        B --> E[Silver Layer\n(Cleaning, Enrichment, Classification)]
        E --> F[Gold Layer\n(8 Analytical Data Marts)]
    end

    subgraph Serving["Serving Layer"]
        F --> G[ClickHouse\n(Analytics Warehouse)]
        G --> H[Grafana\n(Interactive Dashboards)]
    end

    subgraph Orchestration["Orchestration & Monitoring"]
        I[Apache Airflow] --> B
        I --> E
        I --> F
        I --> G
        J[Prometheus] --> H
    end

Data Flow Breakdown:

1- External Ingestion: Apache NiFi continuously polls external news websites or APIs (configurable via its web UI), scrapes
   the content, and publishes the raw data to the news-raw Kafka topic.

2- Streaming Ingestion: The Spark Structured Streaming job (bronze_inge1stion.py) consumes these messages in micro-batches
   and writes them to the Bronze Iceberg table.

3- Batch/Backfill: The scrape_news.py (direct HTTP scraping) and ingest_news_batch.py (mock data generation) act as 
   additional ingestion sources for testing and historical backfill.

4- Transformation & Serving: The Silver and Gold layers clean, enrich, and aggregate the data before serving it via 
   ClickHouse and Grafana.

🛠️ Tech Stack
   Component	            Technology	         Version	   Purpose
   Data Flow & Ingestion	Apache NiFi	         1.25.0	   Pull-based web scraping, data routing, and pushing to Kafka.
Message Queue	         Apache Kafka	         3.7.0	      Decoupled, high-throughput message broker for streaming data.
   Processing Engine	      Apache Spark	      3.5.0	      Unified batch and stream processing (Structured Streaming).
   Storage Format	         Apache Iceberg	      1.5.0	      ACID-compliant tables with time travel and schema evolution.
   Metadata Catalog	      Iceberg REST Catalog	latest	   Centralized table metadata management.
   Analytics DB	         ClickHouse	         Latest	   High-speed OLAP queries for dashboards.
   Distributed FS	         HDFS	               3.3.6	      Underlying storage for Iceberg.
   Orchestration	         Apache Airflow	      2.9.1	      Workflow scheduling, task automation, and retry logic.
   Containerization	      Docker & Compose	   29.4.1	   Container orchestration and isolation.
   Monitoring	            Prometheus + Grafana	Latest	   Metrics collection and visualization.

🔄 Data Pipeline
1. Bronze Layer (Raw Data)
   Sources:

      NiFi → Kafka → bronze_ingestion.py: Consumes streaming data from the news-raw Kafka topic (populated by NiFi).

      scrape_news.py: Extracts 90 articles from Hacker News (3 pages) as a batch job.

      ingest_news_batch.py: Generates 50 mock articles with intentionally injected NULL/empty values to test data quality rules.

   Storage: Table rest_prod.bronze_layer.news_bronze (partitioned by days(ingested_at)).

Schema: 

   raw_payload STRING, ingested_at TIMESTAMP, source_type STRING

2. Silver Layer (Cleaned & Enriched Data)

   Transformations:

      Parse JSON payloads.

      Extract and standardize fields.

      Clean source names (fallback to "Unknown Source").

      Classify categories: Hardware & AI, Apple Ecosystem, General Tech.

      Assign sentiment: Positive, Neutral, Negative.

      Strict Data Quality Rule: Remove any record with a NULL or empty title.

      Deduplicate based on title (with watermarking).

   Storage: Table rest_prod.silver_layer.news_silver (partitioned by source_type and days(ingested_at)).

3. Gold Layer (Analytical Data Marts)

   Eight purpose-built analytical tables are generated:

      Table	                           Description
      news_daily_stats	               Daily article counts per source.
      gold_latest_news	               Granular live news feed with full details.
      gold_daily_trends	               Daily trends with average sentiment scores per category.
      gold_top_keywords	               Trending keywords and their mention counts.
      gold_source_reliability	         Source performance KPIs (diversity, title length).
      gold_breaking_news_alerts	      Anomaly detection for sudden topic surges.
      gold_hourly_traffic_density	   Heatmap data for article volume by hour and category.
      gold_cross_source_matching	      Cross-source story coverage and cluster volume.

4. Serving Layer (ClickHouse)

   All Gold tables are migrated to ClickHouse into the news_analytics database.

   Optimized for fast, real-time dashboard queries.

5. Orchestration (Airflow)

   Three main DAGs ensure fully automated pipeline execution:

      1_streaming_trigger_dag: Runs every 5 minutes (streaming micro-batches).

      2_scheduled_batch_dag: Runs every 15 minutes (scraping and batch simulation).

      3_gold_layer_update: Runs every 15 minutes (Gold transformations and ClickHouse loading).


🚀 Getting Started
   Prerequisites

      Docker & Docker Compose installed.

      make utility (pre-installed on most Linux/macOS systems).

      8GB+ RAM (16GB recommended).

   1. Start the Infrastructure

      # Start core services (HDFS, Kafka, Iceberg REST)

         make up-bigdata

         # Start Spark Cluster and Jupyter
         make up-spark

         # Start NiFi for external streaming data ingestion
         make up-nifi

         # Start Airflow, ClickHouse, and Monitoring (optional)
         make up-airflow
         make up-clickhouse
         make up-monitoring

   2. Initialize the Catalog and Tables

      make base   # Cleans HDFS and the catalog, then rebuilds empty tables

   3. Run the End-to-End Pipeline

      make try    # Executes: run-bronze → run-silver → run-gold → load-clickhouse

   4. Access the Services

      Service	      URL	                     Credentials
      NiFi UI	      https://localhost:8443	   admin / SuperSecretPassword123!
      Airflow	      http://localhost:8085	   admin / admin
      Jupyter Lab	   http://localhost:8888	   (No auth)
      Spark Master	http://localhost:8081	   -
      ClickHouse	   http://localhost:8123	   clickhouse / clickhouse
      Kafka UI	      http://localhost:8090	   -
      HDFS NameNode	http://localhost:9870	   -
      Grafana	      http://localhost:3000	   admin / admin

   5. Shutdown

      # Stop containers while preserving all data (recommended)

         docker compose down

      # Stop containers and delete all data (full reset)

         docker compose down -v   # Use with extreme caution!

📊 Grafana Dashboards

   Three interactive dashboards have been designed to cover all analytical needs:

   1. Content Analytics & Trends

      Word Cloud for the most frequent keywords.

      Time-series chart tracking sentiment evolution across categories.

      Donut chart showing topic coverage distribution.

   2. Operations & Live Pulse

      Heatmap visualizing article density by hour and category.

      Alert table displaying breaking news anomalies with spike percentages.

      Live feed showing the latest 15 articles.

   3. Source Reliability & KPIs

      Bar chart ranking sources by total article output.

      Comparative charts for average title length and category diversity per source

      Note: These dashboards are saved as JSON files in grafana/provisioning/dashboards/. They are automatically re-created whenever Grafana starts, ensuring zero loss of visualization configurations.

📈 Results & Statistics (Post make try)

   ClickHouse Table	            Record Count	 Notes
   daily_stats	                     209	          Daily stats per source
   gold_latest_news	               302	       Cleaned, enriched news (empty titles removed)
   gold_daily_trends	               224	          Daily trends with sentiment
   gold_top_keywords	               1089	       Extracted trending keywords
   gold_source_reliability	         199	          Source reliability metrics
   gold_breaking_news_alerts	      227	       Anomaly alerts
   gold_hourly_traffic_density	   101	          Hourly traffic heatmap data
   gold_cross_source_matching	      302	       Cross-source story coverage

   Data Quality Validation:

      ✅ Zero records with NULL or empty titles in Silver/Gold layers.

      ✅ All missing source_name values defaulted to "Unknown Source".

      ✅ All missing author values defaulted to "Unknown Author".

      ✅ Missing published_at values fall back to bronze_ingested_at.

🧠 Challenges & Solutions

   Challenge	                        Solution

   Catalog mismatches with HDFS	      Implemented make base to reinitialize both the catalog
                                       and storage simultaneously.

   Loss of Grafana dashboards	         Implemented provisioning by storing dashboards as JSON
                                       files (grafana/provisioning/dashboards/).

   Dirty data (NULLs/empties)	         Applied strict data quality rules (e.g., filtering empty titles)
                                       within the Silver transformation.

   Slow ClickHouse queries	            Optimized using appropriate ORDER BY keys and table partitioning.

   Task overlapping in Airflow	      Set max_active_runs=1 to prevent concurrent DAG executions.

   NiFi integration setup	            Exposed NiFi on localhost:8443 and configured it to write to the internal Kafka broker.

📁 Project Structure
   News_Article_Platform/
   ├── Makefile                           # Management commands (up, down, run-*)
   ├── docker-compose.yaml                # Full multi-service Docker definition
   ├── assets/
│   └── screenshots/
│       ├── 1_trends_dashboard.png
│       ├── 2_ops_dashboard.png
│       └── 3_reliability_dashboard.png
   ├── conf/      
   │   ├── core-site.xml                  # HDFS configuration
   │   ├── hdfs-site.xml      
   │   └── spark-defaults.conf            # Spark configuration
   ├── dags/                              # Airflow DAGs
   │   ├── 1_streaming_trigger_dag.py
   │   ├── 2_scheduled_batch_dag.py
   │   ├── 3_gold_layer_update.py
   │   └── iceberg_maintenance.py
   ├── notebooks/                         # PySpark scripts
   │   ├── bronze_ingestion.py
   │   ├── scrape_news.py
   │   ├── ingest_news_batch.py
   │   ├── silver_transformation.py
   │   ├── gold_transformation.py
   │   ├── gold_to_clickhouse.py
   │   └── rebuild_tables.py
   ├── grafana/
   │   └── provisioning/
   │       ├── datasources/
   │       │   └── clickhouse.yaml
   │       └── dashboards/
   │           ├── trends_dashboard.json
   │           ├── ops_dashboard.json
   │           └── reliability_dashboard.json
   └── monitoring/
      └── prometheus.yml

👥 Team

   Aimen Al-Faqih – Data Engineer & Pipeline Architect.

📝 Evaluation Criteria

   Criterion	                  Status	  Notes
   Multi-Source Ingestion	      ✅	       Streaming (NiFi → Kafka), Scraping, Batch Simulation
   Batch & Stream Processing	   ✅	       Spark Structured Streaming + Batch Jobs
   Data Transformation	         ✅	       Cleaning, dedup, sentiment, classification
   Advanced Storage	            ✅	       Iceberg (ACID, Partitioning, Time Travel)
   Orchestration	               ✅	       Airflow with retries and scheduling
   Data Quality	               ✅	       Strict filters, null handling, validation
   Serving & Dashboards	         ✅	       3 interactive Grafana dashboards
   Infrastructure as Code	      ✅	       Docker Compose + Provisioning
   Monitoring	                  ✅	       Prometheus + Grafana

