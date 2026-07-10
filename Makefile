# =====================================================================
# Makefile for Platform Management and Medallion Data Pipelines
# =====================================================================

.PHONY: up-nifi down-nifi up-bigdata down-bigdata up-spark down-spark \
        up-clickhouse down-clickhouse up-airflow down-airflow \
        up-monitoring down-all ps try base \
        run-bronze run-silver run-gold load-clickhouse

# -----------------------------------------------------------------
# 1. NiFi Stream (Data Ingestion and Streaming Tool)
# Active Containers: nifi
# -----------------------------------------------------------------
up-nifi:
	docker compose --profile nifi-stream up -d

down-nifi:
	docker compose --profile nifi-stream down

# -----------------------------------------------------------------
# 2. Big Data Core (Kafka + Hadoop + Iceberg REST)
# Active Containers: kafka, kafka-ui, namenode, secondarynamenode, datanode, 
#                    resourcemanager, nodemanager, historyserver, iceberg-rest
# -----------------------------------------------------------------
up-bigdata:
	docker compose --profile bigdata-core up -d

down-bigdata:
	docker compose --profile bigdata-core down

# -----------------------------------------------------------------
# 3. Processing (Spark Cluster + Jupyter Notebook)
# Active Containers: spark-master, spark-worker, jupyter-spark
# -----------------------------------------------------------------
up-spark:
	docker compose --profile processing up -d

down-spark:
	docker compose --profile processing down

# -----------------------------------------------------------------
# 4. Analytics Store (ClickHouse Server)
# Active Containers: clickhouse, postgres-airflow (shared helper)
# -----------------------------------------------------------------
up-clickhouse:
	docker compose --profile analytics up -d

down-clickhouse:
	docker compose --profile analytics down

# -----------------------------------------------------------------
# 5. Orchestration (Apache Airflow Platform)
# Active Containers: postgres-airflow, airflow-init, airflow-webserver, 
#                    airflow-scheduler, airflow-triggerer
# -----------------------------------------------------------------
up-airflow:
	docker compose --profile orchestration up -d
	sudo chmod 666 /var/run/docker.sock

down-airflow:
	docker compose --profile orchestration down

# -----------------------------------------------------------------
# 6. Monitoring Tools (Prometheus & Grafana)
# Active Containers: prometheus, grafana
# -----------------------------------------------------------------
up-monitoring:
	docker compose --profile monitoring up -d

down-monitoring:
	docker compose --profile monitoring down

# -----------------------------------------------------------------
# Global Operations and Resource Cleanup
# -----------------------------------------------------------------
down-all:
	docker compose --profile nifi-stream --profile bigdata-core --profile processing --profile analytics --profile orchestration --profile monitoring down

ps:
	docker compose ps

# =====================================================================
# Data Environment Setup Tasks
# =====================================================================

# -----------------------------------------------------------------
# Infrastructure Base Setup (Safe Reset, Purge, and Table Rebuild)
# -----------------------------------------------------------------
base:
	@echo "Starting safe environment cleanup..."
	# 1. Stop catalog server temporarily to cut open connections
	docker compose down iceberg-rest
	# 2. Remove warehouse and checkpoint directories from HDFS safely
	docker exec -it namenode hdfs dfs -rm -r -f /user/iceberg/warehouse
	docker exec -it namenode hdfs dfs -rm -r -f /user/iceberg/checkpoints
	# 3. Clean catalog tables inside backend database to avoid schema conflicts
	docker exec -it postgres-airflow psql -U airflow -d airflow_db -c "TRUNCATE TABLE iceberg_tables, iceberg_namespace_properties CASCADE;" 2>/dev/null || true
	# 4. Restart catalog server
	docker compose up -d iceberg-rest
	@echo "Waiting for catalog initialization..."
	sleep 5
	# 5. Recreate root directory and grant full permissions
	docker exec -it namenode hdfs dfs -mkdir -p /user/iceberg/warehouse
	docker exec -it namenode hdfs dfs -chmod -R 777 /user/iceberg
	# 6. Run Spark script to rebuild clean tables from scratch
	docker exec -it spark-master spark-submit /home/jovyan/work/rebuild_tables.py

# =====================================================================
# Pipeline Ingestion and Processing Execution (Medallion Architecture)
# =====================================================================

# -----------------------------------------------------------------
# 1. Bronze Layer: Ingest data from the three primary sources
# -----------------------------------------------------------------
run-bronze:
	@echo "======= [1/4] Ingesting data from 3 sources to Bronze Layer ======="
	docker exec -it spark-master spark-submit /home/jovyan/work/bronze_ingestion.py
	docker exec -it spark-master spark-submit /home/jovyan/work/scrape_news.py
	docker exec -it spark-master spark-submit /home/jovyan/work/ingest_news_batch.py

# -----------------------------------------------------------------
# 2. Silver Layer: Data Cleaning and Transformation from Bronze to Silver
# -----------------------------------------------------------------
run-silver:
	@echo "======= [2/4] Processing and transforming data from Bronze to Silver ======="
	docker exec -it spark-master spark-submit /home/jovyan/work/silver_transformation.py

# -----------------------------------------------------------------
# 3. Gold Layer: Aggregation and Aggregated Data Mart Preparation
# -----------------------------------------------------------------
run-gold:
	@echo "======= [3/4] Aggregating and preparing data from Silver to Gold ======="
	docker exec -it spark-master spark-submit /home/jovyan/work/gold_transformation.py

# -----------------------------------------------------------------
# 4. Analytics Store: Ship finalized Data Marts into ClickHouse
# -----------------------------------------------------------------
load-clickhouse:
	@echo "======= [4/4] Loading analytical data from Gold to ClickHouse ======="
	docker exec -it spark-master spark-submit /home/jovyan/work/gold_to_clickhouse.py

# -----------------------------------------------------------------
# Sequential Execution of the End-to-End Medallion Pipeline
# -----------------------------------------------------------------
try: run-bronze run-silver run-gold load-clickhouse
	@echo "All pipeline stages executed successfully. Data loaded into ClickHouse."

	iceberg_maintenance.py