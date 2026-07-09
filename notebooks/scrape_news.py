from datetime import datetime
import json
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit
import requests

# 1. Initialize Spark Session
spark = (
    SparkSession.builder.appName('WebScrapingMultiPageIngestion')
    .config(
        'spark.sql.extensions',
        'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    )
    .config(
        'spark.sql.catalog.rest_prod', 'org.apache.iceberg.spark.SparkCatalog'
    )
    .config('spark.sql.catalog.rest_prod.type', 'rest')
    .config('spark.sql.catalog.rest_prod.uri', 'http://iceberg-rest:8181')
    .config(
        'spark.sql.catalog.rest_prod.io-impl',
        'org.apache.iceberg.hadoop.HadoopFileIO',
    )
    .config(
        'spark.sql.catalog.rest_prod.warehouse',
        'hdfs://namenode:9000/user/iceberg/warehouse',
    )
    .config(
        'spark.sql.warehouse.dir',
        'hdfs://namenode:9000/user/iceberg/warehouse',
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

articles = []
run_id = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Define the number of pages to extract (e.g., 3 pages)
base_url = 'https://news.ycombinator.com/'
current_page_url = base_url
num_pages_to_scrape = 3

for page in range(num_pages_to_scrape):
    print(f'Scraping data from page number {page + 1}...')
    response = requests.get(
        current_page_url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
        },
    )

    if response.status_code != 200:
        print(f'Error loading page: {response.status_code}')
        break

    soup = BeautifulSoup(response.text, 'html.parser')
    story_links = soup.select('.athing')
    subtexts = soup.select('.subtext')

    for idx, story in enumerate(story_links):
        title_element = story.select_one('.titleline > a')
        if not title_element:
            continue
        title = title_element.get_text()
        link = title_element.get('href')

        site_element = story.select_one('.sitestr')
        if site_element:
            source_name = f'Web Scraper ({site_element.get_text()})'
        else:
            source_name = 'Web Scraper (Hacker News)'

        author = 'Unknown Author'
        points = '0 points'
        comments = '0 comments'
        published_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if idx < len(subtexts):
            subtext = subtexts[idx]
            user_element = subtext.select_one('.hnuser')
            if user_element:
                author = user_element.get_text()

            score_element = subtext.select_one('.score')
            if score_element:
                points = score_element.get_text()

            comment_element = (
                subtext.select('a')[-1] if subtext.select('a') else None
            )
            if comment_element and 'comment' in comment_element.get_text():
                comments = comment_element.get_text()
            else:
                comments = '0 comments'

            age_element = subtext.select_one('.age')
            if age_element and age_element.get('title'):
                raw_time = age_element.get('title').split(' ')[0]
                try:
                    published_at = datetime.strptime(
                        raw_time, '%Y-%m-%dT%H:%M:%S'
                    ).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    published_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        article_dict = {
            'title': title,
            'url': link,
            'description': f'Article by {author} with {points} and {comments}.',
            'source_name': source_name,
            'author': author,
            'publishedAt': published_at,
        }
        articles.append(article_dict)

    # Locate the pagination link for the next page
    more_link = soup.select_one('.morelink')
    if more_link and more_link.get('href'):
        current_page_url = base_url + more_link.get('href')
    else:
        break

print(
    f'Successfully extracted a total of {len(articles)} articles from {num_pages_to_scrape} pages for Run ID: {run_id}'
)

# 3. Persist the aggregated data into the Bronze layer table using Spark
if articles:
    json_strings = [json.dumps(art) for art in articles]
    raw_rdd = spark.sparkContext.parallelize(json_strings)
    api_df = spark.read.json(raw_rdd)

    bronze_scraper_df = api_df.withColumn(
        'source_type', lit('Web-Scraper')
    ).select(
        expr(
            'to_json(named_struct('
            + ', '.join([f"'{c}', `{c}`" for c in api_df.columns])
            + '))'
        ).alias('raw_payload'),
        col('source_type'),
        current_timestamp().alias('ingested_at'),
    )

    (
        bronze_scraper_df.write.format('iceberg')
        .mode('append')
        .saveAsTable('rest_prod.bronze_layer.news_bronze')
    )

    print('Successfully saved all multi-page scraping records to the Bronze layer table.')