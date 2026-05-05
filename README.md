# Cloud Data Engineering Projects

A comprehensive collection of data engineering projects demonstrating real-world implementations of cloud-based data pipelines, ETL processes, and analytics solutions.

## Projects Overview

### Real-Time Data Processing

#### 1. [Stock Market Kafka Project](./StockMarketKafkaProject)
Real-time stock market data pipeline using Apache Kafka, AWS EC2, S3, Glue, and Athena. Demonstrates event streaming and serverless analytics.

**Tech Stack:** Kafka, Python, AWS (EC2, S3, Glue, Athena)

#### 2. [FMP Pipeline with Airflow](./fmp-pipeline-airflow)
Financial data pipeline using Apache Airflow for orchestration and AWS MSK for real-time data streaming.

**Tech Stack:** Airflow, AWS MSK, Python

### ETL & Data Warehousing

#### 3. [ETL with Redshift, Glue & Step Functions](./etl-with-redshift-glue-stepfunction)
Serverless ETL pipeline orchestrated by AWS Step Functions, processing data through Glue and loading into Redshift for analytics.

**Tech Stack:** AWS (Redshift, Glue, Step Functions, S3), Python

#### 4. [SCD Data Warehousing with Snowflake](./scd-data-warehousing-with-snowflake)
Implementation of Slowly Changing Dimensions (SCD) Type 2 in Snowflake for historical data tracking.

**Tech Stack:** Snowflake, Python, SQL

#### 5. [ETL with Python - World's Largest Banks](./ETLwithPython(WorldLargestBankData))
ETL pipeline extracting, transforming, and loading data about the world's largest banks.

**Tech Stack:** Python, Pandas, SQL

#### 6. [Snowflake Data Loading](./SnowflakeLoadingData)
Demonstrates various data loading techniques and best practices in Snowflake.

**Tech Stack:** Snowflake, SQL

#### 7. [Snowflake S3 Lambda EventBridge](./SnowflakeS3LambdaEventBridgeRule)
Event-driven data pipeline using AWS Lambda and EventBridge to automatically load data into Snowflake when files arrive in S3.

**Tech Stack:** AWS (Lambda, EventBridge, S3), Snowflake, Python

### Workflow Orchestration

#### 8. [Ecommerce Airflow Data Pipeline](./EcommerceAirflowDataPipeline)
End-to-end ecommerce data pipeline orchestrated with Apache Airflow, processing customer and order data.

**Tech Stack:** Airflow, Snowflake, AWS S3, Python

#### 9. [Weather Airflow Docker ETL](./WeatherAirflowDockerETL)
Containerized ETL pipeline using Docker and Airflow to fetch and process weather data.

**Tech Stack:** Airflow, Docker, Python, Weather API

### Data Analytics & Visualization

#### 10. [End-to-End Data Analytics Project](./EndtoEndDataAnalyticsProject(Python+SQL))
Complete analytics workflow from data extraction to visualization using Python and SQL.

**Tech Stack:** Python, SQL, Pandas, Matplotlib

#### 11. [Netflix Movies Case Study](./CaseStudyInvestigatingNetflixMoviesandGuestStarsinTheOffice)
Data analysis project investigating Netflix movies and guest appearances in The Office series.

**Tech Stack:** Python, Pandas, Jupyter Notebook

### Web Scraping

#### 12. [PakWheels Data Scraping](./PakWheelsDataScraping)
Web scraping project extracting vehicle listings and pricing data from PakWheels.

**Tech Stack:** Python, BeautifulSoup, Selenium

#### 13. [CoinMarketCap Data Scraping](./CoinMarketCapDataScraping)
Automated cryptocurrency data extraction from CoinMarketCap for market analysis.

**Tech Stack:** Python, BeautifulSoup, Requests

## Technologies Used

- **Cloud Platforms:** AWS (EC2, S3, Lambda, Glue, Athena, Redshift, Step Functions), Snowflake
- **Data Processing:** Apache Kafka, Apache Airflow, Python, Pandas
- **Databases:** Snowflake, Amazon Redshift, SQL
- **Containerization:** Docker
- **Languages:** Python, SQL

## Getting Started

Each project contains its own README with detailed setup instructions, architecture diagrams, and usage guidelines. Navigate to individual project directories for specific documentation.

## Prerequisites

- Python 3.7+
- AWS Account (for cloud-based projects)
- Docker (for containerized projects)
- Basic understanding of data engineering concepts

## Project Structure

Each project follows a consistent structure:
```
project-name/
├── README.md           # Project documentation
├── architecture/       # Architecture diagrams (where applicable)
├── src/               # Source code
├── data/              # Sample data files
└── config/            # Configuration files
```

## License

This repository is for educational and portfolio purposes.

## Contact

For questions or collaboration opportunities, please reach out through GitHub.
