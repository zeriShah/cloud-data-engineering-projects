# End-to-End Data Analytics Project (Python + SQL)

## Overview

This project demonstrates a complete data analytics workflow combining Python for data extraction and transformation with SQL for advanced analytics. The pipeline downloads retail orders data from Kaggle, performs data cleaning and feature engineering using pandas, loads it into SQL Server, and executes complex analytical queries.

## Project Objective

Build an end-to-end analytics solution that:
- Automates data extraction from Kaggle API
- Performs data cleaning and transformation in Python
- Loads data into SQL Server database
- Executes advanced SQL analytics queries
- Generates business insights from retail data

## Architecture

```
┌──────────────┐
│  Kaggle API  │
└──────┬───────┘
       │ Extract
       ▼
┌──────────────┐
│    Pandas    │ (Clean & Transform)
│  - Normalize │
│  - Calculate │
│  - Validate  │
└──────┬───────┘
       │ Load
       ▼
┌──────────────┐
│  SQL Server  │
└──────┬───────┘
       │ Analyze
       ▼
┌──────────────┐
│   Business   │
│   Insights   │
└──────────────┘
```

## Technology Stack

- **Python 3.x**: Data processing and automation
- **Pandas**: Data manipulation and transformation
- **Kaggle API**: Dataset download
- **SQL Server**: Relational database
- **pyodbc**: Database connectivity
- **SQLAlchemy**: ORM and database engine
- **Jupyter Notebook**: Interactive development

## Project Structure

```
EndtoEndDataAnalyticsProject(Python+SQL)/
├── README.md
├── data.ipynb                # Python ETL pipeline
├── SQLQuery1.sql             # SQL analytics queries
├── end to end analytical.png # Architecture diagram
└── data/
    └── orders.csv            # Downloaded dataset
```

## Features

### 1. Data Extraction
- Automated download from Kaggle using API
- Dataset: Retail Orders (9,994 records)
- Handles authentication and file extraction

### 2. Data Transformation
- Column name standardization (lowercase, underscore separation)
- Missing value handling
- Date parsing and conversion
- Calculated fields:
  - **Discount**: `list_price × discount_percent × 0.01`
  - **Sale Price**: `list_price - discount`
  - **Profit**: `sale_price - cost_price`

### 3. Data Loading
- Creates SQL Server database programmatically
- Bulk insert using SQLAlchemy
- Supports both replace and append modes

### 4. SQL Analytics
- Regional sales analysis
- Product performance metrics
- Time-series analysis
- Top N queries with window functions
- Year-over-year growth analysis
- Pivot table transformations

## Installation & Setup

### Prerequisites

```bash
Python 3.7+
SQL Server (Express or higher)
Kaggle account
```

### Install Dependencies

```bash
pip install pandas kaggle pyodbc sqlalchemy
```

### Kaggle API Setup

1. Create Kaggle account and generate API token
2. Download `kaggle.json` from Kaggle account settings
3. Place in `~/.kaggle/` directory (Linux/Mac) or `C:\Users\<username>\.kaggle\` (Windows)

```bash
# Linux/Mac
mkdir -p ~/.kaggle
mv kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json

# Windows
mkdir %USERPROFILE%\.kaggle
move kaggle.json %USERPROFILE%\.kaggle\
```

### SQL Server Setup

1. Install SQL Server and SQL Server Management Studio (SSMS)
2. Enable SQL Server authentication
3. Create login credentials
4. Install ODBC Driver 17 for SQL Server

```bash
# Windows - Download from Microsoft
# Linux
sudo apt-get install unixodbc-dev
```

## Usage

### Running the Pipeline

1. Open Jupyter Notebook:
```bash
jupyter notebook data.ipynb
```

2. Execute cells sequentially:
   - Download dataset from Kaggle
   - Load and explore data
   - Clean and transform data
   - Connect to SQL Server
   - Create database
   - Load data into SQL Server

3. Run SQL queries in SSMS or notebook:
```bash
# Connect to SQL Server
# Execute queries from SQLQuery1.sql
```

## Data Schema

### Orders Table (df_orders)

| Column | Type | Description |
|--------|------|-------------|
| order_id | INT | Unique order identifier |
| order_date | DATE | Order placement date |
| ship_mode | VARCHAR | Shipping method |
| segment | VARCHAR | Customer segment |
| country | VARCHAR | Country |
| city | VARCHAR | City |
| state | VARCHAR | State |
| postal_code | INT | ZIP/Postal code |
| region | VARCHAR | Geographic region |
| category | VARCHAR | Product category |
| sub_category | VARCHAR | Product sub-category |
| product_id | VARCHAR | Unique product identifier |
| cost_price | INT | Product cost |
| list_price | INT | Listed price |
| quantity | INT | Order quantity |
| discount_percent | INT | Discount percentage |
| discount | FLOAT | Calculated discount amount |
| sale_price | FLOAT | Final sale price |
| profit | FLOAT | Profit per order |

## SQL Analytics Queries

### 1. Regional Sales Analysis
```sql
SELECT state, category, SUM(cost_price) AS Total_cost_per_state 
FROM df_orders
GROUP BY state, category 
ORDER BY state;
```

### 2. Top 10 Products by Region
```sql
WITH cte AS (
    SELECT region, product_id, SUM(cost_price) AS cost
    FROM df_orders
    GROUP BY region, product_id
)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY region ORDER BY cost DESC) AS rn
    FROM cte
) A
WHERE rn <= 10;
```

### 3. Month-over-Month Sales Comparison
```sql
WITH cte AS (
    SELECT 
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month,
        SUM(sale_price) AS sales
    FROM df_orders
    GROUP BY YEAR(order_date), MONTH(order_date)
)
SELECT 
    order_year,
    SUM(CASE WHEN order_month = 2 THEN sales ELSE 0 END) AS feb_sales,
    SUM(CASE WHEN order_month = 3 THEN sales ELSE 0 END) AS march_sales
FROM cte
GROUP BY order_year;
```

### 4. Top 5 Products by Region
```sql
SELECT t.region, t.product_id, t.total_sales
FROM (
    SELECT region, product_id, SUM(sale_price) AS total_sales,
           ROW_NUMBER() OVER(PARTITION BY region ORDER BY SUM(sale_price) DESC) AS rn
    FROM df_orders
    GROUP BY region, product_id
) t
WHERE t.rn <= 5
ORDER BY t.region, t.total_sales DESC;
```

### 5. Highest Growth Sub-Category (YoY)
```sql
SELECT TOP 1 sub_category,
       SUM(CASE WHEN YEAR(order_date) = 2022 THEN sale_price ELSE 0 END) AS sales_2022,
       SUM(CASE WHEN YEAR(order_date) = 2023 THEN sale_price ELSE 0 END) AS sales_2023,
       SUM(CASE WHEN YEAR(order_date) = 2023 THEN sale_price ELSE 0 END) -
       SUM(CASE WHEN YEAR(order_date) = 2022 THEN sale_price ELSE 0 END) AS growth
FROM df_orders
GROUP BY sub_category
ORDER BY growth DESC;
```

## Key Insights

The analytics queries reveal:
- Regional sales performance and trends
- Top-performing products by geography
- Seasonal sales patterns
- Year-over-year growth metrics
- Category and sub-category profitability

## Key Learnings

- End-to-end data pipeline development
- Kaggle API integration
- Data cleaning and feature engineering
- SQL Server database management
- Advanced SQL analytics (window functions, CTEs, pivots)
- Python-SQL integration patterns

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Kaggle API authentication error | Verify kaggle.json is in correct location with proper permissions |
| SQL Server connection failed | Check server name, credentials, and SQL Server service status |
| ODBC Driver not found | Install ODBC Driver 17 for SQL Server |
| Database already exists error | Use `if_exists='append'` or drop existing database |
| Date parsing error | Verify date format matches `YYYY-MM-DD` |

## Future Enhancements

- Add data visualization with Matplotlib/Seaborn
- Implement Power BI dashboard
- Add automated reporting
- Create stored procedures for common queries
- Implement incremental data loading
- Add data quality checks and validation

## Dataset Information

- **Source**: Kaggle - Retail Orders Dataset
- **Records**: 9,994 orders
- **Time Period**: 2022-2023
- **Geography**: United States
- **Categories**: Furniture, Office Supplies, Technology

## References

- [Kaggle API Documentation](https://github.com/Kaggle/kaggle-api)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [SQL Server Documentation](https://docs.microsoft.com/en-us/sql/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
