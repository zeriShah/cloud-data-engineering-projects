# PakWheels Data Scraping Project

## Overview

This project demonstrates web scraping techniques to extract vehicle listing data from PakWheels, Pakistan's largest automotive marketplace. The scraped data is processed, cleaned, and visualized using Streamlit for interactive analysis.

## Project Objective

Build an automated web scraping solution that:
- Extracts vehicle listings from PakWheels website
- Cleans and structures the scraped data
- Provides interactive data visualization
- Analyzes pricing trends and market insights

## Architecture

![PakWheels Scraping Architecture](./pak%20wheels.png)

```
┌──────────────┐
│  PakWheels   │
│   Website    │
└──────┬───────┘
       │ Web Scraping
       ▼
┌──────────────┐
│ Data Extract │ (BeautifulSoup/Selenium)
└──────┬───────┘
       │ Clean & Transform
       ▼
┌──────────────┐
│  CSV Storage │
└──────┬───────┘
       │ Visualize
       ▼
┌──────────────┐
│  Streamlit   │
│  Dashboard   │
└──────────────┘
```

## Technology Stack

- **Python 3.x**: Core programming language
- **BeautifulSoup4**: HTML parsing and data extraction
- **Selenium**: Dynamic content scraping
- **Pandas**: Data manipulation and cleaning
- **Streamlit**: Interactive web dashboard
- **Matplotlib**: Data visualization

## Project Structure

```
PakWheelsDataScraping/
├── README.md
├── app.py                    # Streamlit dashboard
├── app.ipynb                 # Dashboard development notebook
├── dataExtraction.ipynb      # Web scraping implementation
└── pak_cars_clean.csv        # Cleaned output data
```

## Features

### 1. Web Scraping
- Automated data extraction from PakWheels listings
- Handles pagination and multiple pages
- Extracts key vehicle attributes

### 2. Data Cleaning
- Removes duplicates and invalid entries
- Standardizes data formats
- Handles missing values

### 3. Interactive Dashboard
- **Top 10 Car Brands**: Bar chart showing most listed brands
- **Average Price by Brand**: Price comparison across brands
- **Fuel Type Distribution**: Breakdown of fuel types
- **Transmission Analysis**: Manual vs Automatic distribution
- **Year vs Price Scatter Plot**: Price trends over vehicle years

## Installation & Setup

### Prerequisites
```bash
Python 3.7+
Chrome/Firefox browser (for Selenium)
```

### Install Dependencies
```bash
pip install beautifulsoup4 selenium pandas streamlit matplotlib requests lxml
```

### WebDriver Setup
For Selenium scraping:
```bash
# Download ChromeDriver matching your Chrome version
# Place in PATH or project directory
```

## Usage

### Running the Scraper

1. Open the scraping notebook:
```bash
jupyter notebook dataExtraction.ipynb
```

2. Execute cells to scrape data from PakWheels

### Launching the Dashboard

```bash
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Data Schema

### Scraped Data Fields
| Column | Type | Description |
|--------|------|-------------|
| Car Name | String | Full vehicle name |
| Brand | String | Manufacturer name |
| Price | Integer | Vehicle price in PKR |
| Year | Integer | Manufacturing year |
| Fuel | String | Fuel type (Petrol/Diesel/CNG/Hybrid) |
| Transmission | String | Manual or Automatic |
| Mileage | Integer | Kilometers driven |
| Location | String | City/region |

## Dashboard Features

### 1. Brand Analysis
- Identifies most popular car brands
- Shows market share distribution

### 2. Pricing Insights
- Average prices by brand
- Price trends over years
- Outlier detection

### 3. Market Trends
- Fuel type preferences
- Transmission type distribution
- Regional pricing variations

## Key Learnings

- Web scraping best practices
- Handling dynamic web content
- Data cleaning and preprocessing
- Building interactive dashboards
- Market data analysis techniques

## Ethical Considerations

- Respects robots.txt guidelines
- Implements rate limiting to avoid server overload
- Data used for educational purposes only
- No commercial use of scraped data

## Future Enhancements

- Add real-time scraping scheduler
- Implement price prediction model
- Add more visualization types
- Create email alerts for price drops
- Expand to other automotive websites

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Selenium WebDriver error | Ensure ChromeDriver version matches Chrome browser |
| Connection timeout | Check internet connection and website availability |
| Empty data extraction | Website structure may have changed, update selectors |
| Streamlit not loading | Verify all dependencies installed correctly |

## Notes

- Website structure may change over time, requiring selector updates
- Implement delays between requests to be respectful to the server
- Store data incrementally to avoid losing progress
- Consider using proxies for large-scale scraping

## Legal Disclaimer

This project is for educational purposes only. Always review and comply with website terms of service before scraping. PakWheels data is property of PakWheels.com.

## References

- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Selenium Documentation](https://selenium-python.readthedocs.io/)
- [Streamlit Documentation](https://docs.streamlit.io/)
