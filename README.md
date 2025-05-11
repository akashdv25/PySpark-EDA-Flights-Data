# 🛫 Flight Data Analysis with PySpark

![](img/hero.jpg)

## 📋 Table of Contents
- [🎯 Project Overview](#-project-overview)
- [🌟 Key Features](#-key-features)
- [🔄 Data Processing Pipeline](#-data-processing-pipeline)
- [📊 Key Findings](#-key-findings)
- [💻 Technical Implementation](#-technical-implementation)
- [🚀 Performance Insights](#-performance-insights)
- [📁 Project Structure](#-project-structure)
- [🛠️ Setup and Installation](#️-setup-and-installation)
- [📝 Usage](#-usage)


## 🎯 Project Overview

This project implements a scalable data processing pipeline using PySpark to analyze flight delays across the United States. The analysis focuses on flight data from 2015, processing information about delays, cancellations, and airline performance.

![](img/data-pipeline.jpg)

### Objectives
- Build an efficient data processing pipeline using PySpark
- Analyze patterns in flight delays and cancellations
- Compare airline performance metrics
- Demonstrate best practices in big data processing
- Implement and compare different optimization techniques

## 🌟 Key Features

- Scalable data processing with PySpark
- Comprehensive flight delay analysis
- Airline performance comparison
- Airport and route analysis
- Optimized data processing techniques
- Integration with AWS S3

## 🔄 Data Processing Pipeline

### Data Cleaning Strategy
- Implemented selective null handling:
  - Dropped records with nulls in columns having <20% missing data
  - Removed columns with >20% missing data
  - Special handling for time-dependent data
- Time format standardization
- Data validation and quality checks

![](img/data-cleaning.jpg)

### Data Enrichment
- Implemented broadcast joins for efficient data merging
- Created derived metrics for delay analysis
- Standardized time formats across the dataset
- Added categorical classifications for delays

## 📊 Key Findings

### Delay Analysis
- 51.94% of flights experienced delays in the analyzed period
- Hawaiian Airlines showed the highest efficiency with minimal delays
- Negative delay values indicated early departures/arrivals

### Airport Statistics
- Hartsfield-Jackson Atlanta International Airport: Busiest airport
- Top cities by flight volume:
  1. Atlanta
  2. Chicago
  3. Dallas-Fort Worth

### Airline Performance
- Hawaiian Airlines: Best on-time performance
- Virgin America: Lowest flight volume
- Comprehensive delay categorization implemented

![](img/rank.jpg)

## 💻 Technical Implementation

### Data Processing Optimizations
1. Broadcast Joins
   - Implemented for efficient small-to-large table joins
   - Reduced data shuffling
   - Improved join performance

2. Caching Strategy
   - Strategic dataframe caching
   - Improved query performance
   - Efficient memory utilization

3. UDF vs Native Functions
   ```python
   # Performance Comparison
   Native Functions: 4.72 seconds
   UDF Implementation: 5.03 seconds
   ```

## 🚀 Performance Insights

### Native vs UDF Performance
- Native PySpark functions showed superior performance
- Minimal difference in small datasets
- Significant impact potential in large-scale implementations
- Recommendation: Prefer native functions for production


## 📁 Project Structure

### Source Code (`src/`)
The `src` directory contains the core functionality of the project:

1. **data_ingestion.py**
   - `get_spark_session()`: Configures and creates SparkSession with S3 access
   - `load_dataframes()`: Loads flight, airline, and airport data from S3

2. **data_cleaning.py**
   - `clean_nulls()`: Handles null values based on threshold strategy
   - `remove_duplicates()`: Eliminates duplicate records from DataFrames

3. **data_enrichment.py**
   - Functions for enhancing the dataset
   - Implements delay categorization

4. **upload_csv_to_s3.py**
   - Handles data export to S3
   - Manages parquet file conversion

5. **subset_flights_df.py**
   - Created manageable dataset subset from 6 million rows to 180000 rows
   - Implements data sampling logic

### Documentation (`docs/`)
The `docs` directory contains:

1. **documenting-code.txt**
   - Code documentation during the process of building the project
   - Documentation 

2. **assignment-requirements.txt**
   - Project requirements and specifications
   - Implementation guidelines

### Project Organization
```
PySpark-EDA-Flights-Data/
├── src/                  # Source code
│   ├── data_ingestion.py
│   ├── data_cleaning.py
│   ├── data_enrichment.py
│   ├── upload_csv_to_s3.py
│   └── subset_flights_df.py
├── docs/                 # Documentation
│   ├── documenting-code.txt
│   └── assignment-requirements.txt
├── notebook/            # Jupyter notebooks
│   └── flights_data_analysis.ipynb
└── README.md           # Project documentation
```

## 🛠️ Setup and Installation

### Prerequisites
```bash
- Python 3.x
- PySpark
- AWS CLI (for S3 integration)
```

### Installation Steps
1. Clone the repository
   ```bash
   git clone https://github.com/yourusername/flight-data-analysis.git
   cd flight-data-analysis
   ```

2. Set up virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

## 📝 Usage

1. Configure AWS credentials (if using S3)
   ```bash
   aws configure
   ```

2. Run the analysis notebook
   ```bash
   jupyter notebook notebook/flights_data_analysis.ipynb
   ```



---

![](img/thanks.jpg)

Built with ❤️ using PySpark
