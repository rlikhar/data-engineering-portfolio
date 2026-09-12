# Azure Retail Data Engineering Pipeline

An end-to-end data engineering solution built on Azure, demonstrating modern cloud data pipeline architecture for retail analytics. This project showcases the integration of multiple Azure services to create a scalable, production-ready data platform.

![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 📋 Overview

This project implements a complete retail data analytics pipeline using Azure cloud services, processing data from multiple sources and delivering business insights through interactive dashboards. The solution follows industry best practices including the Medallion Architecture for data refinement and Delta Lake for reliable data storage.

## 🏗️ Architecture
<img width="1200" height="364" alt="image" src="https://github.com/user-attachments/assets/6febec92-b37d-4c45-908e-9d628ef5189c" />

### Medallion Architecture Layers

- **🥉 Bronze Layer**: Raw data ingestion from source systems (SQL DB, API)
- **🥈 Silver Layer**: Cleaned, validated, and deduplicated data with proper data types
- **🥇 Gold Layer**: Business-level aggregations and metrics optimized for analytics

## 🛠️ Technologies Used

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | Azure SQL Database | Transactional data storage (transactions, products, stores) |
| **Data Lake** | Azure Data Lake Storage Gen2 | Centralized data storage with hierarchical namespace |
| **Orchestration** | Azure Data Factory | ETL pipeline orchestration and data movement |
| **Processing** | Azure Databricks | Distributed data processing using PySpark |
| **Storage Format** | Delta Lake | ACID-compliant data storage with versioning |
| **Visualization** | Power BI | Business intelligence and interactive dashboards |
| **Language** | Python/PySpark | Data transformation and processing logic |

## 📊 Data Sources

### 1. Azure SQL Database
- **Transaction Data**: Customer purchases, timestamps, quantities
- **Product Data**: Product details, categories, pricing
- **Store Data**: Store locations and information

### 2. REST API
- **Customer Data**: Customer information in JSON format

## ✨ Features

- ✅ **Multi-source Data Integration**: Seamlessly combines SQL and API data sources
- ✅ **Medallion Architecture**: Industry-standard data refinement approach
- ✅ **Data Quality Management**: Automated cleaning, deduplication, and validation
- ✅ **Scalable Processing**: Leverages Databricks clusters for distributed computing
- ✅ **Delta Lake Format**: ACID transactions and time travel capabilities
- ✅ **Interactive Dashboards**: Rich visualizations for business insights
- ✅ **Automated ETL**: Scheduled pipelines for continuous data refresh

## 📈 Key Metrics & Analytics

The solution provides insights into:

- 💰 **Total Sales Revenue**: Aggregated sales across all stores and products
- 📦 **Product Performance**: Best-selling products and categories
- 🏪 **Store Analytics**: Sales distribution and performance by location
- 📊 **Transaction Metrics**: Number of transactions and average transaction value
- 📅 **Temporal Trends**: Sales patterns over time
- 🎯 **Category Analysis**: Sales breakdown by product categories

## 🚀 Getting Started

### Prerequisites

- Azure Subscription
- Azure SQL Database instance
- Azure Data Lake Storage Gen2 account
- Azure Data Factory workspace
- Azure Databricks workspace
- Power BI Desktop (for local development)
- Basic knowledge of Python/PySpark, SQL, and Azure services

### Setup Instructions

#### 1. Azure SQL Database Setup

```sql
-- Create database and tables
CREATE DATABASE RetailDB;

-- Transaction Table
CREATE TABLE Transactions (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    StoreID INT,
    Quantity INT,
    TransactionDate DATETIME,
    Price DECIMAL(10,2)
);

-- Product Table
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10,2)
);

-- Store Table
CREATE TABLE Stores (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(100),
    Location VARCHAR(100)
);
```

#### 2. Azure Data Lake Storage Configuration

Create the following directory structure:
```
adls-container/
├── bronze/
│   ├── transactions/
│   ├── products/
│   ├── stores/
│   └── customers/
├── silver/
│   └── unified_data/
└── gold/
    └── aggregated_metrics/
```

#### 3. Azure Data Factory Pipelines

Create pipelines for:
- SQL to ADLS data copy (transactions, products, stores)
- API to ADLS data ingestion (customers)
- Schedule triggers for automated refresh

#### 4. Databricks Configuration and Data Loading in various Layers

- Added python notebook for reference (based on Unified Catalogue)
  
#### 7. Power BI Dashboard

Connect Power BI to the gold layer Delta tables and create:
- KPI cards (total sales, transactions, avg. transaction)
- Line charts (sales trends over time)
- Bar charts (store sales)
- Pie/Donut charts (category distribution)
- Map chart (sales by location)

## 🔄 Data Flow

1. **Ingestion**: ADF extracts data from SQL DB and API
2. **Landing**: Raw data stored in Bronze layer (ADLS)
3. **Transformation**: Databricks processes and cleans data → Silver layer
4. **Aggregation**: Business metrics calculated → Gold layer
5. **Visualization**: Power BI connects to Gold layer for reporting

## 📊 Sample Visualizations (PowerBi Dashboard)

<img width="1307" height="731" alt="image" src="https://github.com/user-attachments/assets/fc4e955c-21c2-4bc7-866a-4e100d18cd2a" />


## 🚧 Future Enhancements

- [ ] Implement real-time streaming with Azure Event Hubs
- [ ] Add machine learning models for sales forecasting
- [ ] Implement data quality monitoring and alerts
- [ ] Create automated testing framework
- [ ] Add CI/CD pipeline for deployment automation
- [ ] Implement slowly changing dimensions (SCD) handling
- [ ] Add customer segmentation analytics
- [ ] Integrate Azure Synapse Analytics for advanced analytics
