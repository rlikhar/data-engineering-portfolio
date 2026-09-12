# Azure Data Engineering Projects

A central repository showcasing end-to-end data engineering solutions and production-ready cloud architectures built on Microsoft Azure. Each folder in this repository represents a standalone, fully implemented data pipeline project targeting real-world domain analytics.

---

## 📁 Projects Overview

| Project | Description | Tech Stack | Status |
| --- | --- | --- | --- |
| **[Retail_Data]** | Azure Retail Data Engineering Pipeline — Batch and near-real-time ingestion, transformation, and analytical reporting for retail transactional data. | Azure Data Factory, ADLS Gen2, Databricks, Synapse Analytics, Power BI | Completed |
| *Future Projects* | Upcoming data pipeline implementations focusing on streaming, real-time analytics, and machine learning integration. | TBD | Planned |

---

## 🛒 Featured Project: Retail Data Engineering Pipeline

The **`Retail_Data`** directory contains an end-to-end data engineering solution built on Azure, demonstrating modern cloud data pipeline architecture for retail analytics. This project showcases the integration of multiple Azure services to create a scalable, production-ready data platform.

### Key Highlights

* **Medallion Architecture:** Standardized data layer processing across Bronze (Raw), Silver (Cleansed/Transformed), and Gold (Business Aggregated) zones in Azure Data Lake Storage Gen2.
* **Automated Ingestion & Orchestration:** Built event-driven and scheduled ETL/ELT data pipelines using Azure Data Factory.
* **Scalable Data Transformations:** Implemented PySpark and Delta Lake pipelines on Azure Databricks for scalable cleaning, enrichment, and schema enforcement.
* **Analytical Serving Layer:** Modeled curated gold data into star schemas within Azure Synapse Analytics for enterprise reporting and Power BI dashboards.

---

## 🛠️ Tech Stack & Services

* **Cloud Platform:** Microsoft Azure
* **Orchestration:** Azure Data Factory (ADF)
* **Data Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake
* **Compute & Transformation:** Azure Databricks (PySpark, Spark SQL)
* **Data Warehousing:** Azure Synapse Analytics
* **Security & Governance:** Azure Key Vault, Managed Identities, Azure Active Directory (Microsoft Entra ID)

---

## 📂 Repository Structure

```text
.
├── README.md                              # Main repository documentation
└── Retail_Data/                           # Azure Retail Data Engineering Pipeline
    ├── Retail Sales Dashboard.pbix        # Architecture diagrams and specifications
    ├── databrick_notebook.py/             # PySpark / Delta Lake transformation notebooks
    ├── SQL_Table.txt/                     # Synapse DDL scripts and analytical queries
    └── README.md                          # Project-specific setup and deployment guide

```

---

## 🚀 Getting Started

1. **Clone the Repository:**
```bash
git clone https://github.com/rlikhar/data-engineering-portfolio/edit/main/azure-project.git
cd azure-project

```


2. **Explore Individual Projects:**
```bash
cd Retail_Data

```


Refer to the `README.md` inside each project subfolder for detailed configuration, prerequisites, and execution steps.
