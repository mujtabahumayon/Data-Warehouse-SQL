# 🧱 Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This end-to-end solution showcases modern data engineering and analytics practices, from ingesting raw data to delivering actionable business insights through SQL-based reporting and dashboard-ready data models.

---

## 🏗️ Data Architecture: Medallion Design

This project follows the **Medallion Architecture** (Bronze → Silver → Gold) to structure the data pipeline:

<img width="920" alt="image" src="https://github.com/user-attachments/assets/a272f260-8b4b-43f2-b7f6-77d5376f7f7c" />

- **Bronze Layer**: Ingests raw data directly from source systems (CSV files) into a MySQL database.
- **Silver Layer**: Applies data cleansing, standardization, and transformation to prepare for analysis.
- **Gold Layer**: Hosts business-ready, star-schema modeled data optimized for reporting and BI tools.

---

## 📖 Project Overview

This project involves:

- 🧱 **Data Architecture**: Designing a scalable warehouse using Medallion principles.
- 🔄 **ETL Pipelines**: Automating data extraction, transformation, and loading.
- 🧮 **Dimensional Modeling**: Creating fact and dimension tables for analytical performance.
- 📊 **Analytics & Reporting**: Writing SQL queries to generate reports on customer behavior, sales trends, and product performance.

---

## 🛠️ Tools & Technologies

- **MySQL Workbench** – Database engine for building and testing the warehouse  
- **MySQL Workbench** – SQL development and database management  
- **CSV Files** – Raw ERP and CRM data sources  
- **DrawIO** – Diagrams for architecture, data flow, and data models  
- **GitHub** – Version control and collaboration platform  

---

## 🚀 Project Requirements

### 💾 Data Engineering (Warehouse)

**Objective**: Build a modern data warehouse to consolidate and model ERP/CRM sales data for analytics.

- Data Sources: Import CSV files from ERP and CRM systems.
- Cleansing: Resolve missing values, duplicates, and inconsistencies.
- Integration: Create a unified model for end-user analytical consumption.
- Documentation: Include a complete data dictionary, architecture, and transformation logic.

### 📊 Analytics & Reporting

**Objective**: Deliver insights through advanced SQL queries.

- Customer Segmentation & Behavior Analysis
- Product Performance Metrics
- Sales Trend Insights & Aggregations

---

## 📂 Repository Structure

```bash
data-warehouse-project/
├── datasets/                   # Raw ERP and CRM datasets
│   ├── CUST AZ12.csv
│   ├── LOC_A101.csv
│   ├── PX_CAT_G1V2.csv
│   ├── cust_info.csv
│   ├── prd_info.csv
│   ├── sales_details.csv
├── docs/                       # Project diagrams and metadata
│   ├── Data Flow Diagram.png
│   ├── Data Mart - Star Schema.png
│   ├── Data Warehouse.png
│   ├── Integration Model - How Tables are Related.png
│   ├── data_models.drawio
├── scripts/                    # SQL scripts for each data layer
│   ├── bronze/
│   |  ├── ddl_bronze_layer.sql
│   ├── silver/
|   |  ├── DDL_silver_layer.sql
|   |  ├── silver_layer_loadingdata.sql
│   ├── gold/
|   |  ├── ddl_gold.sql
|   ├── Full_DataWarehouse_datapipeline.sql
|   ├── init_database.sqp
├── tests/                      # Data quality checks and validation queries
|   ├── silver_layer_quality_checls.sql
├── README.md                   # Project overview and instructions
├── LICENSE                     # Open-source license
```


