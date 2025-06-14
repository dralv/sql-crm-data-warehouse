# SQL CRM Data Warehouse

A multi-layered data warehouse solution for CRM and ERP data, designed for SQL Server. This project implements a robust ETL pipeline with Bronze, Silver, and Gold layers, supporting data cleansing, transformation, and analytics-ready modeling.

![Data Flow](image-1.png)

---

## Project Structure

```
datasets/
  source_crm/
    cust_info.csv
    prd_info.csv
    sales_details.csv
  source_erp/
    CUST_AZ12.csv
    LOC_A101.csv
    PX_CAT_G1V2.csv
docs/
  data_catalog.md
  dataflow.png
scripts/
  init_database.sql
  bronze/
    ddl_bronze.sql
    proc_load_bronze.sql
  silver/
    ddl_silver.sql
    proc_load_silver.sql
    workbench.sql
  gold/
    ddd_gold.sql
tests/
  test_silver_quality_check.sql
```

---

## Layers Overview

- **Bronze:** Raw data ingestion from CSV files.
- **Silver:** Cleansed, deduplicated, and standardized data.
- **Gold:** Business-level, analytics-ready views (dimensions and facts).

---

## Setup & Usage

### 1. Initialize Database & Schemas

Run the following script to create the database and schemas:

```sql
USE master;
GO
CREATE DATABASE DataWareHouseCrm;
GO
USE DataWareHouseCrm;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
```

### 2. Create Bronze & Silver Tables

```sql
-- ddl_bronze.sql
-- ddl_silver.sql
```

### 3. Load Data

- **Bronze Layer:** Loads raw CSVs into staging tables.
- **Silver Layer:** Cleans and transforms data from Bronze.

### 4. Create Gold Views

```sql
-- ddd_gold.sql
```

---

## Data Model (Gold Layer)

### gold.dim_customers

| Column Name     | Data Type    | Description                                      |
|-----------------|-------------|--------------------------------------------------|
| customer_key    | INT         | Surrogate key                                    |
| customer_id     | INT         | Unique customer ID                               |
| customer_number | NVARCHAR(50)| Alphanumeric customer identifier                 |
| first_name      | NVARCHAR(50)| Customer's first name                            |
| last_name       | NVARCHAR(50)| Customer's last name                             |
| country         | NVARCHAR(50)| Country of residence                             |
| marital_status  | NVARCHAR(50)| Marital status                                   |
| gender          | NVARCHAR(50)| Gender                                           |
| birthdate       | DATE        | Date of birth                                    |
| create_date     | DATE        | Record creation date                             |

### gold.dim_products

| Column Name     | Data Type    | Description                                      |
|-----------------|-------------|--------------------------------------------------|
| product_key     | INT         | Surrogate key                                    |
| product_id      | INT         | Unique product ID                                |
| product_number  | NVARCHAR(50)| Alphanumeric product identifier                  |
| product_name    | NVARCHAR(50)| Product name                                     |
| category_id     | NVARCHAR(50)| Category ID                                      |
| category        | NVARCHAR(50)| Category name                                    |
| subcategory     | NVARCHAR(50)| Subcategory name                                 |
| maintenance     | NVARCHAR(50)| Maintenance type                                 |
| cost            | INT         | Product cost                                     |
| product_line    | NVARCHAR(50)| Product line                                     |
| start_date      | DATE        | Product start date                               |

### gold.fact_sales

| Column Name     | Data Type    | Description                                      |
|-----------------|-------------|--------------------------------------------------|
| order_number    | NVARCHAR(50)| Sales order number                               |
| product_key     | INT         | Foreign key to dim_products                      |
| customer_key    | INT         | Foreign key to dim_customers                     |
| order_date      | DATE        | Order date                                       |
| shipping_date   | DATE        | Shipping date                                    |
| due_date        | DATE        | Due date                                         |
| sales_amount    | INT         | Sales amount                                     |
| quantity        | INT         | Quantity sold                                    |
| price           | INT         | Price per unit                                   |

---

## Data Quality

Validation scripts are provided in `tests/test_silver_quality_check.sql` to ensure:

- No duplicate or null primary keys
- Standardized categorical values
- Valid and consistent dates
- Business rule enforcement (e.g., sales = quantity × price)

---

## ETL Flow

1. **Ingest:** Load CSVs into Bronze tables.
2. **Cleanse & Transform:** Move and standardize data into Silver tables.
3. **Model:** Build Gold views for analytics and reporting.

---

## License

See [LICENSE](LICENSE).
