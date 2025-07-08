/*
=============================================================
Create Database and Schemas (MySQL Workbench)
=============================================================
Script Purpose:
    This script creates a new database schema called 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three more databases/schemas:
    'bronze', 'silver', and 'gold'. 
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Creating Bronze, Silver & Gold Layer Tables (As MySQL Workbench does not support nested Databases)

-- Bronze Layer --

-- Drop table if it exists
DROP TABLE IF EXISTS bronze_crm_prd_info;
-- Create Table
CREATE TABLE bronze_CRM_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

-- Drop table if it exists
DROP TABLE IF EXISTS bronze_CRM_prd_info;
-- Create Table
CREATE TABLE bronze_CRM_prd_info(
prd_id INT,
prd_key	VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

-- Drop table if it exists
DROP TABLE IF EXISTS bronze_CRM_sales_details;
-- Create Table
CREATE TABLE bronze_CRM_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

-- Drop table if it exists
DROP TABLE IF EXISTS bronze_ERP_CUST_AZ12;
-- Create Table
CREATE TABLE bronze_ERP_CUST_AZ12(
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50)
);

-- Drop table if it exists
DROP TABLE IF EXISTS bronze_ERP_LOC_A101;
-- Create Table
CREATE TABLE bronze_ERP_LOC_A101(
CID VARCHAR(50),
CNTRY VARCHAR(50)
);

-- Drop table if it exists
DROP TABLE IF EXISTS bronze_ERP_PX_CAT_G1V2;
-- Create Table
CREATE TABLE bronze_ERP_PX_CAT_G1V2(
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50)
);



-- Inserting/Loading Data
SHOW VARIABLES LIKE 'secure_file_priv';
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SHOW VARIABLES LIKE 'local_infile';

-- CRM: cust_info
TRUNCATE TABLE bronze_CRM_cust_info;
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_CRM_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_CRM_cust_info;
SELECT COUNT(*) FROM bronze_CRM_cust_info;

-- CRM: prd_info
TRUNCATE TABLE bronze_CRM_prd_info;
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze_CRM_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_CRM_prd_info;
SELECT COUNT(*) FROM bronze_CRM_prd_info;

-- CRM: sales_details
TRUNCATE TABLE bronze_CRM_sales_details;
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_CRM_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_CRM_sales_details;
SELECT COUNT(*) FROM bronze_CRM_sales_details;

-- ERP: CUST_AZ12
TRUNCATE TABLE bronze_ERP_CUST_AZ12;
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_ERP_CUST_AZ12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ERP_CUST_AZ12;
SELECT COUNT(*) FROM bronze_ERP_CUST_AZ12;

-- ERP: LOC_A101
TRUNCATE TABLE bronze_ERP_CUST_AZ12;
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze_ERP_LOC_A101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ERP_LOC_A101;
SELECT COUNT(*) FROM bronze_ERP_LOC_A101;

-- ERP: PX_CAT_G1V2
TRUNCATE TABLE bronze_ERP_PX_CAT_G1V2;
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_ERP_PX_CAT_G1V2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ERP_PX_CAT_G1V2;
SELECT COUNT(*) FROM bronze_ERP_PX_CAT_G1V2;

