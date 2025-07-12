DROP DATABASE DataWarehouse_TEST;
CREATE DATABASE DataWarehouse_TEST;

-- STEP 0: Create logging table
CREATE TABLE IF NOT EXISTS etl_process_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    step_name VARCHAR(100),
    start_time DATETIME,
    end_time DATETIME,
    duration_seconds INT,
    status VARCHAR(20),
    message TEXT
);

-- ===================== BRONZE LAYER ===================== --
-- Bronze - CRM Customer Info (TEST only)
DROP TABLE IF EXISTS bronze_CRM_cust_info_TEST;
CREATE TABLE bronze_CRM_cust_info_TEST (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date VARCHAR(20)
);
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_CRM_cust_info_TEST
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, @cst_create_date)
SET cst_create_date = CASE WHEN @cst_create_date IN ('0000-00-00', '', 'NULL') THEN NULL ELSE @cst_create_date END;

-- Bronze - CRM Product Info (TEST only)
DROP TABLE IF EXISTS bronze_CRM_prd_info_TEST;
CREATE TABLE bronze_CRM_prd_info_TEST (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt VARCHAR(20),
    prd_end_dt VARCHAR(20)
);
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze_CRM_prd_info_TEST
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(prd_id, prd_key, prd_nm, prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
SET 
    prd_start_dt = CASE WHEN @prd_start_dt IN ('0000-00-00', '', 'NULL') THEN NULL ELSE @prd_start_dt END,
    prd_end_dt = CASE WHEN @prd_end_dt IN ('0000-00-00', '', 'NULL') THEN NULL ELSE @prd_end_dt END;

-- Bronze - CRM Sales Details (TEST only)
DROP TABLE IF EXISTS bronze_CRM_sales_details_TEST;
CREATE TABLE bronze_CRM_sales_details_TEST (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt VARCHAR(20),
    sls_ship_dt VARCHAR(20),
    sls_due_dt VARCHAR(20),
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_CRM_sales_details_TEST
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(sls_ord_num, sls_prd_key, sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, sls_sales, sls_quantity, sls_price)
SET 
    sls_order_dt = CASE WHEN @sls_order_dt IN ('00000000', '', 'NULL') THEN NULL ELSE @sls_order_dt END,
    sls_ship_dt = CASE WHEN @sls_ship_dt IN ('00000000', '', 'NULL') THEN NULL ELSE @sls_ship_dt END,
    sls_due_dt = CASE WHEN @sls_due_dt IN ('00000000', '', 'NULL') THEN NULL ELSE @sls_due_dt END;

-- Bronze - ERP CUST_AZ12 (TEST only)
DROP TABLE IF EXISTS bronze_ERP_CUST_AZ12_TEST;
CREATE TABLE bronze_ERP_CUST_AZ12_TEST (
    CID VARCHAR(50),
    BDATE VARCHAR(20),
    GEN VARCHAR(50)
);
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_ERP_CUST_AZ12_TEST
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(CID, @BDATE, GEN)
SET BDATE = CASE WHEN @BDATE IN ('0000-00-00', '', 'NULL') THEN NULL ELSE @BDATE END;

-- Bronze - ERP LOC_A101 (TEST only)
DROP TABLE IF EXISTS bronze_ERP_LOC_A101_TEST;
CREATE TABLE bronze_ERP_LOC_A101_TEST (
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze_ERP_LOC_A101_TEST
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Bronze - ERP PX_CAT_G1V2 (TEST only)
DROP TABLE IF EXISTS bronze_ERP_PX_CAT_G1V2_TEST;
CREATE TABLE bronze_ERP_PX_CAT_G1V2_TEST (
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);
LOAD DATA LOCAL INFILE '/Users/mujtabahumayon/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_ERP_PX_CAT_G1V2_TEST
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- ===================== SILVER LAYER ===================== --

-- Drop and recreate silver_CRM_cust_info_TEST with proper DATE column
DROP TABLE IF EXISTS silver_CRM_cust_info_TEST;
CREATE TABLE silver_CRM_cust_info_TEST (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO etl_process_log (step_name, start_time, status)
VALUES ('Silver - CRM Cust Info', NOW(), 'Running');
SET @log_id = LAST_INSERT_ID();

TRUNCATE TABLE silver_CRM_cust_info_TEST;
INSERT INTO silver_CRM_cust_info_TEST (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
)
SELECT
    cst_id, cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'F' THEN 'Female'
        WHEN 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    CASE 
        WHEN cst_create_date IS NULL OR cst_create_date = '' OR cst_create_date = '0000-00-00' THEN NULL
        WHEN cst_create_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(cst_create_date, '%Y-%m-%d')
        ELSE NULL
    END
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM bronze_CRM_cust_info_TEST
    WHERE cst_id IS NOT NULL
) sub
WHERE rn = 1;

UPDATE etl_process_log 
SET end_time = NOW(), 
    duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), 
    status = 'Success', 
    message = 'Completed Silver - CRM Cust Info'
WHERE id = @log_id;

-- Drop and recreate silver_CRM_prd_info_TEST with proper DATE columns
DROP TABLE IF EXISTS silver_CRM_prd_info_TEST;
CREATE TABLE silver_CRM_prd_info_TEST (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO etl_process_log (step_name, start_time, status)
VALUES ('Silver - CRM Product Info', NOW(), 'Running');
SET @log_id = LAST_INSERT_ID();

TRUNCATE TABLE silver_CRM_prd_info_TEST;
INSERT INTO silver_CRM_prd_info_TEST (
    prd_id, prd_key, cat_id, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT
    prd_id,
    SUBSTRING(prd_key, 7),
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
    prd_nm,
    IFNULL(prd_cost, 0),
    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        WHEN 'S' THEN 'Other Sales'
        ELSE 'n/a'
    END,
    CASE 
        WHEN prd_start_dt IS NULL OR prd_start_dt = '' OR prd_start_dt = '0000-00-00' THEN NULL
        WHEN prd_start_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(prd_start_dt, '%Y-%m-%d')
        ELSE NULL
    END,
    CASE 
        WHEN prd_end_dt IS NULL OR prd_end_dt = '' OR prd_end_dt = '0000-00-00' THEN NULL
        WHEN prd_end_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(prd_end_dt, '%Y-%m-%d')
        ELSE NULL
    END
FROM bronze_CRM_prd_info_TEST;

UPDATE etl_process_log 
SET end_time = NOW(), 
    duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), 
    status = 'Success', 
    message = 'Completed Silver - CRM Product Info'
WHERE id = @log_id;

-- Drop and recreate silver_CRM_sales_details_TEST with proper DATE columns
DROP TABLE IF EXISTS silver_CRM_sales_details_TEST;
CREATE TABLE silver_CRM_sales_details_TEST (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO etl_process_log (step_name, start_time, status)
VALUES ('Silver - CRM Sales Details', NOW(), 'Running');
SET @log_id = LAST_INSERT_ID();

TRUNCATE TABLE silver_CRM_sales_details_TEST;
INSERT INTO silver_CRM_sales_details_TEST (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt IS NULL OR sls_order_dt = '' OR sls_order_dt = '00000000' OR LENGTH(sls_order_dt) != 8 THEN NULL
        WHEN sls_order_dt REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(sls_order_dt, '%Y%m%d')
        ELSE NULL
    END,
    CASE 
        WHEN sls_ship_dt IS NULL OR sls_ship_dt = '' OR sls_ship_dt = '00000000' OR LENGTH(sls_ship_dt) != 8 THEN NULL
        WHEN sls_ship_dt REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(sls_ship_dt, '%Y%m%d')
        ELSE NULL
    END,
    CASE 
        WHEN sls_due_dt IS NULL OR sls_due_dt = '' OR sls_due_dt = '00000000' OR LENGTH(sls_due_dt) != 8 THEN NULL
        WHEN sls_due_dt REGEXP '^[0-9]{8}$' THEN STR_TO_DATE(sls_due_dt, '%Y%m%d')
        ELSE NULL
    END,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR
             (sls_price > 0 AND sls_sales != sls_quantity * ABS(sls_price))
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0 THEN
            CASE
                WHEN sls_quantity = 0 THEN NULL
                WHEN sls_sales <= 0 THEN NULL
                ELSE sls_sales / NULLIF(sls_quantity, 0)
            END
        ELSE sls_price
    END
FROM bronze_CRM_sales_details_TEST;

UPDATE etl_process_log 
SET end_time = NOW(), 
    duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), 
    status = 'Success', 
    message = 'Completed Silver - CRM Sales Details'
WHERE id = @log_id;

-- Drop and recreate silver_ERP_CUST_AZ12_TEST with proper DATE column
DROP TABLE IF EXISTS silver_ERP_CUST_AZ12_TEST;
CREATE TABLE silver_ERP_CUST_AZ12_TEST (
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO etl_process_log (step_name, start_time, status)
VALUES ('Silver - ERP CUST_AZ12', NOW(), 'Running');
SET @log_id = LAST_INSERT_ID();

TRUNCATE TABLE silver_ERP_CUST_AZ12_TEST;
INSERT INTO silver_ERP_CUST_AZ12_TEST (
    CID, BDATE, GEN
)
SELECT
    CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4) ELSE CID END,
    CASE 
        WHEN BDATE IS NULL OR BDATE = '' OR BDATE = '0000-00-00' THEN NULL
        WHEN BDATE REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(BDATE, '%Y-%m-%d')
        ELSE NULL
    END,
    CASE
        WHEN UPPER(REPLACE(GEN, ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(REPLACE(GEN, ' ', '')) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END
FROM bronze_ERP_CUST_AZ12_TEST;

UPDATE etl_process_log 
SET end_time = NOW(), 
    duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), 
    status = 'Success', 
    message = 'Completed Silver - ERP CUST_AZ12'
WHERE id = @log_id;

-- Drop and recreate silver_ERP_LOC_A101_TEST
DROP TABLE IF EXISTS silver_ERP_LOC_A101_TEST;
CREATE TABLE silver_ERP_LOC_A101_TEST (
    CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO etl_process_log (step_name, start_time, status)
VALUES ('Silver - ERP LOC_A101', NOW(), 'Running');
SET @log_id = LAST_INSERT_ID();

TRUNCATE TABLE silver_ERP_LOC_A101_TEST;
INSERT INTO silver_ERP_LOC_A101_TEST (
    CID, CNTRY
)
SELECT
    REPLACE(CID, '-', ''),
    CASE
        WHEN UPPER(REPLACE(CNTRY, ' ', '')) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(REPLACE(CNTRY, ' ', '')) = 'DE' THEN 'Germany'
        WHEN CNTRY IS NULL OR CNTRY = '' THEN 'n/a'
        ELSE TRIM(CNTRY)
    END
FROM bronze_ERP_LOC_A101_TEST;

UPDATE etl_process_log 
SET end_time = NOW(), 
    duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), 
    status = 'Success', 
    message = 'Completed Silver - ERP LOC_A101'
WHERE id = @log_id;

-- Drop and recreate silver_ERP_PX_CAT_G1V2_TEST
DROP TABLE IF EXISTS silver_ERP_PX_CAT_G1V2_TEST;
CREATE TABLE silver_ERP_PX_CAT_G1V2_TEST (
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_create_date DATETIME DEFAULT NOW()
);

INSERT INTO etl_process_log (step_name, start_time, status)
VALUES ('Silver - ERP PX_CAT_G1V2', NOW(), 'Running');
SET @log_id = LAST_INSERT_ID();

TRUNCATE TABLE silver_ERP_PX_CAT_G1V2_TEST;
INSERT INTO silver_ERP_PX_CAT_G1V2_TEST (
    ID, CAT, SUBCAT, MAINTENANCE
)
SELECT
    ID, CAT, SUBCAT,
    REPLACE(REPLACE(TRIM(MAINTENANCE), '\r', ''), '\n', '')
FROM bronze_ERP_PX_CAT_G1V2_TEST;

UPDATE etl_process_log 
SET end_time = NOW(), 
    duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), 
    status = 'Success', 
    message = 'Completed Silver - ERP PX_CAT_G1V2'
WHERE id = @log_id;

-- ===================== GOLD LAYER ===================== --

-- STEP 7: Gold - Customer Dimension
CREATE OR REPLACE VIEW gold_dim_customers_TEST AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    CI.cst_id AS customer_id,
    CI.cst_key AS customer_number,
    CI.cst_firstname AS first_name,
    CI.cst_lastname AS last_name,
    CA.BDATE AS birthday,
    LOC.CNTRY AS country,
    CI.cst_marital_status,
    CASE
        WHEN CI.cst_gndr != 'n/a' THEN CI.cst_gndr
        ELSE COALESCE(CA.GEN, 'n/a')
    END AS gender,
    CI.cst_create_date
FROM silver_CRM_cust_info_TEST CI
LEFT JOIN silver_ERP_CUST_AZ12_TEST CA ON CI.cst_key = CA.CID
LEFT JOIN silver_ERP_LOC_A101_TEST LOC ON CI.cst_key = LOC.CID;

-- STEP 8: Gold - Product Dimension
CREATE OR REPLACE VIEW gold_dim_products_TEST AS
SELECT
    ROW_NUMBER() OVER (ORDER BY PI.prd_start_dt, PI.prd_key) AS product_key,
    PI.prd_id AS product_id,
    PI.prd_key AS product_num,
    PI.prd_nm AS product_name,
    PI.cat_id AS category_id,
    PC.CAT AS category,
    PC.SUBCAT AS subcategory,
    PC.MAINTENANCE,
    PI.prd_cost AS cost,
    PI.prd_line AS product_line,
    PI.prd_start_dt AS start_date
FROM silver_CRM_prd_info_TEST PI
LEFT JOIN silver_ERP_PX_CAT_G1V2_TEST PC ON PI.cat_id = PC.ID;

-- STEP 9: Gold - Sales Fact Table
CREATE OR REPLACE VIEW gold_fact_sales_TEST AS
SELECT
    SD.sls_ord_num AS order_number,
    GDP.product_key,
    GDC.customer_key,
    SD.sls_order_dt AS order_date,
    SD.sls_ship_dt AS shipping_date,
    SD.sls_due_dt AS due_date,
    SD.sls_sales AS sales_amount,
    SD.sls_quantity AS quantity,
    SD.sls_price AS price
FROM silver_CRM_sales_details_TEST SD
LEFT JOIN gold_dim_products_TEST GDP ON SD.sls_prd_key = GDP.product_num
LEFT JOIN gold_dim_customers_TEST GDC ON SD.sls_cust_id = GDC.customer_id;

SELECT * FROM gold_dim_customers_test;
SELECT * FROM gold_dim_products_test;
SELECT * FROM gold_fact_sales_test;
SELECT * FROM etl_process_log;
