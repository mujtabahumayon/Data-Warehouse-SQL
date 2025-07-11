-- ======= Silver Layer: CREATING TABLES ====== --

-- Drop table if it exists
DROP TABLE IF EXISTS silver_crm_prd_info;
-- Create Table
CREATE TABLE silver_CRM_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME DEFAULT NOW()
);

-- Drop table if it exists
DROP TABLE IF EXISTS silver_CRM_prd_info;
-- Create Table
CREATE TABLE silver_CRM_prd_info(
prd_id INT,
cat_id VARCHAR(50),
prd_key	VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
dwh_create_date DATETIME DEFAULT NOW()
);

DROP TABLE TEST_silver_CRM_prd_info;
CREATE TABLE TEST_silver_CRM_prd_info(
prd_id INT,
prd_key	VARCHAR(50),
cat_id VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME,
dwh_create_date DATETIME DEFAULT NOW()
);

-- Drop table if it exists
DROP TABLE IF EXISTS silver_CRM_sales_details;
-- Create Table
CREATE TABLE silver_CRM_sales_details(
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

-- Drop table if it exists
DROP TABLE IF EXISTS silver_ERP_CUST_AZ12;
-- Create Table
CREATE TABLE silver_ERP_CUST_AZ12(
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

-- Drop table if it exists
DROP TABLE IF EXISTS silver_ERP_LOC_A101;
-- Create Table
CREATE TABLE silver_ERP_LOC_A101(
CID VARCHAR(50),
CNTRY VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);

-- Drop table if it exists
DROP TABLE IF EXISTS silver_ERP_PX_CAT_G1V2;
-- Create Table
CREATE TABLE silver_ERP_PX_CAT_G1V2(
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50),
dwh_create_date DATETIME DEFAULT NOW()
);
