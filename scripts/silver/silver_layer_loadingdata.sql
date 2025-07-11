/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

-- ======================================= --
-- ======== silver_CRM_cust_info ========= --
-- ======================================= --

-- TRUNCATE
TRUNCATE silver_CRM_cust_info;

-- Loading Data:
	INSERT INTO silver_CRM_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze_CRM_cust_info
		WHERE cst_id IS NOT NULL
	) AS SUBQ
	WHERE flag_last = 1;

UPDATE bronze_CRM_cust_info
SET cst_create_date = NULL
WHERE CAST(cst_create_date AS CHAR) = '0000-00-00';

-- ======================================= --
-- ======== silver.crm_prd_info ========== --
-- ======================================= --	

-- TRUNCATE:
TRUNCATE TABLE silver_CRM_prd_info;

-- Loading Data:
INSERT INTO silver_CRM_prd_info(
prd_id,
prd_key,
cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

SELECT 
	prd_id, 
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
    prd_nm,
	IFNULL (prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line)) 
		 WHEN 'R' THEN 'Road'
		 WHEN 'M' THEN 'Mountain'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         Else 'n/a'
END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(DATE_SUB(
		LEAD(prd_start_dt) OVER (PARTITION BY REPLACE(SUBSTRING(prd_key,1,5), '-', '_') ORDER BY prd_start_dt),
		INTERVAL 1 DAY) AS DATE
	) AS prd_end_dt
FROM bronze_CRM_prd_info;

-- ======================================= --
-- ========== crm_sales_details ========== --
-- ======================================= --

-- TRUNCATE
TRUNCATE silver_CRM_sales_details;

-- Loading Data:
INSERT INTO silver_CRM_sales_details (
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
  END AS sls_order_dt,
  CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
  END AS sls_ship_dt,
  CASE 
    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
  END AS sls_due_dt,
  CASE 
    WHEN sls_sales IS NULL 
      OR sls_sales <= 0 
      OR (sls_price IS NOT NULL AND sls_price > 0 AND sls_sales != sls_quantity * ABS(sls_price))
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,
  sls_quantity,
  CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 THEN
      CASE 
        WHEN sls_quantity IS NULL OR sls_quantity = 0 THEN NULL
        WHEN sls_sales IS NULL OR sls_sales <= 0 THEN NULL
        WHEN (sls_price IS NOT NULL AND sls_price > 0 AND sls_sales != sls_quantity * ABS(sls_price))
        THEN (sls_quantity * ABS(sls_price)) / NULLIF(sls_quantity, 0)
        ELSE sls_sales / NULLIF(sls_quantity, 0)
      END
    ELSE sls_price
  END AS sls_price
FROM bronze_CRM_sales_details;


-- ======================================= --
-- ======== silver_ERP_CUST_AZ12 ========= --
-- ======================================= --

-- TRUNCATE:
TRUNCATE silver_ERP_CUST_AZ12;

-- Loading Data:

INSERT INTO silver_ERP_CUST_AZ12(
CID,
BDATE,
GEN
)

SELECT 
CASE WHEN CID LIKE "NAS%" THEN SUBSTRING(CID, 4, LENGTH(CID))
	ELSE CID
END AS CID,
CASE WHEN BDATE > NOW()
	THEN NULL
    ELSE BDATE
END AS BDATE,
CASE
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(GEN, '\r', ''), '\n', ''), '\t', ''), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(GEN, '\r', ''), '\n', ''), '\t', ''), ' ', '')) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS GEN
FROM bronze_ERP_CUST_AZ12;

-- ======================================= --
-- ========= silver_ERP_LOC_A101========== --
-- ======================================= --

-- TRUNCATE:
TRUNCATE silver_ERP_LOC_A101;

-- Loading Data:
INSERT INTO silver_ERP_LOC_A101(
CID,
CNTRY
)
SELECT 
REPLACE(CID, '-', '') AS CID, 
CASE
    WHEN UPPER(
           REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CNTRY), '\r', ''), '\n', ''), '\t', ''), ' ', '')
         ) IN ('US', 'USA') THEN 'United States'

    WHEN UPPER(
           REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CNTRY), '\r', ''), '\n', ''), '\t', ''), ' ', '')
         ) = 'DE' THEN 'Germany'

    WHEN CNTRY IS NULL OR
         REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CNTRY), '\r', ''), '\n', ''), '\t', ''), ' ', '') = '' THEN 'n/a'

    ELSE TRIM(CNTRY)
  END AS CNTRY
FROM bronze_ERP_LOC_A101;

SELECT DISTINCT
  CNTRY AS OLD_CNTRY,
  CASE
    WHEN UPPER(
           REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CNTRY), '\r', ''), '\n', ''), '\t', ''), ' ', '')
         ) IN ('US', 'USA') THEN 'United States'

    WHEN UPPER(
           REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CNTRY), '\r', ''), '\n', ''), '\t', ''), ' ', '')
         ) = 'DE' THEN 'Germany'

    WHEN CNTRY IS NULL OR
         REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CNTRY), '\r', ''), '\n', ''), '\t', ''), ' ', '') = '' THEN 'n/a'

    ELSE TRIM(CNTRY)
  END AS CNTRY
FROM bronze_ERP_LOC_A101
ORDER BY CNTRY;

-- ======================================= --
-- ======= silver_ERP_PX_CAT_G1V2 ======== --
-- ======================================= --

-- TRUNCATE:
TRUNCATE silver_ERP_PX_CAT_G1V2;

-- Loading Data:

INSERT INTO silver_ERP_PX_CAT_G1V2(
ID,
CAT,
SUBCAT,
MAINTENANCE
)
SELECT 
ID,
CAT,
SUBCAT,
REPLACE(REPLACE(REPLACE(REPLACE(TRIM(MAINTENANCE), '\r', ''), '\n', ''), '\t', ''), ' ', '')
FROM bronze_ERP_PX_CAT_G1V2;

SELECT * FROM silver_ERP_PX_CAT_G1V2;
SELECT DISTINCT MAINTENANCE FROM silver_ERP_PX_CAT_G1V2;
