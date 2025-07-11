-- Silver Layer Tests --

-- silver_CRM_cust_info
-- silver_CRM_prd_info
SELECT * FROM silver_CRM_prd_info
WHERE prd_key = "HL-U509-R" OR prd_key = "HL-U509";

-- silver_CRM_sales_details

-- Testing crm_sales_details:
SELECT * FROM silver_CRM_sales_details -- Checking if order date always comes before shipping date and due date
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
-- Checking bad calculations
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver_CRM_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;
-- More Tests
SELECT DISTINCT
  sls_sales AS old_sales,
  sls_quantity,
  sls_price AS old_price,

  -- Fix sls_sales safely
  CASE 
    WHEN sls_sales IS NULL 
      OR sls_sales <= 0 
      OR (sls_price IS NOT NULL AND sls_price > 0 AND sls_sales != sls_quantity * ABS(sls_price))
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,

  -- Fix sls_price using sales only if quantity is valid and sls_sales is reliable
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

FROM bronze_CRM_sales_details

WHERE sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
   OR sls_sales != sls_quantity * sls_price

ORDER BY sls_sales, sls_quantity, sls_price;

-- silver_ERP_CUST_AZ12
-- GEN
SELECT DISTINCT
  GEN,
  CASE
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(GEN, '\r', ''), '\n', ''), '\t', ''), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(GEN, '\r', ''), '\n', ''), '\t', ''), ' ', '')) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen_cleaned
FROM bronze_ERP_CUST_AZ12;

SELECT DISTINCT GEN FROM silver_ERP_CUST_AZ12;
-- BDATE
SELECT BDATE FROM silver_ERP_CUST_AZ12 WHERE BDATE > NOW();
SELECT BDATE FROM bronze_ERP_CUST_AZ12 WHERE BDATE > NOW();
SELECT BDATE,
CASE WHEN BDATE > NOW()
	THEN NULL
    ELSE BDATE
END AS BDATE
FROM silver_ERP_CUST_AZ12;
-- CID
SELECT 
CASE WHEN CID LIKE "NAS%" THEN SUBSTRING(CID, 4, LENGTH(CID))
	ELSE CID
END AS CID
FROM bronze_ERP_CUST_AZ12;
-- TESTING WHOLE TABLE:
	SELECT * FROM silver_ERP_CUST_AZ12;
    
-- silver_ERP_LOC_A101
    -- CHECKING FINAL TABLE
SELECT * FROM silver_ERP_LOC_A101;

-- silver_ERP_PX_CAT_G1V2
SELECT * FROM bronze_ERP_PX_CAT_G1V2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);


