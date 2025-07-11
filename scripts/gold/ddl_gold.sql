-- ========= Gold Layer ========= --

-- Dimension Customer: Description of Customer (Object: 1)
CREATE VIEW gold_dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate Key
	CI.cst_id AS customer_id,
	CI.cst_key AS customer_number,
	CI.cst_firstname AS first_name,
	CI.cst_lastname AS last_name,
	CA.BDATE AS birthday,
	loc.CNTRY AS country,
	CI.cst_marital_status AS marital_status,
		CASE 
			WHEN CI.cst_gndr != 'n/a' then CI.cst_gndr -- <IF CRM is Master Data for Gender> 
			ELSE COALESCE(CA.gen, 'n/a')
		END AS gender,
	CI.cst_create_date AS create_date
FROM silver_CRM_cust_info CI
	LEFT JOIN silver_ERP_CUST_AZ12 CA 
			ON CI.cst_key = CA.CID
	LEFT JOIN silver_ERP_LOC_A101 loc
			ON CI.cst_key = loc.CID;


-- Dimension Product: Description of Product (Object: 2)
CREATE VIEW gold_dim_products AS
-- SELECT prd_key, COUNT(*) FROM(  			<> Checking for Duplicates <>
SELECT
	ROW_NUMBER() OVER (ORDER BY PI.prd_start_dt, PI.prd_key) AS product_key, -- Surrogate Key
	PI.prd_id AS product_id,
	PI.prd_key AS product_num,
    PI.prd_nm product_name,
	PI.cat_id AS category_id,
	PC.CAT AS category,
    PC.SUBCAT AS subcategory,
    PC.MAINTENANCE,
	PI.prd_cost AS cost,
	PI.prd_line AS product_line,
	PI.prd_start_dt AS start_date
FROM silver_CRM_prd_info PI
LEFT JOIN silver_ERP_PX_CAT_G1V2 PC
	ON PI.cat_id = PC.ID;
	-- WHERE PI.prd_end_dt IS NULL;				-- Filtering out Historical Data
    -- )t GROUP BY prd_key					<> Checking for Duplicates <>
    -- HAVING COUNT(*) > 1; 				<> Checking for Duplicates <>
    


-- Transaction Records: Facts about Transactions (Object: 3)
SELECT * FROM gold_fact_sales;
CREATE VIEW gold_fact_sales AS
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
FROM silver_CRM_sales_details SD
LEFT JOIN gold_dim_products GDP
	ON SD.sls_prd_key = GDP.product_num
LEFT JOIN gold_dim_customers GDC
	ON SD.sls_cust_id = GDC.customer_id;

-- Foreign Key Integrity Check (Dimensions)
SELECT * FROM gold_fact_sales f
LEFT JOIN gold_dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold_dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL;



