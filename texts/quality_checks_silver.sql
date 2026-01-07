/*
=============================================================================
Quality Checks
=============================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schema. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
=============================================================================
*/
-- If data is a string and an initial : check how many
SELECT DISTINCT prd_line FROM bronze.crm_prd_info
GO
-- If data is a string and an initial : make lowercases uppercases, and change to full word
--...
cst_gndr,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
		THEN 'n/a'
--...
--If data is a string : check for unwanted spaces
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname)
GO
--If data is a primary key : check for repetition
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*)>1 OR cst_id IS NULL
GO

--Check for nulls or negative numbers
SELECT prd_cost FROM bronze.crm_prd_info WHERE prd_cost IS NULL or prd_cost < 0

-- Invalid date order
SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price, 
FROM bronze.crm_sales_details 
WHERE sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price
