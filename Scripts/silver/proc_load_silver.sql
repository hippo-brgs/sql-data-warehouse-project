/*
==========================================================================
Stored Procedure : Load Silver Layer (Bronze --> Silver)
==========================================================================
Script purpose :
This stored procedure performs the ETL process (Extract, Transform, Load) of to populate the 'silver' schema tables from the 'bronze' schema.
Actions performed :
  - Truncate Silver tables
  - Insert transformed and cleansed data from Bronze into Silver layer
Parameters :
None
This procedure does not accept parameters nor return any value

Usage example :
EXEC silver.load_silver;
==========================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
BEGIN TRY
	PRINT '==================================================================='
	PRINT 'Loading the silver layer'
	PRINT '==================================================================='
	PRINT '-------------------------------------------------------------------'
	PRINT 'Loading the CRM files'
	PRINT '-------------------------------------------------------------------'
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_silver DATETIME, @end_time_silver DATETIME;
	SET @start_time_silver = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT '>> Inserting Data Into : silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

	SELECT cst_id,
		cst_key,
		-- Delete unwanted spaces
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		-- Normalize coded data into user-friendly data
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM (
		-- Delete duplicates of the primary key (chooses the most recent iteration of the identified duplicates)
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
			)t WHERE t.flag_last = 1
	SET @end_time = GETDATE();
	PRINT'>>Loading time : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT'~~~~~~~~~~~~~~~~~~~~~~~~'

	--==========================================================================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	PRINT '>> Inserting Data Into : silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

	SELECT
		prd_id,
		REPLACE(SUBSTRING (prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING (prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 WHEN 'R' THEN 'Road'
			 ELSE 'n/a'
		END AS prd_line,
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT'>>Loading time : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT'~~~~~~~~~~~~~~~~~~~~~~~~'

	--==========================================================================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	PRINT '>> Inserting Data Into : silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT'>>Loading time : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT'~~~~~~~~~~~~~~~~~~~~~~~~'

	--==========================================================================================================
	SET @start_time = GETDATE();
	PRINT '-------------------------------------------------------------------'
	PRINT 'Loading the ERP files'
	PRINT '-------------------------------------------------------------------'
	PRINT '>> Truncating Table : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT '>> Inserting Data Into : silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)

	SELECT
		CASE WHEN cid LIKE 'NAS%'
			THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE()
			THEN NULL
			ELSE bdate
		END AS bdate,
		CASE  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
	PRINT'>>Loading time : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT'~~~~~~~~~~~~~~~~~~~~~~~~'

	--==========================================================================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT '>> Inserting Data Into : silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid, cntry)

	SELECT
		REPLACE (cid,'-','') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
			WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT'>>Loading time : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT'~~~~~~~~~~~~~~~~~~~~~~~~'

	--==========================================================================================================
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	PRINT '>> Inserting Data Into : silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)

	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT'>>Loading time : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT'~~~~~~~~~~~~~~~~~~~~~~~~'
	SET @end_time_silver = GETDATE();
	PRINT'=========================================================================='
	PRINT'Silver layer loading completed'
	PRINT'>>Loading time of the silver layer: ' + CAST(DATEDIFF(second, @start_time_silver, @end_time_silver) AS NVARCHAR) + ' seconds'
	PRINT'=========================================================================='
END TRY
BEGIN CATCH
	PRINT '========================================================================'
	PRINT 'An error occured'
	PRINT 'Error message' + ERROR_MESSAGE();
	PRINT 'Error message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error message' + CAST(ERROR_STATE() AS NVARCHAR);
END CATCH
END
