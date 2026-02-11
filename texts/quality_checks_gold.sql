/*
============================================================================
Quality checks
============================================================================
Script purpose :
This script performs quality checks to validate the integrity, consistency, and accuracy of the gold layer.
These checks ensure :
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integrity between fact and dimension tables.
  - Validation of relationships in the data model for analytical purposes.

Usage notes :
Run these checks after data loading silver layer
Investigate and resolve any discrepancies found during the checks
============================================================================
*/

-- =========================================================================
-- Checking 'gold.customer_key'
-- =========================================================================
-- Checks for uniqueness of customer key in gold.dim_customers
-- Epectation : No result
SELECT customer_key,
COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key HAVING COUNT(*) > 1;


-- =========================================================================
-- Checking 'gold.product_key'
-- =========================================================================
-- Checks for uniqueness of product key in gold.dim_products
-- Epectation : No result
SELECT product_key,
COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key HAVING COUNT(*) > 1;


-- =========================================================================
-- Checking 'gold.fact_sales'
-- =========================================================================
-- Checks the connectivity between fact and dimensions
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
