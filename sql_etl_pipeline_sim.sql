
-- SQL ETL Pipeline Simulation using SQLite + DB Browser
-- Deliverables: SQL scripts, ETL logs, cleaned production tables

-- Step 0: Drop tables if they exist (for clean reruns)
DROP TABLE IF EXISTS staging_sales;
DROP TABLE IF EXISTS production_sales;
DROP TABLE IF EXISTS audit_log;

-- Step 1: Create Staging Table (used to import CSV data)
CREATE TABLE staging_sales (
    sale_id INTEGER,
    customer_name TEXT,
    product TEXT,
    quantity INTEGER,
    price REAL,
    sale_date TEXT
);

-- Step 2: Create Production Table (cleaned, transformed data)
CREATE TABLE production_sales (
    sale_id INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    product TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    total_amount REAL NOT NULL,
    sale_date TEXT NOT NULL
);

-- Step 3: Create Audit Table
CREATE TABLE audit_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    inserted_on TEXT DEFAULT CURRENT_TIMESTAMP,
    records_inserted INTEGER
);

-- Step 4: Transform and Load Clean Data
INSERT INTO production_sales (sale_id, customer_name, product, quantity, total_amount, sale_date)
SELECT 
    sale_id,
    TRIM(customer_name),
    TRIM(product),
    quantity,
    quantity * price AS total_amount,
    sale_date
FROM staging_sales
WHERE sale_id IS NOT NULL
  AND customer_name IS NOT NULL
  AND product IS NOT NULL
  AND quantity IS NOT NULL
  AND price IS NOT NULL
GROUP BY sale_id;  -- Deduplicate

-- Step 5: Log inserts
INSERT INTO audit_log (records_inserted)
SELECT COUNT(*) FROM production_sales;

-- Step 6: Create Trigger to auto-delete from staging after transfer
DROP TRIGGER IF EXISTS cleanup_staging;
CREATE TRIGGER cleanup_staging
AFTER INSERT ON production_sales
BEGIN
    DELETE FROM staging_sales WHERE sale_id IN (SELECT sale_id FROM production_sales);
END;

-- Step 7: Optional View to Inspect
CREATE VIEW view_sales_summary AS
SELECT product, SUM(quantity) AS total_sold, SUM(total_amount) AS total_revenue
FROM production_sales
GROUP BY product;
