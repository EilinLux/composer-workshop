-- ==============================================================================
-- Chapter 04 — BigQuery Upstream Seed Script (seed_sales_data.sql)
-- Reference Notebook: ../04_dataproc_serverless_etl.ipynb
--
-- Creates the `example_composer_workshop` dataset (in EU) and populates the
-- partitioned & clustered `sales_data` table consumed by `bq_etl_job.py`.
-- ==============================================================================

-- 1. Create the BigQuery dataset if it does not already exist
CREATE SCHEMA IF NOT EXISTS `example_composer_workshop`
OPTIONS(
  location = 'EU'
);

-- 2. Create and seed the upstream sales_data table
-- Partitioning by `transaction_date` and clustering by `product_id` minimizes
-- bytes scanned when our daily PySpark job filters on a specific processing date.
CREATE OR REPLACE TABLE `example_composer_workshop.sales_data`
(
  transaction_id STRING,
  product_id STRING,
  amount FLOAT64,
  transaction_date DATE,
  store_region STRING
)
PARTITION BY transaction_date
CLUSTER BY product_id
AS
SELECT * FROM UNNEST([
  STRUCT('TXN-1001' AS transaction_id, 'PROD-A1' AS product_id, 129.99 AS amount, DATE '2025-06-12' AS transaction_date, 'europe-west1' AS store_region),
  STRUCT('TXN-1002', 'PROD-A1', 79.50, DATE '2025-06-12', 'europe-west1'),
  STRUCT('TXN-1003', 'PROD-B2', 249.00, DATE '2025-06-12', 'europe-west1'),
  STRUCT('TXN-1004', 'PROD-C3', 45.20, DATE '2025-06-12', 'europe-west4'),
  STRUCT('TXN-1005', 'PROD-B2', 110.00, DATE '2025-06-12', 'europe-west1'),
  STRUCT('TXN-1006', 'PROD-A1', 199.99, DATE '2025-06-13', 'europe-west1'),
  STRUCT('TXN-1007', 'PROD-D4', 320.00, DATE '2025-06-13', 'europe-west1')
]);
