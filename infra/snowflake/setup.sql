-- Snowflake setup for the Q-commerce platform (run once, as ACCOUNTADMIN, with "Run All").
USE ROLE ACCOUNTADMIN;

-- Small warehouse that suspends itself after 60 seconds to save credits
CREATE WAREHOUSE IF NOT EXISTS QCOMMERCE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database and the three medallion layers
CREATE DATABASE IF NOT EXISTS QCOMMERCE;
CREATE SCHEMA IF NOT EXISTS QCOMMERCE.BRONZE;   -- raw Kafka events (written by services/bronze_loader)
CREATE SCHEMA IF NOT EXISTS QCOMMERCE.SILVER;   -- staging, SCD2, dimensions and facts (dbt)
CREATE SCHEMA IF NOT EXISTS QCOMMERCE.GOLD;     -- business marts (dbt)

-- Optional: make the warehouse the default for each team member (repeat per user)
-- ALTER USER <user_name> SET DEFAULT_WAREHOUSE = QCOMMERCE_WH;
