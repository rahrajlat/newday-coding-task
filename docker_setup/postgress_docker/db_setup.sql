-- Creates common schemas for a dbt project. Safe to run multiple times.

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS tests;
CREATE SCHEMA IF NOT EXISTS dbt;  -- often used for dbt-generated objects


-- Optional: comment each schema (handy in UI tools)
COMMENT ON SCHEMA raw IS 'Landing/raw data';
COMMENT ON SCHEMA staging IS 'Cleaned/typed staging models';
COMMENT ON SCHEMA intermediate IS 'Transformations between staging and marts';
COMMENT ON SCHEMA marts IS 'Business-facing models';
COMMENT ON SCHEMA tests IS 'Test artifacts';
COMMENT ON SCHEMA dbt IS 'dbt internal/working schema';