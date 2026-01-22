
-- ============================================================================
-- DATABASE SCHEMA CREATION - Data Warehouse Layer Structure
-- ============================================================================
-- Purpose: Create three logical schemas for the medallion architecture
--          (Bronze → Silver → Gold data pipeline)


USE DataWarehouse;    -- Switch to target database
GO

CREATE SCHEMA bronze;   -- will contain Raw data from source systems (CRM, ERP)
GO

CREATE SCHEMA silver;  -- will contain Cleaned and validated data
GO

CREATE SCHEMA gold;    -- will contain Business-ready aggregated data for reporting
GO

