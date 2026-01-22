
/* This script creates a stored procedure that loads data from CSV files into the 6 bronze layer tables. 
It uses TRUNCATE (to clear old data) and BULK INSERT (to efficiently load CSV data) for each table, 
making the entire ETL process reusable by simply executing EXEC bronze.load_bronze.*/
 
-- Process Flow:
--   1. Truncate existing data in bronze tables (full refresh approach)
--   2. Bulk load CSV data from CRM system (3 tables)
--   3. Bulk load CSV data from ERP system (3 tables)
--
-- Notes:
--   - TRUNCATE removes all data but keeps table structure
--   - BULK INSERT is optimized for loading large CSV files
--   - TABLOCK improves performance by using table-level locking
--   - FIRSTROW = 2 skips the header row in CSV files
-- ============================================================================



CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS 
BEGIN 
    -- ========================================================================
    -- Execution Start
    -- ========================================================================
    PRINT 'Starting Bronze Layer Data Load Process...'
    PRINT 'Timestamp: ' + CONVERT(VARCHAR, GETDATE(), 120)
    PRINT '============================================================================'
    
    -- ========================================================================
    -- CRM SYSTEM DATA LOADS
    -- ========================================================================
    PRINT 'SOURCE: CRM System'
    PRINT '----------------------------------------------------------------------------'
    
    -- ------------------------------------------------------------------------
    -- Load: Customer Information
    -- ------------------------------------------------------------------------
    PRINT '  [1/3] Loading bronze.crm_cust_info...'
    
    -- Clear existing data to prevent duplicates and capture any source changes
    TRUNCATE TABLE bronze.crm_cust_info; 
    
    -- Bulk load customer data from CSV
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\DELL\Downloads\DWH project SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,              -- Skip header row
        FIELDTERMINATOR = ',',     -- CSV comma delimiter
        TABLOCK                    -- Use table lock for better performance
    );
    
    PRINT '  ✓ Completed: bronze.crm_cust_info'
    
    -- ------------------------------------------------------------------------
    -- Load: Product Information
    -- ------------------------------------------------------------------------
    PRINT '  [2/3] Loading bronze.crm_prd_info...'
    
    TRUNCATE TABLE bronze.crm_prd_info; 
    
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\DELL\Downloads\DWH project SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    
    PRINT '  ✓ Completed: bronze.crm_prd_info'
    
    -- ------------------------------------------------------------------------
    -- Load: Sales Transaction Details
    -- ------------------------------------------------------------------------
    PRINT '  [3/3] Loading bronze.crm_sales_details...'
    
    TRUNCATE TABLE bronze.crm_sales_details; 
    
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\DELL\Downloads\DWH project SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    
    PRINT '  ✓ Completed: bronze.crm_sales_details'
    
    PRINT '============================================================================'
    
    -- ========================================================================
    -- ERP SYSTEM DATA LOADS
    -- ========================================================================
    PRINT 'SOURCE: ERP System'
    PRINT '----------------------------------------------------------------------------'
    
    -- ------------------------------------------------------------------------
    -- Load: Customer Location Data
    -- ------------------------------------------------------------------------
    PRINT '  [1/3] Loading bronze.erp_loc_a101...'
    
    TRUNCATE TABLE bronze.erp_loc_a101; 
    
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\DELL\Downloads\DWH project SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    
    PRINT '  ✓ Completed: bronze.erp_loc_a101'
    
    -- ------------------------------------------------------------------------
    -- Load: Customer Demographic Data
    -- ------------------------------------------------------------------------
    PRINT '  [2/3] Loading bronze.erp_cust_az12...'
    
    TRUNCATE TABLE bronze.erp_cust_az12; 
    
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\DELL\Downloads\DWH project SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    
    PRINT '  ✓ Completed: bronze.erp_cust_az12'
    
    -- ------------------------------------------------------------------------
    -- Load: Product Category & Maintenance Data
    -- ------------------------------------------------------------------------
    PRINT '  [3/3] Loading bronze.erp_px_cat_g1v2...'
    
    TRUNCATE TABLE bronze.erp_px_cat_g1v2; 
    
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\DELL\Downloads\DWH project SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    
    PRINT '  ✓ Completed: bronze.erp_px_cat_g1v2'
    
    -- ========================================================================
    -- Execution Complete
    -- ========================================================================
    PRINT '============================================================================'
    PRINT 'Bronze Layer Data Load Process COMPLETED Successfully!'
    PRINT 'Timestamp: ' + CONVERT(VARCHAR, GETDATE(), 120)
    PRINT '============================================================================'
    
END -- End of stored procedure

-- ============================================================================
-- VERIFICATION & USAGE
-- ============================================================================
-- Location: Database Explorer > DataWarehouse > Programmability > 
--           Stored Procedures > bronze.load_bronze
