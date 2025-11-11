/*
================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schemas from the external CSV Files.
  It Performs the below operations:
  - Truncate the data before inserting into tables.
  - Use 'Bulk Insert' Command to load data from CSV Files to bronze Tables.

 Parameters:
 None.
This stored procedure does not accept any  parameters or return any values.

Usage Example :
EXEC bronze.load_bronze;
=================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME
SET @batch_start_time = GETDATE();
SET @start_time=GETDATE();
PRINT 'truncating table:crm_cust_info';
truncate table bronze.crm_cust_info;
print '>> insertintg into table:crm_cust_info';
bulk insert bronze.crm_cust_info
from 'C:\Users\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
SET @end_time=GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT '----------------------------------------------------------------------------'
SET @start_time=GETDATE();
PRINT '>>truncate table bronze.crm_prd_info';
truncate table bronze.crm_prd_info;
print 'insertint data into table:';
bulk insert bronze.crm_prd_info
from 'C:\Users\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>loading time duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
PRINT '----------------------------------------------------------------------------'
SET @start_time=GETDATE();
PRINT '>> truncate table bronze.crm_sales_details';
truncate table bronze.crm_sales_details;
PRINT '>> DATA INSERT INTO TABLE bronze.crm_sales_details';
bulk insert bronze.crm_sales_details
from 'C:\Users\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
SET @end_time = GETDATE();
PRINT '>> LOADING TIME:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------------------------------------------'
SET @start_time=GETDATE();
PRINT '>> truncate table bronze.erp_CUST_AZ12';
truncate table bronze.erp_CUST_AZ12;
PRINT '>> ISERTING DATA INTO TABLE :bronze.erp_CUST_AZ12';
bulk insert bronze.erp_CUST_AZ12
from 'C:\Users\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>loading data time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------------------------------------------'
SET @start_time=GETDATE();
PRINT 'truncating table bronze.erp_LOC_A101';
truncate table bronze.erp_LOC_A101;
PRINT 'inserting data into table:bronze.erp_LOC_A101';
bulk insert bronze.erp_LOC_A101
from 'C:\Users\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
);
SET @end_time=GETDATE();
PRINT '>>loading data time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------------------------------------------'
SET @start_time=GETDATE();
print '>> truncating table bronze.erp_PX_CAT_G1V2';
truncate table bronze.erp_PX_CAT_G1V2;
PRINT '>> Inserting data into table:bronze.erp_PX_CAT_G1V2';
bulk insert bronze.erp_PX_CAT_G1V2
from 'C:\Users\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
firstrow = 2,
fieldterminator = ',',
tablock
)
SET @end_time=GETDATE();
PRINT '>>loading data time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '------------------------------------------------------------------------------------------';
SET @batch_end_time=GETDATE();
PRINT '>> WHOLE table loading DURATION:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
END
