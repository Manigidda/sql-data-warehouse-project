/*
================================================================================
Stored procedure : Load Silver Layer (Bronze -> Silver)
================================================================================
Script purpose:
    This Stored Peorforms the ETL (Extract, Transfer, Load) process to populate
    the 'Silver' schema tables from the 'bronze' schema.
  Actions Performed:
      - Truncate Silver tables.
      - Inserts transformed and cleansed data from Bronze into Silver tables.
  Parameters:
    None.
    This stored procedure does not accept any paramters or return any values.

Usage Example:
  EXEC silver.load_silver;
=================================================================================
*/


CREATE OR ALTER PROCEDURE SILVER.load_silver AS 
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME
SET @batch_start_time= GETDATE();
SET @start_time=GETDATE();
PRINT '>>TRUNCATING TABLE SILVER.crm_cust_info<<';
PRINT '>> INSERTING INTO TABLE SILVER.crm_cust_info';
TRUNCATE TABLE SILVER.crm_cust_info;
insert into SILVER.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,cst_create_date
)
select cst_id,cst_key,TRIM(cst_firstname) AS "cst_firstname",TRIM(cst_lastname) as "cst_lastname",
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
     ELSE 'N/A'
END cst_marital_status,
CASE WHEN UPPER(cst_gndr) = 'M' then 'Male'
     WHEN UPPER(cst_gndr) = 'F' then 'Female'
     ELSE 'N/A'
END cst_gndr,
cst_create_date
from 
(select *, Row_number() over (partition by cst_id order by cst_create_date desc) AS ROW_RK
from bronze.crm_cust_info)t
WHERE ROW_RK=1;
SET @end_time = GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT '>>-------------------------------------------------------------------------------------<<'
SET @start_time=GETDATE();
PRINT '>>TRUNCATING TABLE SILVER.erp_loc_a101<<';
TRUNCATE TABLE SILVER.erp_loc_a101; 

PRINT '>>INSERT INTO TABLE SILVER.erp_loc_a101<<';
INSERT INTO SILVER.erp_loc_a101(
CID,
CNTRY)
select REPLACE(CID , '-','') AS CID,
CASE WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
     WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
     when UPPER(TRIM(CNTRY))='DE' THEN 'Germany' 
     ELSE TRIM(CNTRY)
END CNTRY
from bronze.erp_loc_a101;
SET @end_time = GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT'>>-------------------------------------------------------------------------------------<<';
SET @start_time=GETDATE();
PRINT '>>TRUNCATING TABLE SILVER.erp_px_cat_gv12<<'; 
TRUNCATE TABLE SILVER.erp_px_cat_g1v2;
PRINT '>>INSERT INTO TABLE SILVER.erp_px_cat_gv12<<';
INSERT INTO SILVER.erp_px_cat_g1v2 (
ID,
CAT,
SUBCAT,
MAINTENANCE
)
SELECT TRIM(ID),TRIM(CAT),TRIM(SUBCAT),TRIM(MAINTENANCE) FROM bronze.erp_px_cat_g1v2;
SET @end_time=GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT '>>-------------------------------------------------------------------------------------<<'
SET @start_time=GETDATE();
PRINT '>>TRUNCATING TABLE SILVER.erp_cust_az12<<';
TRUNCATE TABLE SILVER.erp_cust_az12;
PRINT '>>INSERTED INTO TABLE SILVER.erp_cust_az12<<';
insert into SILVER.erp_cust_az12 ( 
CID,
BDATE,
GEN)
select
CASE WHEN UPPER(CID) LIKE 'NAS%' then SUBSTRING(CID,4,LEN(CID))
     ELSE CID 
END CID,
CASE WHEN BDATE > GETDATE() THEN NULL 
     ELSE BDATE
END BDATE,
CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE')  THEN 'Female'
     WHEN UPPER(TRIM(GEN)) IN ('M','MALE')    THEN 'Male'
     else 'N/A'
END GEN
FROM bronze.erp_CUST_AZ12;
SET @end_time=GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT '>>-------------------------------------------------------------------------------------<<'
SET @start_time=GETDATE();
PRINT '>>TRUNCATING TABLE SILVER.crm_sales_details<<';
TRUNCATE TABLE SILVER.crm_sales_details ;
PRINT '>>INSERTED INTO TABLE SILVER.crm_sales_details<<';
insert into SILVER.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)
SELECT sls_ord_num,
sls_prd_key,
sls_cust_id, 
CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8
     THEN NULL
else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt, 
CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 
     THEN NULL
else CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt)!=8
     THEN NULL
else CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
CASE WHEN sls_sales <=0 OR SLS_SALES IS NULL OR SLS_SALES != SLS_QUANTITY * ABS(SLS_PRICE)
     THEN SLS_QUANTITY * ABS(SLS_PRICE)
ELSE sls_sales 
end sls_sales,
sls_quantity,
CASE WHEN SLS_PRICE <=0 OR SLS_PRICE IS NULL
     THEN SLS_SALES / NULLIF(SLS_QUANTITY,0)
ELSE sls_price
END sls_price
from bronze.crm_sales_details;
SET @end_time=GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT '>>-------------------------------------------------------------------------------------<<'
SET @start_time=GETDATE();
PRINT '>>TRUNCATING TABLE SILVER.crm_prd_info<<';
TRUNCATE TABLE SILVER.crm_prd_info;
PRINT '>>INSERTED INTO TABLE SILVER.crm_prd_info<<';
insert into SILVER.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select prd_id,REPLACE(SUBSTRING(prd_key,1,5),'-','_') as "cat_id",
SUBSTRING(prd_key,7,len(prd_key)) AS 'prd_key',prd_nm,ISNULL(prd_cost,0) as "prd_cost",
CASE UPPER(TRIM(prd_line))
           when UPPER(TRIM('R')) then 'Road'
           when UPPER(TRIM('M')) then 'Mountain'
           when UPPER(TRIM('S')) then 'Other sales'
           when UPPER(TRIM('T')) then 'Touring'
           else 'N/A'
end 'prd_line',
CAST(prd_start_dt AS DATE) as "prd_start_dt",
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt)-1 AS DATE) as "prd_end_dt"
from bronze.crm_prd_info;
SET @end_time = GETDATE();
PRINT '>> Loading time:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
PRINT '>>-------------------------------------------------------------------------------------<<'
SET @batch_end_time=GETDATE();
print 'WHOLE Table Loadig Duration' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+'seconds'
END
