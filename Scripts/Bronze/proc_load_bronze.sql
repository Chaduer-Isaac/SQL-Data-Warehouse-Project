/*

Stored Procedure: Load Bronze Layer (Source -> Bronze)

Parameters:

None.

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT' command to load data from csv Files to bronze tables.

I
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Bronze.load_bronze;

*/


CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
DECLARE @Start_time DATETIME, @End_time DATETIME,
@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
SET @batch_end_time = GETDATE();
PRINT'===========================';
PRINT 'Loading Bronze Layer';
PRINT'===========================';

PRINT '----------------------------';
PRINT 'Loading CRM columns';
PRINT '----------------------------';

SET @Start_time=GETDATE();
TRUNCATE TABLE Bronze.crm_cust_info
BULK INSERT Bronze.crm_cust_info
FROM 'C:\Users\MICROSOFT\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
FIRSTROW = 2,
FIELDTERMINATOR= ',',
TABLOCK
);
SET @End_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + ' seconds'
PRINT '--------------------------'

SET @Start_time=GETDATE();
TRUNCATE TABLE Bronze.crm_prd_info
BULK INSERT Bronze.crm_prd_info
FROM 'C:\Users\MICROSOFT\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
FIRSTROW = 2,
FIELDTERMINATOR= ',',
TABLOCK
);
SET @End_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + ' seconds'
PRINT '--------------------------'

SET @End_time = GETDATE();
TRUNCATE TABLE Bronze.crm_sales_details
BULK INSERT Bronze.crm_sales_details
FROM 'C:\Users\MICROSOFT\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
FIRSTROW = 2,
FIELDTERMINATOR= ',',
TABLOCK
);
SET @End_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + ' seconds'
PRINT '--------------------------'


PRINT '----------------------------';
PRINT 'Loading ERP columns';
PRINT '----------------------------';

SET @End_time = GETDATE();
TRUNCATE TABLE Bronze.erp_cust_az12
BULK INSERT Bronze.erp_cust_az12
FROM 'C:\Users\MICROSOFT\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
FIRSTROW = 2,
FIELDTERMINATOR= ',',
TABLOCK
);
SET @End_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + ' seconds'
PRINT '--------------------------'

SET @End_time = GETDATE();
TRUNCATE TABLE Bronze.erp_loc_a101
BULK INSERT Bronze.erp_loc_a101
FROM 'C:\Users\MICROSOFT\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
FIRSTROW = 2,
FIELDTERMINATOR= ',',
TABLOCK
);
SET @End_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + ' seconds'
PRINT '--------------------------'

SET @End_time = GETDATE();
TRUNCATE TABLE Bronze.erp_px_cat_g1v2
BULK INSERT Bronze.erp_px_cat_g1v2
FROM 'C:\Users\MICROSOFT\Downloads\datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
FIRSTROW = 2,
FIELDTERMINATOR= ',',
TABLOCK
);
SET @End_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@Start_time,@End_time) AS NVARCHAR) + ' seconds'
PRINT '--------------------------'

SET @batch_end_time = GETDATE();
PRINT '============================'
PRINT 'loading bronze completed';
PRINT 'Total loading duration:' + CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time)
AS NVARCHAR) + 'Seconds';
PRINT '============================'
END TRY

BEGIN CATCH
PRINT '=============================================';
PRINT 'ERROR OCCURED DURING LOADING MESSAGE!';
PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT '=============================================';
END CATCH
END
