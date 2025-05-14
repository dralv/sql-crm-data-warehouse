/*
================================================================================
 Procedure Name : bronze.load_bronze
 Description    : Loads raw data into the 'bronze' layer from flat CSV files 
                  using BULK INSERT operations. This procedure truncates existing
                  data in the staging tables before loading new records from the 
                  file system.

                  Source: Local filesystem paths pointing to CRM and ERP datasets.

                  Tables Loaded:
                  - bronze.crm_cust_info
                  - bronze.crm_prd_info
                  - bronze.crm_sales_details
                  - bronze.erp_cust_az12
                  - bronze.erp_loc_a101
                  - bronze.erp_px_cat_g1v2

 Author         : Alvaro Araujo
 Created Date   : 05-14-2025
 Environment    : SQL Server

--------------------------------------------------------------------------------
 WARNING        : This procedure TRUNCATES all data in the listed tables before
                  reloading. Ensure data integrity and backup strategies are in 
                  place prior to execution.

                  Also note that BULK INSERT paths are hardcoded. This script will 
                  fail if run on another environment without access to the same
                  local file system paths.
--------------------------------------------------------------------------------
================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME;
	SET @bronze_start_time = GETDATE();
	BEGIN TRY
		PRINT '*|========================|*';
		PRINT 'Loading Bronze Layer';
		PRINT '*|========================|*';

		PRINT '*|------------------------|*';
		PRINT 'Loading CRM Tables';
		PRINT '*|------------------------|*';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\dralv\Documents\Engenharia de Dados\sql-crm-data-warehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\dralv\Documents\Engenharia de Dados\sql-crm-data-warehouse\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\dralv\Documents\Engenharia de Dados\sql-crm-data-warehouse\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------';


		PRINT '*|------------------------|*';
		PRINT 'Loading ERP Tables';
		PRINT '*|------------------------|*';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\dralv\Documents\Engenharia de Dados\sql-crm-data-warehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\dralv\Documents\Engenharia de Dados\sql-crm-data-warehouse\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\dralv\Documents\Engenharia de Dados\sql-crm-data-warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------';
	END TRY
	BEGIN CATCH
		PRINT '===================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================';
	END CATCH
SET @bronze_end_time = GETDATE();
PRINT '>> Bronze layer Load Duration: ' + CAST(DATEDIFF(second,@bronze_start_time, @bronze_end_time) AS NVARCHAR) + 'seconds';
PRINT '>> -------------------------';
END;