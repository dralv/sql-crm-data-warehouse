/*
================================================================================
 Script Name  : init_database.sql
 Description  : This script drops the existing 'DataWareHouseCrm' database 
                (if it exists), recreates it, and initializes the base schemas
                used for a multi-layered data architecture (bronze, silver, gold).
                
                - bronze: Raw data ingestion layer
                - silver: Cleaned and transformed data
                - gold  : Final curated data for analytics and reporting

 Author       : Alvaro
 Created Date : 05-13-2025
 Environment  : SQL Server

--------------------------------------------------------------------------------
 WARNING      : This script will irreversibly DROP the 'DataWareHouseCrm' 
                database if it already exists. All data will be permanently lost.
                Ensure that a proper backup has been taken before execution.
--------------------------------------------------------------------------------
================================================================================
*/
USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWareHouseCrm')
BEGIN 
	ALTER DATABASE DataWareHouseCrm SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouseCrm
END;
GO

CREATE DATABASE DataWareHouseCrm;

USE DataWareHouseCrm;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;