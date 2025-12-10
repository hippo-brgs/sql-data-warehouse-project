/*
========================================
CREATION OF THE DATABASE AND THE SCHEMAS
========================================
Script purpose :
This script will create a database named "DataWarehouse" after checking if it already exists. 
If it exists, the existant database will be deleted and replaced by a new empty one. 
Additionally, three schemas will be created in the database (Bronze, Silver, and Gold).

WARNING : If the database exists, make sure to have backups or the datas will be no longer existants
*/
USE master;
GO
  
-- Drop the database if it exists--
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
