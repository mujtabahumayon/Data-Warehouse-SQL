/*
=============================================================
Create Database and Schemas (MySQL Workbench)
=============================================================
Script Purpose:
    This script creates a new database schema called 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three more databases/schemas:
    'bronze', 'silver', and 'gold'. 
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA dwh_bronze;

CREATE SCHEMA dwh_silver;

CREATE SCHEMA dwh_gold;
