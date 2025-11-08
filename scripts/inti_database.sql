/*
===============================================
Create Database and Schemas
==============================================
Script Purpose:
the script creates a new database named 'Datawarehouse' after checking if it already exists.
If the database exists, it is droppped and recreated. Additionally, the script sets up three schemas
within the database (datawarehouse): 'bronze', 'SILVER', 'GOLD'

WARNING:
Running this script will drop the entire database 'Datawarehouse' database if it
exists.
All the data in database will be permentaly deleted. Proceed with caution and ensure you have proper backup before proceeding this script.
*/

USE master;
GO
--Drop and recreate the database 'Datawarehouse'
IF EXISTS (select 1 from sys.database where name='Datawarehouse')
alter DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATELY
DROP DATABASE Datawarehouse;
END;
GO
--create database Datawarehouse--
create DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

--create schemas--
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO
