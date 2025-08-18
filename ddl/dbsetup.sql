-- This script create required database and database tables

-- CREATE [bronze_db]
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'bronze_db')
CREATE DATABASE bronze_db
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'bronze_db', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\bronze_db.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'bronze_db_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\bronze_db_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT
GO

-- CREATE [silver_db]
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'silver_db')
CREATE DATABASE silver_db
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'silver_db', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\silver_db.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'silver_db_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\silver_db_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT
GO

-- CREATE [gold_db]
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'gold_db')
CREATE DATABASE gold_db
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'gold_db', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\gold_db.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'gold_db_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\gold_db_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT
GO

-- CREATE [system_db]
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'system_db')
CREATE DATABASE system_db
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'system_db', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\system_db.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'system_db_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL15.SQLEXPRESS\MSSQL\DATA\system_db_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT
GO

USE  system_db;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ach_runs' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE system_db.dbo.ach_runs (runid integer, createdate datetime, startdatetimeid datetime, enddatetimeid datetime, status varchar(20))
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ach_logs' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE system_db.dbo.ach_logs (runid integer, loglevel varchar(30), message_line varchar(max), logtime datetime);
GO

DROP SEQUENCE IF EXISTS dbo.sq_runid;

CREATE SEQUENCE dbo.sq_runid START WITH 1 INCREMENT BY 1  MINVALUE 1  CYCLE CACHE 10;    

USE bronze_db;
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'raw_customers' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.raw_customers (
    CustomerID     INT,
    Age            INT,            
    Gender         VARCHAR(10),    
    Tenure         INT,            
    MonthlyCharges DECIMAL(10,2),  
    ContractType   VARCHAR(20),    
    InternetService VARCHAR(20),   
    TotalCharges   DECIMAL(12,2),  
    TechSupport    VARCHAR(10),    
    Churn          VARCHAR(10),
	inserttime	   DATETIME,
    runid          INT	       
);

USE silver_db;
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'customers' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.customers (
    CustomerID     INT,
    Age_band       VARCHAR(20),            
    Gender         VARCHAR(20),    
    Tenure         INT,            
    Tenure_band    VARCHAR(20),         
    MonthlyCharges DECIMAL(10,2),  
    ContractType   VARCHAR(20),    
    InternetService VARCHAR(20),   
    TotalCharges   DECIMAL(12,2),  
    TechSupport    VARCHAR(10),    
    Churn          VARCHAR(10),
    Category       VARCHAR(10),
	inserttime	   DATETIME,
    runid          INT	     
);

USE gold_db;
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'customers_by_category' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.customers_by_category (
    Category       VARCHAR(10),
    total_customers INT,
    last_refresh_time   DATETIME
);

USE gold_db;
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'aggrevenue_by_contracts' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.aggrevenue_by_contracts (
    ContractType       VARCHAR(10),
    agg_revenues       FLOAT,
    last_refresh_time   DATETIME
);

USE gold_db;
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'aggrevenue_summary' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.aggrevenue_summary (
    ContractType       VARCHAR(10),
    InternetService    VARCHAR(10),
    agg_revenues       FLOAT,
    last_refresh_time   DATETIME
);

USE gold_db;
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'customer_demographics' AND schema_id = SCHEMA_ID('dbo'))
CREATE TABLE dbo.customer_demographics (
    CustomerID     INT,
    Age_band       VARCHAR(20),            
    Gender         VARCHAR(20),    
    TechSupport    VARCHAR(10),    
    Category       VARCHAR(10),
	inserttime	   DATETIME,
    last_refresh_time   DATETIME	     
);