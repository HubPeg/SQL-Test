USE master;
-- Drop Helper Procedure if it exists
DROP PROCEDURE IF EXISTS usp_DropDatabase;

GO

-- Create Helper Procedure to Drop Database
CREATE PROCEDURE usp_DropDatabase
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @KillCommand NVARCHAR(MAX);
    DECLARE @Sql NVARCHAR(MAX);

    -- Build the command to kill active connections
    SELECT @KillCommand = @KillCommand + 'KILL ' + CONVERT(NVARCHAR(10), session_id) + ';'
    FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID(@DatabaseName) AND session_id <> @@SPID;

    -- Execute the kill commands
    EXEC sp_executesql @KillCommand;

    -- Drop the database
    SET @Sql = N'DROP DATABASE IF EXISTS [' + @DatabaseName + ']';
    EXEC sp_executesql @Sql;
END;

GO

-- Execute the Helper Procedure to Drop the Database
EXEC usp_DropDatabase 'DWH2';

GO

-- Create the Database
CREATE DATABASE DWH2 COLLATE SQL_Latin1_General_CP1_CI_AS;

GO

-- Select the Database
USE DWH2;

GO

-- Create Schema
CREATE SCHEMA sales;

GO

/*
Anlegen der Dimensions- und Faktentabellen
*/

-- Anlegen der Dimension "Product"
CREATE TABLE sales.DimProduct (
    ProductNumber VARCHAR(25) PRIMARY KEY,
    Name VARCHAR(50),
    MakeFlag BIT
);

GO

-- Anlegen der Dimension "Customer"
CREATE TABLE sales.DimCustomer (
    CustomerID INT PRIMARY KEY,
    Title VARCHAR(10),
    FirstName VARCHAR(50),
    MiddleName VARCHAR(50),
    LastName VARCHAR(50)
);

GO

-- Anlegen der Dimension "Territory"
CREATE TABLE sales.DimTerritory (
    TerritoryID INT PRIMARY KEY,
    Name VARCHAR(50),
    Country VARCHAR(10),
    [Group] VARCHAR(50)
);

GO

-- Anlegen der Dimension "Date"
CREATE TABLE sales.DimDate (
    DateID INT IDENTITY(1,1) PRIMARY KEY,
    OrderDate DATE,
    DueDate DATE,
    ShipDate DATE
);

GO

-- Anlegen der Faktentabelle inkl. Foreign Keys zu den Dimensionen
CREATE TABLE sales.FactSales (
    TerritoryID INT,
    ProductNumber VARCHAR(25),
    CustomerID INT,
    DateID INT,
    OrderQty INT,
    UnitPrice MONEY,
    LineTotal MONEY,
    PRIMARY KEY (TerritoryID, ProductNumber, CustomerID, DateID),
    FOREIGN KEY (ProductNumber) REFERENCES sales.DimProduct(ProductNumber),
    FOREIGN KEY (CustomerID) REFERENCES sales.DimCustomer(CustomerID),
    FOREIGN KEY (TerritoryID) REFERENCES sales.DimTerritory(TerritoryID),
    FOREIGN KEY (DateID) REFERENCES sales.DimDate(DateID)
);

GO

/*
Anlegen der Stored Procedures als einfache "ETL-Pipelines".
Hiermit werden die Tabellen anschliessend befüllt aus dem ERP.
*/

-- Stored Procedure für die Beladung der Dimension "Product" aus dem ERP
CREATE OR ALTER PROCEDURE LoadDimProduct AS
BEGIN
    INSERT INTO sales.DimProduct (ProductNumber, Name, MakeFlag)
    SELECT ProductNumber, Name, MakeFlag
    FROM [ERP].Production.Product
    WHERE ProductNumber NOT IN (SELECT ProductNumber FROM sales.DimProduct);
END;

GO

-- Stored Procedure für die Beladung der Dimension "Customer" aus dem ERP
CREATE OR ALTER PROCEDURE LoadDimCustomer AS
BEGIN
    INSERT INTO sales.DimCustomer (CustomerID, Title, FirstName, MiddleName, LastName)
    SELECT C.CustomerID, P.Title, P.FirstName, P.MiddleName, P.LastName
    FROM ERP.Sales.Customer AS C
    INNER JOIN ERP.Person.Person AS P ON C.PersonID = P.BusinessEntityID
    WHERE C.CustomerID NOT IN (SELECT CustomerID FROM sales.DimCustomer);
END;

GO

-- Stored Procedure für die Beladung der Dimension "Territory" aus dem ERP
CREATE OR ALTER PROCEDURE LoadDimTerritory AS
BEGIN
    INSERT INTO sales.DimTerritory (TerritoryID, Name, Country, [Group])
    SELECT T.TerritoryID, T.Name, T.CountryRegionCode, T.[Group]
    FROM [ERP].Sales.SalesTerritory T
    WHERE T.TerritoryID NOT IN (SELECT TerritoryID FROM sales.DimTerritory);
END;

GO

-- Stored Procedure für die Beladung der Dimension "Date" aus dem ERP
CREATE OR ALTER PROCEDURE LoadDimDate AS
BEGIN
    INSERT INTO sales.DimDate (OrderDate, DueDate, ShipDate)
    SELECT DISTINCT SOH.OrderDate, SOH.DueDate, SOH.ShipDate
    FROM [ERP].Sales.SalesOrderHeader SOH
    WHERE NOT EXISTS (
        SELECT 1
        FROM sales.DimDate DD
        WHERE SOH.OrderDate = DD.OrderDate
          AND SOH.DueDate = DD.DueDate
          AND SOH.ShipDate = DD.ShipDate
    );
END;

GO

-- Stored Procedure für die Beladung der Faktentabelle aus dem ERP
CREATE OR ALTER PROCEDURE LoadFactSales AS
BEGIN
    INSERT INTO sales.FactSales (TerritoryID, ProductNumber, CustomerID, DateID, OrderQty, UnitPrice, LineTotal)
    SELECT 
        SOH.TerritoryID,  
        P.ProductNumber, 
        C.CustomerID,  
        DD.DateID,
        SUM(SOD.OrderQty) AS TotalOrderQty, -- Aggregate duplicate rows
        AVG(SOD.UnitPrice) AS AvgUnitPrice, -- Average unit price if duplicates exist
        SUM(SOD.LineTotal) AS TotalLineTotal -- Aggregate total line value
    FROM 
        [ERP].Sales.SalesOrderHeader SOH
        JOIN [ERP].Sales.SalesOrderDetail SOD ON SOH.SalesOrderID = SOD.SalesOrderID
        JOIN [ERP].Production.Product P ON SOD.ProductID = P.ProductID
        JOIN [ERP].Sales.Customer C ON SOH.CustomerID = C.CustomerID
        JOIN sales.DimDate DD ON SOH.OrderDate = DD.OrderDate
    WHERE NOT EXISTS (
        SELECT 1
        FROM sales.FactSales FS
        WHERE FS.TerritoryID = SOH.TerritoryID
        AND FS.ProductNumber = P.ProductNumber
        AND FS.CustomerID = C.CustomerID
        AND FS.DateID = DD.DateID
    )
    GROUP BY 
        SOH.TerritoryID, 
        P.ProductNumber, 
        C.CustomerID,
        DD.DateID;
END;

GO

/* 
Beladung des Star-Schemas mit den angelegten Stored Procedures
*/

-- Execute all Stored Procedures to load the Star Schema
EXEC LoadDimProduct;
EXEC LoadDimCustomer;
EXEC LoadDimTerritory;
EXEC LoadDimDate;
EXEC LoadFactSales;

GO

-- Erstellung eines SQL Views, damit das Star-Schema rasch abgefragt werden kann
CREATE OR ALTER VIEW sales.Sales AS

-- Definition eines "Star-Joins" (Modename, wenn zu einer Faktentabelle alle Dimensionstabellen gejoint werden)
SELECT  
        fact.TerritoryID,
        dim_ter.Name AS TerritoryName,
        dim_ter.Country,
        dim_ter.[Group],
        dim_p.Name AS ProductName, 
        dim_p.ProductNumber,
        dim_c.CustomerID,
        dim_c.FirstName,
        dim_c.LastName,
        dim_date.OrderDate,
        dim_date.DueDate,
        dim_date.ShipDate,
        fact.OrderQty AS TotalOrderQty,
        fact.LineTotal AS TotalSales, -- Aggregated sales for analysis
        fact.UnitPrice
FROM sales.FactSales AS fact
LEFT JOIN sales.DimCustomer AS dim_c ON fact.CustomerID = dim_c.CustomerID
LEFT JOIN sales.DimProduct AS dim_p ON fact.ProductNumber = dim_p.ProductNumber
LEFT JOIN sales.DimTerritory AS dim_ter ON fact.TerritoryID = dim_ter.TerritoryID
LEFT JOIN sales.DimDate AS dim_date ON fact.DateID = dim_date.DateID;

GO

-- Selektieren aller relevanten Spalten des gesamten "Sales"
SELECT  
        TerritoryID,
        TerritoryName,
        Country,
        [Group],
        ProductNumber,
        ProductName,
        CustomerID,
        FirstName,
        LastName,
        OrderDate,
        DueDate,
        ShipDate,
        TotalOrderQty,
        TotalSales,
        UnitPrice
FROM sales.Sales AS sales;

GO