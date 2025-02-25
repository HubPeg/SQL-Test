USE master;
-- Löschen der Helper Procedure
DROP PROCEDURE IF EXISTS usp_DropDatabase;

GO

-- "Helper" Stored Procedure um alle Datenbankverbindungen zu beenden und eine Datenbank zu löschen
CREATE PROCEDURE usp_DropDatabase
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @KillCommand NVARCHAR(MAX);
    DECLARE @Sql NVARCHAR(MAX);

    -- Initialize the command to kill active connections
    SET @KillCommand = '';

    -- Build the kill command for each active connection, excluding the current session
    SELECT @KillCommand = @KillCommand + 'KILL ' + CONVERT(NVARCHAR(10), session_id) + ';'
    FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID(@DatabaseName) AND session_id <> @@SPID;

    -- Execute the kill commands
    EXEC sp_executesql @KillCommand;

    -- Drop the database
    SET @Sql = N'DROP DATABASE IF EXISTS [' + @DatabaseName + ']';
    EXEC sp_executesql @Sql;
END

GO

/*
Anlegen der grundsätzlichen Datenbankobjekte (Datenbank, Schema)
*/

-- Ausführen der Helper Stored Procedure um die Datenbank zu löschen
EXEC usp_DropDatabase 'DWH'

GO

-- Anlegen der Datenbank "DWH"
CREATE DATABASE DWH COLLATE SQL_Latin1_General_CP1_CI_AS; -- Gleiche Collation wie die ERP Datenbank

GO

-- Auswählen der DB "DWH"
USE DWH;

GO

-- Anlegen von Staging Schema und Tables
CREATE SCHEMA Staging_HumanResources;

GO

CREATE SCHEMA HumanResources;

GO

CREATE SCHEMA Staging_Sales;

GO

CREATE SCHEMA sales;

GO

/*
Hier werden die Staging Tables erstellt die später mit den Daten der ERP Datenbank befüllt werden.
*/

-- Staging Table für Employee
CREATE TABLE Staging_HumanResources.Employee (
    BusinessEntityID INT PRIMARY KEY,
    NationalIDNumber NVARCHAR(15),
    LoginID NVARCHAR(256),
    JobTitle NVARCHAR(50),
    BirthDate DATE,
    Gender NCHAR(1),
    HireDate DATE,
    ModifiedDate DATETIME
);

-- Staging Table für Department
CREATE TABLE Staging_HumanResources.Department (
    DepartmentID INT PRIMARY KEY,
    Name VARCHAR(50),
    GroupName VARCHAR(50),
    ModifiedDate DATETIME
);

-- Staging Table für EmployeeDepartmentHistory
CREATE TABLE Staging_HumanResources.EmployeeDepartmentHistory (
    BusinessEntityID INT,
    DepartmentID INT,
    StartDate DATE,
    EndDate DATE,
    ModifiedDate DATETIME
);

-- Staging Table für Product
CREATE TABLE Staging_Sales.Product (
    ProductNumber VARCHAR(25) PRIMARY KEY,
    Name VARCHAR(50),
    MakeFlag BIT
);

-- Staging Table für Customer
CREATE TABLE Staging_Sales.Customer (
    CustomerID INT PRIMARY KEY,
    Title VARCHAR(10),
    FirstName VARCHAR(50),
    MiddleName VARCHAR(50),
    LastName VARCHAR(50)
);

-- Staging Table für Territory
CREATE TABLE Staging_Sales.Territory (
    TerritoryID INT PRIMARY KEY,
    Name VARCHAR(50),
    Country VARCHAR(10),
    [Group] VARCHAR(50)
);

-- Staging Table für Date
CREATE TABLE Staging_Sales.Date (
    OrderDate DATE,
    DueDate DATE,
    ShipDate DATE
);

-- Staging Table für Sales
CREATE TABLE Staging_Sales.Sales (
    TerritoryID INT,
    ProductNumber VARCHAR(25),
    CustomerID INT,
    OrderDate DATE,
    OrderQty INT,
    UnitPrice MONEY,
    LineTotal MONEY
);

/*
Ab hier ist der eigentliche Auftrag die Daten der ERP-Datenbank in Staging zu füllen. 
Das ist einfach ersichtlich mit folgenden Befehlen "INSERT INTO, SELECT & FROM"
*/

-- Daten kopieren vom ERP in das Staging HR
INSERT INTO Staging_HumanResources.Employee
SELECT 
    BusinessEntityID, 
    NationalIDNumber, 
    LoginID, 
    JobTitle, 
    BirthDate, 
    Gender, 
    HireDate, 
    ModifiedDate
FROM [ERP].HumanResources.Employee;

INSERT INTO Staging_HumanResources.Department
SELECT 
    DepartmentID, 
    Name, 
    GroupName,
    ModifiedDate
FROM [ERP].HumanResources.Department;

INSERT INTO Staging_HumanResources.EmployeeDepartmentHistory
SELECT 
    BusinessEntityID, 
    DepartmentID, 
    StartDate, 
    EndDate, 
    ModifiedDate
FROM [ERP].HumanResources.EmployeeDepartmentHistory;

GO

-- Daten kopieren vom ERP in das Staging PS
INSERT INTO Staging_Sales.Product (ProductNumber, Name, MakeFlag)
SELECT ProductNumber, Name, MakeFlag
FROM [ERP].Production.Product;

INSERT INTO Staging_Sales.Customer (CustomerID, Title, FirstName, MiddleName, LastName)
SELECT C.CustomerID, P.Title, P.FirstName, P.MiddleName, P.LastName
FROM ERP.Sales.Customer AS C
INNER JOIN ERP.Person.Person AS P ON C.PersonID = P.BusinessEntityID;

INSERT INTO Staging_Sales.Territory (TerritoryID, Name, Country, [Group])
SELECT TerritoryID, Name, CountryRegionCode, [Group]
FROM [ERP].Sales.SalesTerritory;

INSERT INTO Staging_Sales.Date (OrderDate, DueDate, ShipDate)
SELECT DISTINCT OrderDate, DueDate, ShipDate
FROM [ERP].Sales.SalesOrderHeader;

INSERT INTO Staging_Sales.Sales (TerritoryID, ProductNumber, CustomerID, OrderDate, OrderQty, UnitPrice, LineTotal)
SELECT 
    SOH.TerritoryID,
    P.ProductNumber,
    C.CustomerID,
    SOH.OrderDate,
    SOD.OrderQty,
    SOD.UnitPrice,
    SOD.LineTotal
FROM [ERP].Sales.SalesOrderHeader SOH
JOIN [ERP].Sales.SalesOrderDetail SOD ON SOH.SalesOrderID = SOD.SalesOrderID
JOIN [ERP].Production.Product P ON SOD.ProductID = P.ProductID
JOIN [ERP].Sales.Customer C ON SOH.CustomerID = C.CustomerID;

GO

/*
Nun müssen wir zuerst die Dimensionen und und Faktenrtabellen anlegen, bevor wir sie mir den Informationen befüllen die wir aus dem ERP ins Staging geladen haben.
*/

-- Anlegen der Dimension "Department"
CREATE TABLE HumanResources.DimDepartment (
    DepartmentID INT PRIMARY KEY,
    Name VARCHAR(50),
    GroupName VARCHAR(50)
);

GO

-- Anlegen der Dimension "Employee" with ModifiedDate
CREATE TABLE HumanResources.DimEmployee (
    BusinessEntityID INT PRIMARY KEY,
    JobTitle VARCHAR(50),
    BirthDate DATE,
    Gender CHAR(1) CHECK (Gender IN ('M', 'F', 'U')),
    ModifiedDate DATETIME
);

GO

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

-- Anlegen der Faktentabelle mit berechneten Spalten ohne Subqueries
CREATE TABLE HumanResources.FactEmployeeDepartment (
    FactID INT PRIMARY KEY IDENTITY(1,1),
    DepartmentID INT NOT NULL,
    BusinessEntityID INT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    LatestModifiedDate DATETIME NOT NULL,
    BirthDate DATE NULL, -- BirthDate direkt in der Tabelle
    -- Berechnete Spalte für gearbeitete Jahre
    YearsWorked AS CAST(DATEDIFF(DAY, StartDate, ISNULL(EndDate, LatestModifiedDate)) / 365.0 AS DECIMAL(5,1)) PERSISTED,
    -- Berechnete Spalte für Alter
    Age AS DATEDIFF(YEAR, BirthDate, LatestModifiedDate) PERSISTED,
    FOREIGN KEY (BusinessEntityID) REFERENCES HumanResources.DimEmployee(BusinessEntityID),
    FOREIGN KEY (DepartmentID) REFERENCES HumanResources.DimDepartment(DepartmentID)
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
Mit diesem Schritt werden nun die Dimensionen & Faktentabellen befüllt.
*/

-- Beladen für DimEmployee
INSERT INTO HumanResources.DimEmployee (
    BusinessEntityID, 
    JobTitle, 
    BirthDate, 
    Gender, 
    ModifiedDate
)
SELECT 
    BusinessEntityID, 
    JobTitle, 
    BirthDate, 
    Gender, 
    ModifiedDate
FROM Staging_HumanResources.Employee;

-- Beladen für DimDepartment
INSERT INTO HumanResources.DimDepartment (
    DepartmentID, 
    Name, 
    GroupName
)
SELECT 
    DepartmentID, 
    Name, 
    GroupName
FROM Staging_HumanResources.Department;

-- Beladen für FactEmployeeDepartment
INSERT INTO HumanResources.FactEmployeeDepartment (
    DepartmentID, 
    BusinessEntityID, 
    StartDate, 
    EndDate, 
    LatestModifiedDate,
    BirthDate
)
SELECT 
    ed.DepartmentID,
    ed.BusinessEntityID,
    ed.StartDate,
    ISNULL(ed.EndDate, '2014-12-26') AS EndDate, -- Replace NULL with fixed date
    ed.ModifiedDate AS LatestModifiedDate,
    e.BirthDate
FROM Staging_HumanResources.EmployeeDepartmentHistory ed
JOIN Staging_HumanResources.Employee e 
    ON ed.BusinessEntityID = e.BusinessEntityID
JOIN Staging_HumanResources.Department d 
    ON ed.DepartmentID = d.DepartmentID;

GO

-- Beladen für DimProduct
INSERT INTO sales.DimProduct (ProductNumber, Name, MakeFlag)
SELECT DISTINCT ProductNumber, Name, MakeFlag
FROM Staging_Sales.Product;

-- Beladen für DimCustomer
INSERT INTO sales.DimCustomer (CustomerID, Title, FirstName, MiddleName, LastName)
SELECT DISTINCT CustomerID, Title, FirstName, MiddleName, LastName
FROM Staging_Sales.Customer;

-- Beladen für DimTerritory
INSERT INTO sales.DimTerritory (TerritoryID, Name, Country, [Group])
SELECT DISTINCT TerritoryID, Name, Country, [Group]
FROM Staging_Sales.Territory;

-- Beladen für DimDate
INSERT INTO sales.DimDate (OrderDate, DueDate, ShipDate)
SELECT DISTINCT OrderDate, DueDate, ShipDate
FROM Staging_Sales.Date;

-- Beladen für FactSales
INSERT INTO sales.FactSales (TerritoryID, ProductNumber, CustomerID, DateID, OrderQty, UnitPrice, LineTotal)
SELECT 
    s.TerritoryID,
    s.ProductNumber,
    s.CustomerID,
    dd.DateID,
    SUM(s.OrderQty) AS TotalOrderQty,
    AVG(s.UnitPrice) AS AvgUnitPrice,
    SUM(s.LineTotal) AS TotalLineTotal
FROM Staging_Sales.Sales s
JOIN sales.DimDate dd ON s.OrderDate = dd.OrderDate
GROUP BY s.TerritoryID, s.ProductNumber, s.CustomerID, dd.DateID;

GO

-- Erstellung eines SQL Views (mit den berechneten Daten), damit das Star-Schema rasch abgefragt werden kann
CREATE OR ALTER VIEW HumanResources.HumanResourcesView AS
SELECT 
    fact.FactID,
    fact.DepartmentID,
    dept.Name AS DepartmentName,
    dept.GroupName,
    fact.BusinessEntityID,
    emp.JobTitle,
    emp.BirthDate,
    emp.Gender,
    fact.StartDate,
    fact.EndDate,
    fact.LatestModifiedDate, -- Include LatestModifiedDate
    fact.YearsWorked, -- Bereits berechnete Spalte aus der Faktentabelle
    fact.Age -- Bereits berechnete Spalte aus der Faktentabelle
FROM HumanResources.FactEmployeeDepartment AS fact
LEFT JOIN HumanResources.DimDepartment AS dept ON fact.DepartmentID = dept.DepartmentID
LEFT JOIN HumanResources.DimEmployee AS emp ON fact.BusinessEntityID = emp.BusinessEntityID;

GO

CREATE OR ALTER VIEW sales.Sales AS
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

/*
Abfrage der Daten aus dem View
*/
SELECT 
    FactID,
    BusinessEntityID,
    JobTitle,
    BirthDate,
    Gender,
    DepartmentID,
    DepartmentName,
    GroupName,
    StartDate,
    EndDate, -- Include EndDate
    YearsWorked, -- Include YearsWorked
    Age, -- Include Age
    LatestModifiedDate -- Include LatestModifiedDate
FROM HumanResources.HumanResourcesView;

GO

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
