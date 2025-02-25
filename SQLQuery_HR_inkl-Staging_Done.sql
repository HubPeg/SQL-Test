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

-- Step 1: Create Staging Schema and Tables
CREATE SCHEMA Staging_HumanResources;

GO

CREATE SCHEMA HumanResources;

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

/*
Ab hier ist der eigentliche Auftrag die Daten der ERP-Datenbank in Staging zu füllen. 
Das ist einfach ersichtlich mit folgenden Befehlen "INSERT INTO, SELECT & FROM"
*/

-- Step 2: Copy Data from ERP to Staging
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

/*
Nun müssen wir zuerst die Dimensionen und und Faktenrtabellen anlegen, bevor wir sie mir den Informationen befüllen die wir aus dem ERP ins Staging geladen haben.

WICHTIG: Schema für "HumanResources" muss zu beginn auch erstellt werden, Staging_HumanResources reicht nicht.
*/

-- Anlegen Dimension "Department"
CREATE TABLE HumanResources.DimDepartment (
    DepartmentID INT PRIMARY KEY,
    Name VARCHAR(50),
    GroupName VARCHAR(50)
);

GO

-- Dimension "Employee" with ModifiedDate
CREATE TABLE HumanResources.DimEmployee (
    BusinessEntityID INT PRIMARY KEY,
    JobTitle VARCHAR(50),
    BirthDate DATE,
    Gender CHAR(1) CHECK (Gender IN ('M', 'F', 'U')),
    ModifiedDate DATETIME
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

/*
Mit diesem Schritt werden nun die Dimensionen & Faktentabellen befüllt.
*/

-- Load DimEmployee
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

-- Load DimDepartment
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

-- Load FactEmployeeDepartment
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

-- Erstellung eines SQL Views mit bereits berechneten Daten
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