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

-- Anlegen eines Schemas für das neue Star-Schema für die Strukturierung
CREATE SCHEMA HumanResources;

GO

/*
Anlegen der Dimensions- und Faktentabellen
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
Anlegen der Stored Procedures als einfache "ETL-Pipelines".
Hiermit werden die Tabellen anschliessend befüllt aus dem ERP.
*/

-- Stored Procedure für die Beladung der Dimension "Department" aus dem ERP
CREATE PROCEDURE LoadDimDepartment AS
BEGIN
    INSERT INTO HumanResources.DimDepartment (DepartmentID, Name, GroupName)
    SELECT DepartmentID, Name, GroupName
    FROM [ERP].HumanResources.Department
    WHERE DepartmentID NOT IN (SELECT DepartmentID FROM HumanResources.DimDepartment);
END;

GO

-- Stored Procedure für die Beladung der Dimension "Employee" mit ModifiedDate aus dem ERP
CREATE PROCEDURE LoadDimEmployee AS
BEGIN
    INSERT INTO HumanResources.DimEmployee (BusinessEntityID, JobTitle, BirthDate, Gender, ModifiedDate)
    SELECT BusinessEntityID, JobTitle, BirthDate, Gender, ModifiedDate
    FROM [ERP].HumanResources.Employee
    WHERE BusinessEntityID NOT IN (SELECT BusinessEntityID FROM HumanResources.DimEmployee);
END;

GO

-- Stored Procedure für die Beladung der Faktentabelle aus dem ERP
CREATE OR ALTER PROCEDURE LoadFactEmployeeDepartment AS
BEGIN
    BEGIN TRANSACTION;

    BEGIN TRY
        INSERT INTO HumanResources.FactEmployeeDepartment (
            DepartmentID, 
            BusinessEntityID, 
            StartDate, 
            EndDate, 
            LatestModifiedDate,
            BirthDate -- Füge BirthDate hinzu
        )
        SELECT 
            ed.DepartmentID,
            e.BusinessEntityID,
            ed.StartDate,
            ISNULL(ed.EndDate, (SELECT MAX(ModifiedDate) FROM [ERP].HumanResources.Employee)) AS EndDate,
            (SELECT MAX(ModifiedDate) FROM [ERP].HumanResources.Employee) AS LatestModifiedDate,
            e.BirthDate -- Lade BirthDate aus der Dimensionstabelle
        FROM 
            [ERP].HumanResources.EmployeeDepartmentHistory ed
        JOIN 
            [ERP].HumanResources.Employee e 
            ON ed.BusinessEntityID = e.BusinessEntityID
        JOIN 
            [ERP].HumanResources.Department d 
            ON ed.DepartmentID = d.DepartmentID
        WHERE NOT EXISTS (
            SELECT 1 
            FROM HumanResources.FactEmployeeDepartment f
            WHERE f.DepartmentID = ed.DepartmentID
              AND f.BusinessEntityID = ed.BusinessEntityID
              AND f.StartDate = ed.StartDate
              AND (f.EndDate = ed.EndDate OR (f.EndDate IS NULL AND ed.EndDate IS NULL))
        );

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO

/*
Beladung des Star-Schemas mit den angelegten Stored Procedures
*/

-- Ausführen aller Stored Procedures um das Star-Schema zu befüllen
EXEC LoadDimEmployee;
EXEC LoadDimDepartment;
EXEC LoadFactEmployeeDepartment;

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

-- Selektieren aller relevanten Spalten des gesamten "HumanResourcesView"
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
FROM HumanResources.HumanResourcesView AS hr;

GO