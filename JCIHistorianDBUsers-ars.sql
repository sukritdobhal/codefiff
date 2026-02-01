 -- ============================================================================================================================
-- Copyright 2026 Johnson Controls Tyco IP Holdings LLP. All rights reserved.
-- This software constitutes the confidential and proprietary information and
-- intellectual property of Johnson Controls.
-- ============================================================================================================================

 --JCIHistorianDB/DDLScripts/JCIHistorianDBUsers-ars.sql
 DECLARE @sql nvarchar(max)
DECLARE @Schema nvarchar(128)

while exists(select 1 from information_schema.schemata where schema_owner = 'g3-MetasysReportServer')
BEGIN
    select @Schema= SCHEMA_NAME from information_schema.schemata where schema_owner = 'g3-MetasysReportServer'
    set @sql =    'ALTER AUTHORIZATION ON SCHEMA::[' + @Schema + '] TO [dbo]'
    exec( @sql)
END
 
 IF EXISTS (SELECT * from sys.sysusers WHERE name = 'g3-MetasysReportServer')
     drop user [g3-MetasysReportServer]
GO

/****** Object:  User [g3-MetasysReportServer]    Script Date: 03/23/2015 08:41:28 ******/

IF EXISTS (SELECT * FROM sys.sysusers WHERE name = 'g3-MetasysReportServer')
    DROP USER [g3-MetasysReportServer]
GO

IF (SELECT SERVERPROPERTY('babelfishversion')) IS NULL 
BEGIN
    IF EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'g3-MetasysReportServer')
       AND NOT EXISTS (SELECT * FROM sys.sysusers WHERE name = 'g3-MetasysReportServer')
        CREATE USER [g3-MetasysReportServer] FOR LOGIN [g3-MetasysReportServer] WITH DEFAULT_SCHEMA = [dbo]

    
    ALTER USER [g3-MetasysReportServer] WITH LOGIN = [g3-MetasysReportServer]
END
ELSE 
BEGIN
    IF EXISTS (SELECT * FROM sys.server_principals WHERE name = 'g3-MetasysReportServer')
       AND NOT EXISTS (SELECT * FROM sys.sysusers WHERE name = 'g3-MetasysReportServer')
        CREATE USER [g3-MetasysReportServer] FOR LOGIN [g3-MetasysReportServer] WITH DEFAULT_SCHEMA = [dbo]

    
    ALTER USER [g3-MetasysReportServer] WITH LOGIN = [g3-MetasysReportServer]
END




