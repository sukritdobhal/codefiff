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
IF EXISTS (SELECT * from sys.sql_logins WHERE name = 'g3-MetasysReportServer')
  AND NOT EXISTS (SELECT * from sys.sysusers WHERE name = 'g3-MetasysReportServer')
    CREATE USER [g3-MetasysReportServer] FOR LOGIN [g3-MetasysReportServer] WITH DEFAULT_SCHEMA=[dbo]

ALTER USER [g3-MetasysReportServer] WITH LOGIN = [g3-MetasysReportServer]	
