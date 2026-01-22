-- ==========================================================================================================================
--  Copyright 2015 Johnson Controls, Inc.
--    Use or copying of all or any part of the document, except as permitted
--    by the License Agreement is prohibited.
-- ==========================================================================================================================
--  NAME:           HistorianSP.sql
--
--  DESCRIPTION:    Main SP source file.
--
--                  Script is re-runnable.
-- ==========================================================================================================================
--  Revision History
-- ==========================================================================================================================
--  Revison Date        Name                Description (Prob #):
--  ------- -------     -----------------   ------------------------------------
--  8.0    2015Mar26   cschust              Historian is now in g3_ps
--  8.0    2015Jun18   chagdoh              Change real to float
-- ==========================================================================================================================


/****** Object:  StoredProcedure [dbo].[spBulkInsertOtherDigitalPoints]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spBulkInsertOtherDigitalPoints]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spBulkInsertOtherDigitalPoints]
GO

CREATE PROCEDURE [dbo].[spBulkInsertOtherDigitalPoints]
  @filename nvarchar(400)
    AS
      declare @cmd nvarchar(500)

      set @cmd = N'BULK INSERT tblOtherValueDigital FROM "' + @filename
      set @cmd = @cmd + N'"' + char(13) + ' WITH ( FIELDTERMINATOR=",", ROWTERMINATOR="\n", CHECK_CONSTRAINTS)'
      exec(@cmd)
GO
/****** Object:  StoredProcedure [dbo].[spBulkInsertOtherAnalogPoints]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spBulkInsertOtherAnalogPoints]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spBulkInsertOtherAnalogPoints]
GO

CREATE PROCEDURE [dbo].[spBulkInsertOtherAnalogPoints]
  @filename nvarchar(400)
    AS
      declare @cmd nvarchar(500)

      set @cmd = N'BULK INSERT tblOtherValueFloat FROM "' + @filename
      set @cmd = @cmd + N'"' + char(13) + ' WITH ( FIELDTERMINATOR=",", ROWTERMINATOR="\n", CHECK_CONSTRAINTS)'
      exec(@cmd)
GO
/****** Object:  StoredProcedure [dbo].[spu_GetAggregateSamples]    Script Date: 03/23/2015 13:36:46 ******/

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spu_GetAggregateSamples]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_GetAggregateSamples]
GO

CREATE PROCEDURE spu_GetAggregateSamples
(
    @tableName AS NVARCHAR(50),
    @otherTableName AS NVARCHAR(50),
    @id AS BIGINT,
    @start AS DateTime,
    @end As DateTime,
    @aggregateFunction AS NVARCHAR(50),
    @aggregateInterval AS BIGINT,
    @count AS INT,
    @mostRecentFirst AS Bit
)
AS
   SET NOCOUNT ON

   DECLARE @sqlAggFunc AS NVARCHAR(100)

   SELECT @sqlAggFunc =
       CASE @aggregateFunction
           WHEN 'Average' THEN 'AVG(ActualValue)'
           WHEN 'Minimum' THEN 'MIN(ActualValue)'
           WHEN 'Maximum' THEN 'MAX(ActualValue)'
           WHEN 'Standard Deviation' THEN 'STDEV(ActualValue)'
           WHEN 'Range' THEN 'MAX(ActualValue) - MIN(ActualValue)'
           WHEN 'Sum' THEN 'SUM(ActualValue)'
           WHEN 'Variance' THEN 'VAR(ActualValue)'
           ELSE ''
       END

   DECLARE @sql AS NVARCHAR(4000)

   CREATE TABLE #tmpTB
   (
       [UTCDateTime] [datetime] NOT NULL ,
       [ActualValue] [float] NOT NULL,
       [Status] [int] NULL
   )

   SET @sql = 'SELECT UTCDateTime, ActualValue AS ActualValue, 0 AS Status '
   SET @sql = @sql + 'FROM ' + @tableName
   SET @sql = @sql + ' WHERE PointSliceID = (SELECT PointSliceID FROM '
   SET @sql = @sql + ' tblPointSlice WHERE PointID = '
   SET @sql = @sql + + CAST(@id AS NVARCHAR) + ' AND IsRawData = 1)'
   SET @sql = @sql + ' AND UTCDateTime >= ''' + CAST(@start AS nvarchar) + ''''
   SET @sql = @sql + ' AND UTCDateTime <= ''' + CAST(@end AS nvarchar) + ''''

   INSERT INTO #tmpTB([UTCDateTime], [ActualValue], [Status])
       EXEC sp_executesql @sql

   -- Get from the other value table
   SET @sql = 'SELECT UTCDateTime, OtherValue AS ActualValue, Status ' 
   SET @sql = @sql + 'FROM ' + @otherTableName
   SET @sql = @sql + ' WHERE PointSliceID = (SELECT PointSliceID FROM '
   SET @sql = @sql + 'tblPointSlice WHERE PointID = ' 
   SET @sql = @sql + CAST(@id AS NVARCHAR) + ' AND IsRawData = 1)'
   SET @sql = @sql + ' AND UTCDateTime >= ''' + CAST(@start AS nvarchar) + ''''
   SET @sql = @sql + ' AND UTCDateTime <= ''' + CAST(@end AS nvarchar) + ''''
   SET @sql = @sql + ' AND ValueCategoryID = 1'

   INSERT INTO #tmpTB([UTCDateTime], [ActualValue], [Status])
      EXEC sp_executesql @sql

   -- Get the data from the temp table
   SET ROWCOUNT @count

   SET @sql = ''
   SET @sql = 'SELECT min(UTCDateTime) as t, ' 
   SET @sql = @sql + @sqlAggFunc + ' as value, min(Status) as status '
   SET @sql = @sql + 'FROM #tmpTB '
   SET @sql = @sql + ' GROUP BY datediff(minute, ''' + CAST(@start AS nvarchar) + ''''
   SET @sql = @sql + ', UTCDateTime)/' + CAST(@aggregateInterval AS nvarchar)

   if (@mostRecentFirst = 1) SET @sql = @sql + ', Status ORDER BY MIN(UTCDateTime) DESC'
   else  SET @sql = @sql + ', Status ORDER BY MIN(UTCDateTime) ASC'

   EXEC sp_executesql @sql

   SET ROWCOUNT 0

   DROP TABLE #tmpTB
GO
/****** Object:  StoredProcedure [dbo].[spu_GetTrendSamples]    Script Date: 03/23/2015 13:36:46 ******/

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spu_GetTrendSamples]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_GetTrendSamples]
GO

CREATE PROCEDURE spu_GetTrendSamples
(
    @tableName AS NVARCHAR(50),
    @otherTableName AS NVARCHAR(50),
    @id AS BIGINT,
    @start AS DateTime,
    @end As DateTime,
    @count AS INT,
    @mostRecentFirst AS Bit
)
AS
   SET NOCOUNT ON

   SET ROWCOUNT @count

   DECLARE @sql AS NVARCHAR(4000)

   SET @sql = 'SELECT TOP ' + CAST(@count AS NVARCHAR)
   SET @sql = @sql + ' UTCDateTime as t, ActualValue as value, 0 as status '
   SET @sql = @sql + 'FROM ' + @tableName
   SET @sql = @sql + ' WHERE PointSliceID = (SELECT PointSliceID FROM '
   SET @sql = @sql + ' tblPointSlice WHERE PointID = '
   SET @sql = @sql + + CAST(@id AS NVARCHAR) + ' AND IsRawData = 1)'
   SET @sql = @sql + ' AND UTCDateTime >= ''' + CONVERT(nvarchar, @start, 126) + ''''
   SET @sql = @sql + ' AND UTCDateTime <= ''' + CONVERT(nvarchar, @end, 126) + ''''

   SET @sql = @sql + ' UNION '

   SET @sql = @sql + 'SELECT TOP ' + CAST(@count AS NVARCHAR)
   SET @sql = @sql + ' UTCDateTime as t, OtherValue as value, Status as status '
   SET @sql = @sql + 'FROM ' + @otherTableName
   SET @sql = @sql + ' WHERE PointSliceID = (SELECT PointSliceID FROM '
   SET @sql = @sql + 'tblPointSlice WHERE PointID = ' 
   SET @sql = @sql + CAST(@id AS NVARCHAR) + ' AND IsRawData = 1)'
   SET @sql = @sql + ' AND UTCDateTime >= ''' + CONVERT(nvarchar, @start, 126) + ''''
   SET @sql = @sql + ' AND UTCDateTime <= ''' + CONVERT(nvarchar, @end, 126) + ''''
   SET @sql = @sql + ' AND ValueCategoryID = 1'

   if (@mostRecentFirst = 1) SET @sql = @sql + ' ORDER BY UTCDateTime DESC'
   else  SET @sql = @sql + ' ORDER BY UTCDateTime ASC'
   
   -- print @sql 
 
   EXEC sp_executesql @SQL

   SET ROWCOUNT 0

GO
/****** Object:  StoredProcedure [dbo].[spu_TruncatePointSliceValueTemp]    Script Date: 03/23/2015 13:36:46 ******/
-- spu_TruncatePointSliceValueTemp: Drop stored procedure if it already exists
IF  EXISTS  ( select  * 
              from    dbo.sysobjects 
              where   id = object_id(N'[dbo].[spu_TruncatePointSliceValueTemp]') and OBJECTPROPERTY(id, N'IsProcedure') = 1
            )
DROP PROCEDURE [dbo].[spu_TruncatePointSliceValueTemp]
GO
-- ===============================================================================================================
--  Copyright 2008 Johnson Controls, Inc.
--    Use or copying of all or any part of the document, except as permitted
--    by the License Agreement is prohibited.
-- ===============================================================================================================
--  NAME:      spu_TruncatePointSliceValueTemp
--  DESCRIPTION:  Truncate the import staging table, tblPointSliceValueTemp.
--
--                To be called by application prior to import to ensure a clean landing zone.
--
-- ===============================================================================================================
--  PARAMETERS:
--
--  OUTPUTS:
--
--  RESULT SET:
--
--  RETURN CODES:
--
-- ====================================================================
--  TEST HARNESS:
-- ====================================================================
--	exec spu_TruncatePointSliceValueTemp
-- ===============================================================================================================
--    Revision History
-- ---------------------------------------------------------------------------------------------------------------
--  Revison Date        Name            Description (Prob #):
--  ------  ---------   --------------- --------------------------------------------------------------------------
--  5.0.0   2008Oct02   Kyle Stittleburg Original
--
-- ===============================================================================================================
GO
CREATE PROCEDURE [dbo].[spu_TruncatePointSliceValueTemp] 
  -- Add the parameters for the stored procedure here

AS
BEGIN
  BEGIN TRY

    SET NOCOUNT ON

    TRUNCATE TABLE tblPointSliceValueTemp;

  END TRY
------------------------------------------------------------------------------------------------------------------
--                      Exception Handling
------------------------------------------------------------------------------------------------------------------

  BEGIN CATCH

    --
    -- Rollback transaction if still active
    --
    IF  (XACT_STATE() <> 0)
      ROLLBACK TRANSACTION

    --
    -- Error raised
    --
    DECLARE             @ErrorMessage   NVARCHAR(max)
                      , @ErrorSeverity  INT
                      , @ErrorState     INT
                      , @ErrorProc      nvarchar(126)
                      , @ErrorLine      int
                      , @ErrorNumber    int
                      , @ErrorUserDef   int

    Set                 @ErrorUserDef = 50000             -- Error number if error has already be reraised
  
    -- get error info
    SELECT              @ErrorMessage   = Coalesce(ERROR_MESSAGE()  ,'NO ERROR MESSAGE')  
                      , @ErrorSeverity  = Coalesce(ERROR_SEVERITY() ,0)
                      , @ErrorState     = Coalesce(ERROR_STATE()    ,1)
                      , @ErrorProc      = Coalesce(ERROR_PROCEDURE(),'NO PROC NAME')
                      , @ErrorLine      = Coalesce(ERROR_LINE()     ,0) 
                      , @ErrorNumber    = Coalesce(ERROR_NUMBER()   ,@ErrorUserDef)

    If                  (@ErrorState = 0)   -- correct error state to allow reraise
      Set               @ErrorState = 1

    -- has error been reraised?
    If                  (@ErrorNumber = @ErrorUserDef)
    Begin 
      -- Error has been reraised already, continue to reraise              
      RAISERROR 
        (               @ErrorMessage           
                      , @ErrorSeverity 
                      , @ErrorState 
         )
    End Else Begin
      -- Error being caught for the first time
      --  add all error information to error message and reraise
      Set               @ErrorMessage
                      = N'ErrNo: ' + Cast(@ErrorNumber as nvarchar(15))  + N', '
                      + N'Proc: '  + @ErrorProc                          + N', '
                      + N'Line: '  + Cast(@ErrorLine   as nvarchar(15))    + N', '
                      + N'Error: ' + @ErrorMessage                       
      RAISERROR 
        (               @ErrorMessage           
                      , @ErrorSeverity 
                      , @ErrorState 
         )
    End
    --  return error number
    Return @ErrorNumber
  END CATCH
END
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spu_PurgeRollupData]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_PurgeRollupData]
GO

-- ====================================================================================================================
--	Copyright 2009 Johnson Controls, Inc.
--		Use or copying of all or any part of the document, except as permitted
--		by the License Agreement is prohibited.
-- ====================================================================================================================
--	File NAME:		spu_PurgeRollupData.sql
--	DESCRIPTION:	purge select point rollup data within the Trend database
-- ====================================================================================================================
--	Inputs
-- ====================================================================================================================
--	@PointList                   xml	            List of Historian Points to remove rollup data
--
--                      <root>
--                          <row point="282" />
--                          <row point="283" />
--                      </root>
-- ====================================================================================================================
--	Outputs
-- ====================================================================================================================
--    NONE
--
-- ====================================================================================================================
--		Revision History
-- ====================================================================================================================
--	Revison	Date	    Name			Description (Prob #):
--	-------	---------	--------------  -------------------------------------------------------------------------------
--	5.0.0	  Original  Toby Schuster
-- ====================================================================================================================
GO


CREATE PROCEDURE [dbo].[spu_PurgeRollupData]
        @PointList   as xml
        AS
BEGIN
       SET NOCOUNT ON;

    IF @PointList IS NULL RETURN
    -- Get rollup data pointsliceID
    DECLARE @RollupPointSliceIDs TABLE
    (
	    PointSliceID int NOT NULL
    )

    INSERT INTO @RollupPointSliceIDs (PointSliceID) 
	    SELECT          PointSliceID
	    FROM            dbo.tblPointSlice                        AS ps
	    INNER JOIN      @PointList.nodes(N'/root/row')           AS m(r)
        ON              m.r.value(N'@point', N'int') =  ps.PointId            
	    WHERE           IsRawData                 =  0

    UPDATE      tblPointSlice 
    SET         LowTimeMarker = '1900-01-01', HighTimeMarker = '1900-01-01'
    FROM        tblPointSlice                               AS ps
    INNER JOIN  @RollupPointSliceIDs                        AS psid
    ON          ps.PointSliceId = psid.PointSliceID
    WHERE       IsRawData = 0


END

GO
/****** Object:  StoredProcedure [dbo].[spu_GetTrendSampleCount]    Script Date: 03/23/2015 13:36:46 ******/
IF  EXISTS  ( SELECT  * 
              FROM    dbo.sysobjects 
              WHERE   id = OBJECT_ID(N'[dbo].[spu_GetTrendSampleCount]') and OBJECTPROPERTY(id, N'IsProcedure') = 1
            )
DROP PROCEDURE [dbo].[spu_GetTrendSampleCount]
GO
-- ===============================================================================================================================
--  Copyright 2009 Johnson Controls, Inc.
--    Use or copying of all or any part of the document, except as permitted
--    by the License Agreement is prohibited.
-- ===============================================================================================================================
--  NAME:         spu_GetTrendSampleCount
--  DESCRIPTION:  get trend sample count for a time period
--
----    Revision History
-- -------------------------------------------------------------------------------------------------------------------------------
--  Revison Date        Name                Description (Prob #):
--  ------  ---------   ---------------     ------------------------------------------------------------------------------------------
--  5.0.0   2009May11  cwagnem            Original
-- ===============================================================================================================================
GO

CREATE PROCEDURE [dbo].[spu_GetTrendSampleCount] 


    (
    @type             INT,
    @id               INT,
    @dateStart        DATETIME,
    @dateEnd          DATETIME,
    @count            INT OUT
    )
AS


SET @count = 0
IF (@type = 1)
   BEGIN
   SELECT @count  = COUNT(UTCDateTime)
   FROM tblActualValueFloat
   WHERE PointSliceID = (SELECT PointSliceID 
                         FROM tblPointSlice 
                         WHERE PointID = @id AND 
                         IsRawData = 1) AND 
         (UTCDateTime >= @dateStart AND UTCDateTime <= @dateEnd)
   SELECT @count = @count + COUNT(UTCDateTime)
   FROM tblOtherValueFloat
   WHERE PointSliceID = (SELECT PointSliceID 
                         FROM tblPointSlice 
                         WHERE PointID = @id AND
                         IsRawData = 1) AND
         (UTCDateTime >= @dateStart AND UTCDateTime <= @dateEnd) AND
         ValueCategoryID = 1
   END
ELSE
   BEGIN
   SELECT @count = COUNT(UTCDateTime)
   FROM tblActualValueDigital
   WHERE PointSliceID = (SELECT PointSliceID FROM tblPointSlice 
                         WHERE PointID = @id AND 
                         IsRawData = 1) AND 
         (UTCDateTime >= @dateStart AND UTCDateTime <= @dateEnd)
   SELECT @count = @count + COUNT(UTCDateTime)
   FROM tblOtherValueDigital
   WHERE PointSliceID = (SELECT PointSliceID FROM tblPointSlice 
                         WHERE PointID = @id AND
                         IsRawData = 1) AND
         (UTCDateTime >= @dateStart AND UTCDateTime <= @dateEnd) AND
         ValueCategoryID = 1
   END

GO
/****** Object:  StoredProcedure [dbo].[spu_PurgeJCIHistorianDb]    Script Date: 03/23/2015 13:36:46 ******/

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spu_PurgeJCIHistorianDb]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].spu_PurgeJCIHistorianDb
GO

/*===========================================================================
(C) Copyright Johnson Controls, Inc. 2007-2009
Use or Copying of all or any part of this program, except as permitted by
License Agreement,is prohibited.

DESCRIPTION: This proc will purge old records from the JCIHistorianDb database

INPUTS: 
    @PurgeTime        datatime defining the boundry line where all older raw data 
                      records should deleted - No default
    @RollupPurgeTime  datatime defining the boundry line where all older rollup
                      records should deleted - Default is NULL which means to keep 
                      @PurgeTime + 1 year.
            ALL TIMES ARE UTC
OUTPUTS: Old records are removed from the database

RETURNS: No return set

EXAMPLE: 
    exec dbo.spu_PurgeJCIHistorianDB '2000-12-31T00:00:00' 
    exec dbo.spu_PurgeJCIHistorianDB '2000-12-31T00:00:00', '2000-12-31T00:00:00'

History:                   
--------------  -----------    ---------   ---------    ---------------------------------------
Date            Author         Release     Task#        Reason for change
--------------  -----------    ---------   ---------    ---------------------------------------
2007-11-19      cschust                                 Original
2008-10-29      cschust         5.0         70237       Handle EUPM Rollup data
2009-12-21      cschust         5.0         78353       Leave a raw sample =< purge data for rollup process
29Mar2010       cschust         5.1         80793       Fix purge when running on empty db
===========================================================================*/


CREATE proc [dbo].[spu_PurgeJCIHistorianDb] 
    (
    @PurgeTime              datetime,
    @RollupPurgeTime        datetime = NULL
    )

as

set nocount on
-- CONSTANTS
DECLARE                     @PURGE_ALL_YEAR                 int
SET                         @PURGE_ALL_YEAR                 = 9999

-- VARIABLES
DECLARE                     @IsRawData                      bit
DECLARE                     @loop                           int
DECLARE                     @Row                            int  
DECLARE                     @RowsDeleted                    bit
SET                         @RowsDeleted                    = 0
DECLARE                     @SliceTime                      datetime
DECLARE                     @DefaultMarkerTime              datetime
SET                         @DefaultMarkerTime              = '1900-01-01T00:00:00'
DECLARE                     @MaxId                          int                         
DECLARE                     @MinId                          int                         

-- Temp tables
CREATE TABLE                #tbl                            
(
                            [id]                            int
)
 
CREATE TABLE                #ps                            
(
                            [id]                            int
) 

DECLARE                     @SlicePurgeDates                table
                         (
                            RowId                           int identity
                           ,PointSliceId                    int
                           ,PurgeTime                       datetime
                           ,PRIMARY KEY (PointSliceId, PurgeTime, Rowid)
                         )

-- Configure rollup purge date
IF   (@RollupPurgeTime is NULL)  
    SET                     @RollupPurgeTime                = DATEADD(yy, -1, @PurgeTime)


-- GET each pointslice purge date into a temp table
IF                          YEAR(@PurgeTime)                <> @PURGE_ALL_YEAR   
BEGIN
    SELECT                  @MaxId                           = COALESCE(max (pointsliceid) , 0)
                          , @MinId                           = COALESCE(min (pointsliceid) , 1) 
    FROM                    dbo.tblPointSlice

    SET @loop = @MaxId
    SELECT                  @IsRawData                      = IsRawData 
    FROM                    dbo.tblPointSlice 
    WHERE                   PointSliceId                    = @loop

    WHILE                  (@loop                           >= @MinId)
    BEGIN 
        IF (@IsRawData                 = 1) 
        BEGIN -- Set purge time to keep at least 1 raw data sample <= the desred purge time
               SELECT   @SliceTime = coalesce(MAX(ps.UTCDateTime), @PurgeTime)
               FROM
               (
                   SELECT MAX(UTCDateTime) as UTCDateTime 
                   FROM                    dbo.tblActualValueDigital
                   WHERE                   PointSliceID                =  @loop
                   AND                     UTCDateTime                <=  @PurgeTime  
                   UNION
                   SELECT MAX(UTCDateTime) as UTCDateTime 
                   FROM                    dbo.tblActualValueFloat
                   WHERE                   PointSliceID                =  @loop
                   AND                     UTCDateTime                <=  @PurgeTime  
               )                                                                           as ps

        END
        ELSE
        BEGIN -- Set purge time to the greater of RollupPurgeTime or slice low time marker
               SELECT   @SliceTime = CASE
                                   WHEN LowTimeMarker > @RollupPurgeTime
                                      THEN LowTimeMarker
                                   ELSE 
                                      @RollupPurgeTime
                                   END
               from     tblPointSlice
               WHERE    PointSliceID                               =  @loop 
        END

        INSERT INTO @SlicePurgeDates
                   (PointSliceId, PurgeTime)
             VALUES(@Loop,       @SliceTime)
             
    SET @Loop = @loop - 1
             
    SELECT                  @IsRawData                      = IsRawData 
    FROM                    dbo.tblPointSlice 
    WHERE                   PointSliceId                    = @loop
             
 
    END
END



----------------------------------------------------------------------------
-- purge value data from all value tables
----------------------------------------------------------------------------

IF                          YEAR(@PurgeTime)                = @PURGE_ALL_YEAR   
BEGIN
    TRUNCATE TABLE          dbo.tblActualValueFloat
    TRUNCATE TABLE          dbo.tblActualValueDigital
    TRUNCATE TABLE          dbo.tblOtherValueFloat
    TRUNCATE TABLE          dbo.tblOtherValueDigital

    DELETE	 FROM			tblPointUnit
    DELETE   FROM			tblUnit
    DELETE   FROM			tblPointSlice
    DELETE   FROM			tblPoint

    DBCC CHECKIDENT('tblPointSlice', RESEED, 1) 
    DBCC CHECKIDENT('tblPoint',      RESEED, 1) 

    --  Set exit flag, we cleaned up all data in this block
    SET                     @RowsDeleted                    = 0
END
ELSE 
BEGIN

    SET  ROWCOUNT           10000 
    SET                     @loop                           = @MaxId
 
    -- Delete values from each table one pointslice at a time.
    -- We are chunking the deletes to control the log file size as best as possible. 
    WHILE                  (@loop                           >= @MinId)
    BEGIN
    
    -- Get the slices predefined purge date
    SELECT                  @SliceTime                       = COALESCE( PurgeTime, @PurgeTime)
    FROM                    @SlicePurgeDates
    WHERE                   PointSliceId                     = @loop
    SET                     @row                             = 1
   
    WHILE                  (@Row                             > 0)
    BEGIN 
 
        -- Purge from the tblActualValueFloat table            
        DELETE                     
        FROM                dbo.tblActualValueFloat    
        WHERE               PointSliceId                    = @loop
        AND                 UTCDateTime                     < @SliceTime 
 
        SET                 @Row                            = @@rowcount  
        
        
        IF                 (@Row                            > 0) 
        BEGIN 
            SET             @RowsDeleted                    = 1
        END
    END 
    
    SET                     @row                             = 1

    WHILE                  (@Row                           > 0)
    BEGIN 
        
        -- Purge from the tblActualValueDigital table            
        DELETE
        FROM                dbo.tblActualValueDigital   
        WHERE               PointSliceId                    = @loop
        AND                 UTCDateTime                     < @SliceTime 
  
        SET                 @Row                            = @@rowcount  


        
        IF                 (@Row                            > 0) 
        BEGIN 
            SET             @RowsDeleted                    = 1
        END
    END 
    
    SET                     @row                            = 1

    WHILE                  (@Row                            > 0)
    BEGIN 
 
        -- Purge from the tblOtherValueDigital table            
        DELETE
        FROM                dbo.tblOtherValueDigital  
        WHERE               PointSliceId                    = @loop
        AND                 UTCDateTime                     < @SliceTime 
 
        SET                 @Row                            = @@rowcount  

         
        IF                 (@Row                            > 0) 
        BEGIN 
            SET             @RowsDeleted                    = 1
        END
     END 
    
    SET                     @row                            = 1

    WHILE                  (@Row                            > 0)
    BEGIN 
              
        -- Purge from the tblOtherValueFloat table            
        DELETE
        FROM              dbo.tblOtherValueFloat    
        WHERE               PointSliceId                    = @loop
        AND                 UTCDateTime                     < @SliceTime 
 
        SET                 @Row                            = @@rowcount  
        
        IF                 (@Row                            > 0) 
        BEGIN 
             SET            @RowsDeleted                    = 1
        END
    END 
        -- Get the next PointSlice loop value
        SET                 @loop                           = @loop - 1
    END
END

-- Restore the rowcount value
SET ROWCOUNT                0 


--Exit if we did not do any deletes to save time
IF @RowsDeleted = 0
    RETURN


IF                          YEAR(@PurgeTime)                <> @PURGE_ALL_YEAR
BEGIN
    --Update the point slice low time markers 
    UPDATE                  dbo.tblpointSlice
    SET                     LowTimeMarker                   = spd.PurgeTime
    FROM                    dbo.tblpointSlice               AS ps
    JOIN (SELECT            PointSliceId      as PointSliceId
                          , min(PurgeTime)    as PurgeTime
          FROM              @SlicePurgeDates
          GROUP BY          PointSliceId)                   AS spd
    ON                      ps.PointSliceID                 = spd.PointSliceId
END
ELSE -- Set default time markers
    UPDATE                  dbo.tblpointSlice
    SET                     LowTimeMarker                   = @DefaultMarkerTime
                          , HighTimeMarker                  = @DefaultMarkerTime



----------------------------------------------------------------------------
-- Find unused pointslices to purge association tables
-- A pointslice is unused only if ALL pointslices are empty for a particular 
-- point
----------------------------------------------------------------------------

INSERT into                 #ps 
SELECT                      PointSliceId
FROM                        dbo.tblPointSlice
LEFT JOIN
(   -- Get the used PS IDs
    SELECT                  Distinct dbo.tblPointSlice.PointSliceID  as testid
    FROM                    dbo.tblPointSlice
    WHERE                   PointID IN
                                 (
                                 SELECT Distinct PointID
                                 FROM   dbo.tblPointSlice
                                 WHERE  PointSliceID IN
                                    (
                                    SELECT                  Distinct PointSliceID
                                    FROM                    dbo.tblActualValueDigital 
                                    UNION
                                    SELECT                  Distinct PointSliceID
                                    FROM                    dbo.tblActualValueFloat 
                                    UNION
                                    SELECT                  Distinct PointSliceID
                                    FROM                    dbo.tblOtherValueDigital 
                                    UNION
                                    SELECT                  Distinct PointSliceID
                                    FROM                    dbo.tblOtherValueFloat 
                                    )
                                 )
)
                                                            as subquery
ON                          dbo.tblPointSlice.PointSliceId  = subquery.TestId
WHERE                       subquery.TestId                 IS NULL

----------------------------------------------------------------------------
-- tblPointUnit
-- We need to remove pointunit entries only for points that have NO
-- pointslice table entries
---------------------------------------------------------------------------

TRUNCATE TABLE              #tbl
INSERT into                 #tbl 
SELECT                      PointId 
FROM                        dbo.tblPointSlice              as tps
LEFT JOIN                   #ps                            as ps
ON                          tps.PointSliceId               = ps.[Id]
WHERE                       ps.[ID]                        IS NOT NULL

SET ROWCOUNT                5000
SET                         @Row                           = 1 
WHILE                      (@Row                           > 0) 
BEGIN 
    DELETE FROM             dbo.tblPointUnit
    WHERE                   PointId IN (SELECT [id] FROM #tbl)
    SET                     @Row =                         @@rowcount  
End
SET ROWCOUNT                0 

----------------------------------------------------------------------------
-- tblUnit
----------------------------------------------------------------------------

TRUNCATE TABLE              #tbl
INSERT into                 #tbl 
SELECT                      dbo.tblUnit.UnitId
FROM                        dbo.tblUnit
LEFT JOIN                   dbo.tblPointUnit 
ON                          dbo.tblUnit.UnitId             = dbo.tblPointUnit.UnitID 
WHERE                       dbo.tblPointUnit.UnitID        IS NULL

SET ROWCOUNT                5000
SET                         @Row                           = 1 
WHILE                      (@Row                           > 0) 
BEGIN 
    DELETE FROM             dbo.tblUnit 
    WHERE                   UnitID IN (SELECT [id] FROM #tbl)         
    SET                     @Row =                         @@rowcount  
End
SET ROWCOUNT                0 

----------------------------------------------------------------------------
-- tblPointSlice 
----------------------------------------------------------------------------

SET ROWCOUNT                5000
SET                         @Row                           = 1 
WHILE                      (@Row                           > 0) 
BEGIN 
    DELETE  
    FROM                    dbo.tblPointSlice 
    WHERE                   PointSliceId in (SELECT [ID]  FROM #ps)                   
    SET                     @Row =                         @@rowcount  
End
SET ROWCOUNT                0 

----------------------------------------------------------------------------
-- tblPoint
----------------------------------------------------------------------------
-- Cleaned by a trigger
GO

/****** Object:  StoredProcedure [dbo].[spu_InsertTrendSample]    Script Date: 03/23/2015 13:36:46 ******/
IF EXISTS (SELECT name FROM sysobjects WHERE name = 'spu_InsertTrendSample')
	DROP PROCEDURE spu_InsertTrendSample
GO

CREATE  PROCEDURE spu_InsertTrendSample
	@PointSliceID	INT,
	@FloatValue	    FLOAT,
	@IntValue		INT,
	@RawDataStatus	INT,
	@PointDataType 	INT,
	@IsReliable 	BIT,
	@UtcDateTime	DATETIME
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @TRUE bit;                  SET @TRUE               = 1
	DECLARE @FALSE bit;                 SET @FALSE              = 0

	-- Translations for values in tblPointSliceValueTemp..PointDataTypeId
	DECLARE @FLOAT_POINT_DT_ID int;     SET @FLOAT_POINT_DT_ID      = 1
	DECLARE @DIGITAL_POINT_DT_ID int;   SET @DIGITAL_POINT_DT_ID    = 2
	DECLARE @BOOL_POINT_DT_ID int;      SET @BOOL_POINT_DT_ID       = 3

	-- Default ValueCategoryId for Unreliable samples = 1 ('Bad Status')
	DECLARE @UNRELIABLE_VAL_CAT_ID int; SET @UNRELIABLE_VAL_CAT_ID  = 1

	DECLARE @IsDigital  BIT; 			SET @IsDigital  = 0

				
	SET @IsDigital = CASE @pointDataType
							WHEN @BOOL_POINT_DT_ID      THEN @TRUE
							WHEN @DIGITAL_POINT_DT_ID   THEN @TRUE
						ELSE @FALSE END

	IF (@IsDigital = 0 AND @isReliable = 0)
	BEGIN
		INSERT INTO dbo.tblOtherValueFloat (PointSliceID, [UTCDateTime], OtherValue, ValueCategoryID, [Status])
		VALUES(@pointSliceID, @utcDateTime, @floatValue, @UNRELIABLE_VAL_CAT_ID, @rawDataStatus)
	END

	IF (@IsDigital = 1 AND @isReliable = 0)
	BEGIN
		INSERT INTO dbo.tblOtherValueDigital ( PointSliceID, [UTCDateTime], OtherValue, ValueCategoryID, [Status])
		VALUES(@pointSliceID, @utcDateTime, @intValue, @UNRELIABLE_VAL_CAT_ID, @rawDataStatus)
	END

	IF (@IsDigital = 0 AND @isReliable = 1)
	BEGIN
		INSERT INTO dbo.tblActualValueFloat ( PointSliceID, [UTCDateTime], ActualValue)
		VALUES(@pointSliceID, @utcDateTime, @floatValue)
	END

	IF (@IsDigital = 1 AND @isReliable = 1)
	BEGIN
		INSERT INTO dbo.tblActualValueDigital ( PointSliceID, [UTCDateTime], ActualValue)
		VALUES(@pointSliceID, @utcDateTime, @intValue)
	END
END

GO
/****** Object:  StoredProcedure [dbo].[spu_ImportTrendData]    Script Date: 03/23/2015 13:36:46 ******/
IF  EXISTS  ( SELECT  * 
              FROM    dbo.sysobjects 
              WHERE   id = OBJECT_ID(N'[dbo].[spu_ImportTrendData]') and OBJECTPROPERTY(id, N'IsProcedure') = 1
            )
DROP PROCEDURE [dbo].[spu_ImportTrendData]
GO
-- ===============================================================================================================================
--  Copyright 2008 Johnson Controls, Inc.
--    Use or copying of all or any part of the document, except as permitted
--    by the License Agreement is prohibited.
-- ===============================================================================================================================
--  NAME:         spu_ImportTrendData
--  DESCRIPTION:  Merges data in tblPointSliceValueTemp into a combination of the following tables:
--                  tblActualValueDigital
--                  tblActualValueFloat
--                  tblOtherValueDigital
--                  tblOtherValueFloat
--
--                If needed, new records will be added to:
--                  tblPoint
--                  tblPointSlice
--                  tblUnit
--                  tblPointUnit
--
--                 Routine will update certain fields of existing tblUnit and tblPoint tables.
--                 Routine will also update existing sample values (this is new behavior vs prior imports).
-- ===============================================================================================================================
--  Inputs
-- -------------------------------------------------------------------------------------------------------------------------------
--  Name            Type    Description
--  --------------- ------- ----------------------------------------------------------------------------------------------------------
-- ===============================================================================================================================
--  Outputs - none
--
-- ===============================================================================================================================
--  Result Set - none
--
-- ===============================================================================================================================
--    Revision History
-- -------------------------------------------------------------------------------------------------------------------------------
--  Revison Date        Name                Description (Prob #):
--  ------  ---------   ---------------     --------------------------------------------------------------------------------------
--  5.0.0   2008Sep29   Kyle Stittleburg    Original
--  5.0.1   2008Oct13   Kyle Stittleburg    Added support for updating samples, added performance tweaks (#tempVal and utilizing AddressId field)
--  5.0.2   2008Oct14   Kyle Stittleburg    Routine will insert dummy row into tblUnitOfMeasure, if needed, preserving old logic.
--  5.0.3   2008Dec04   Kyle Stittleburg    Remove any samples of type Float that have null FloatValue and samples of type Digital that have null IntegerValue
--  5.2.0   2010Jun03   Dave Snyder         Disable trgInsertUpdatePointSlice trigger once the transaction begins, and re-enable
--                                           it when the transaction is just about complete.
-- ===============================================================================================================================
GO

CREATE PROCEDURE [dbo].[spu_ImportTrendData] 
  -- Add the parameters for the stored procedure here
AS
BEGIN
  BEGIN TRY

    SET NOCOUNT ON

    --
    -- Constants  
    --
    DECLARE @UNIT_CATEGORY_ID int
    SET     @UNIT_CATEGORY_ID = 3 -- Default category for a new record in tblUnit

    DECLARE @UNIT_DESCRIPTION nvarchar(510)
    SET     @UNIT_DESCRIPTION = N'(Added by Metasys III Trend)' -- Default description for a new record in tblUnit

    DECLARE @TRUE bit;                  SET @TRUE               = 1
    DECLARE @FALSE bit;                 SET @FALSE              = 0 

    DECLARE @LOG_STATUS_SAMPLE int;     SET @LOG_STATUS_SAMPLE  = 1
    DECLARE @BOOL_SAMPLE int;           SET @BOOL_SAMPLE        = 2
    DECLARE @FLOAT_SAMPLE int;          SET @FLOAT_SAMPLE       = 3
    DECLARE @ENUM_SAMPLE int;           SET @ENUM_SAMPLE        = 4
    DECLARE @UNSIGNED_SAMPLE int;       SET @UNSIGNED_SAMPLE    = 5
    DECLARE @SIGNED_SAMPLE int;         SET @SIGNED_SAMPLE      = 6
    DECLARE @BITSTRING_SAMPLE int;      SET @BITSTRING_SAMPLE   = 7
    DECLARE @NONE_SAMPLE int;           SET @NONE_SAMPLE        = 8
    DECLARE @ERROR_SAMPLE int;          SET @ERROR_SAMPLE       = 9
    DECLARE @TIME_CHANGE_SAMPLE int;    SET @TIME_CHANGE_SAMPLE = 10 

    -- Translations for values in tblPointSliceValueTemp..PointDataTypeId
    DECLARE @FLOAT_POINT_DT_ID int;     SET @FLOAT_POINT_DT_ID      = 1
    DECLARE @DIGITAL_POINT_DT_ID int;   SET @DIGITAL_POINT_DT_ID    = 2 
    DECLARE @BOOL_POINT_DT_ID int;      SET @BOOL_POINT_DT_ID       = 3

    -- Default ValueCategoryId for Unreliable samples = 1 ('Bad Status')
    DECLARE @UNRELIABLE_VAL_CAT_ID int; SET @UNRELIABLE_VAL_CAT_ID  = 1

    --
    -- local variables
    --
    DECLARE @NewUnitID  int
        ,   @UserName   nvarchar(400)
        ,   @PointID    int

    DECLARE @Points xml

    DECLARE @UniquePoints table
    (
            UserName        nvarchar(400)
        ,   PointName       nvarchar(400) primary key
        ,   TimeZoneID      int
        ,   UnitOfMeasureID int
        ,   PointDataTypeID int
        ,   EnumSetId       int
    )

    -- Hold new identity IDs from INSERTs into tblUnit
    DECLARE @NewPointUnits table
    (
            UnitID int
    )
    
    -- Temporary table holds just VALUE related fields, for performance reasons
    CREATE TABLE #tempVal
    (
        PointSliceID    int         NOT NULL,
	    UTCDateTime     datetime    NOT NULL,
	    IsActual        bit         NOT NULL,
	    IsDigital       bit         NOT NULL,
	    DigitalValue    smallint    NULL,
	    FloatValue      float        NULL,
	    Status          int         NULL
    )

    BEGIN TRANSACTION;
    
    DISABLE TRIGGER dbo.trgInsertUpdatePointSlice ON dbo.tblPointSlice;

    -- Delete all the Sample Type which is 1,7, 8,9, 10
    -- Delete any invalid data where we have a Float point with NULL FloatValue
    -- Delete any invalid digital/bools where we have a Digital/Bool point type with NULL DigitalValue
    -- Delete any points of PointDataTypeId not in (Bool, Digital, Float)
    DELETE FROM dbo.tblPointSliceValueTemp
    WHERE SampleType IN (@LOG_STATUS_SAMPLE, @BITSTRING_SAMPLE, @NONE_SAMPLE, @ERROR_SAMPLE, @TIME_CHANGE_SAMPLE)
        OR  (PointDataTypeId IN (@BOOL_POINT_DT_ID, @DIGITAL_POINT_DT_ID) AND IntegerValue IS NULL)
        OR  (PointDataTypeId = @FLOAT_POINT_DT_ID AND FloatValue IS NULL)
        OR  (PointDataTypeId NOT IN (@BOOL_POINT_DT_ID, @FLOAT_POINT_DT_ID, @DIGITAL_POINT_DT_ID)) -- No unsupported point data types

    -- Grab unique points and store into a table variable (hopefully better performance vs. repeatedly doing this with a join to temp table)
    INSERT INTO @UniquePoints (UserName, PointName, TimeZoneID, UnitOfMeasureID, PointDataTypeID, EnumSetId)
    SELECT  psvt.UserName
            ,   psvt.ObjectReference + '.' + psvt.DictionaryString
            ,   psvt.TimeZoneID
            ,   psvt.UnitOfMeasureID
            ,   psvt.PointDataTypeID
            ,   psvt.EnumSetID
    FROM    dbo.tblPointSliceValueTemp AS psvt
    GROUP BY UserName, ObjectReference, DictionaryString, TimeZoneID, UnitOfMeasureID, PointDataTypeID, EnumSetID

    -- If the Unit Of Measure given does not exist, enter a dummy row for it.
    -- NOTE: We only care about storing the correct Unit Of Measure ID.
    INSERT INTO tblUnitOfMeasure (UnitOfMeasureID, UnitOfMeasureName, DisplayNameShort, MeasureType)
    SELECT  
        up.UnitOfMeasureID
    ,   'Units ' + RTRIM(CONVERT(varchar(5), up.UnitOfMeasureID)) AS UnitOfMeasureName
    ,   RTRIM(CONVERT(varchar(5), up.UnitOfMeasureID)) AS DisplayNameShort
    ,   'Other' AS MeasureType
    FROM    @UniquePoints AS up
    LEFT JOIN dbo.tblUnitOfMeasure AS uom ON uom.UnitOfMeasureID = up.UnitOfMeasureID
    WHERE    uom.UnitOfMeasureID IS NULL -- does not exist
    GROUP BY up.UnitOfMeasureID
    
    -- Get the list of points that their UOM are changed
    SET @Points = ( SELECT p.PointId as [point]
		FROM    @UniquePoints AS up
		INNER JOIN dbo.tblPoint AS p ON p.PointName = up.PointName
		WHERE   
			p.UnitOfMeasureID <> up.UnitOfMeasureID
        FOR XML RAW (N'row'), ROOT(N'root') )

    EXECUTE spu_PurgeRollupData @PointList = @Points

    -- Update existing Point records, if needed
    UPDATE  p
    SET     PointDataTypeID = up.PointDataTypeID, 
			TimeZoneID      = up.TimeZoneId,
			UnitOfMeasureID = up.UnitOfMeasureID,
			enumSet         = up.EnumSetID
    FROM    @UniquePoints AS up
    INNER JOIN dbo.tblPoint AS p ON p.PointName = up.PointName
    WHERE   
        p.TimeZoneID <> up.TimeZoneId
        OR p.PointDataTypeID <> up.PointDataTypeID
        OR p.UnitOfMeasureID <> up.UnitOfMeasureID
        OR p.EnumSet <> up.EnumSetId

    -- Add any missing Point records
    INSERT INTO dbo.tblPoint(PointName, TimeZoneID, UnitOfMeasureID, PointDataTypeID, EnumSet)
    SELECT  
            up.PointName
        ,   up.TimeZoneID
        ,   up.UnitOfMeasureID
        ,   up.PointDataTypeID
        ,   up.EnumSetID
    FROM    @UniquePoints AS up
    LEFT JOIN dbo.tblPoint AS p ON p.PointName = up.PointName
    WHERE   p.PointID IS NULL

    -- Update existing Unit records, if needed
    UPDATE  u
    SET     [Name] = up.UserName
    FROM    @UniquePoints AS up
    INNER JOIN dbo.tblPoint AS p ON p.PointName = up.PointName
    INNER JOIN dbo.tblPointUnit AS pu ON pu.PointId = p.PointId
    INNER JOIN dbo.tblUnit AS u on u.UnitId = pu.UnitId
    WHERE   u.[Name] <> up.UserName

    -- Add any missing Unit records
    INSERT INTO dbo.tblUnit([Name], UnitCategoryID, [Description], [AddressID])
    OUTPUT inserted.UnitId INTO @NewPointUnits
    SELECT  
            up.UserName
        ,   @UNIT_CATEGORY_ID AS UnitCategoryId
        ,   @UNIT_DESCRIPTION AS [Description]
        ,   p.PointID AS [AddressID]      -- This is just temporary so we can link back to tblPoint when creating tblPointUnit records
    FROM    @UniquePoints AS up
    INNER JOIN dbo.tblPoint AS p ON p.PointName = up.PointName
    LEFT JOIN dbo.tblPointUnit AS pu ON pu.PointId = p.PointId
    LEFT JOIN dbo.tblUnit AS u on u.UnitId = pu.UnitId
    WHERE   pu.UnitID IS NULL

    -- Add any missing PointUnit association records (tblPointUnit)
    INSERT INTO dbo.tblPointUnit(PointID, UnitID)
    SELECT  p.PointID
        ,   npu.UnitID
    FROM    @NewPointUnits AS npu
    INNER JOIN dbo.tblUnit AS u ON u.UnitID = npu.UnitID
    INNER JOIN dbo.tblPoint AS p ON p.PointID = u.AddressID

    -- Clear out AddressID that we're temporarily using
    UPDATE  dbo.tblUnit SET AddressID = NULL WHERE UnitID IN (SELECT UnitID from @NewPointUnits)

--  Note: If above "hack" doesn't fly, here's a cursor way of accomplishing the same goal
--    -- Add any missing Unit and PointUnit records
--    --  We use a cursor here because tblUnit does not have a natural/alternate key
--    --  so if we're loading two points with the same object name, we need to ensure that
--    --  we grab the correct UnitIDs for each
--    DECLARE curMissingUnits CURSOR FAST_FORWARD FOR
--        SELECT  
--                p.PointID
--            ,   up.ObjectName
--        FROM    @UniquePoints AS up
--        INNER JOIN dbo.tblPoint AS p ON p.PointName = up.PointName
--        LEFT JOIN dbo.tblPointUnit AS pu ON pu.PointId = p.PointId
--        LEFT JOIN dbo.tblUnit AS u on u.UnitId = pu.UnitId
--        WHERE   pu.UnitID IS NULL
--
--    OPEN curMissingUnits
--
--    FETCH NEXT FROM curMissingUnits INTO @PointID, @ObjectName
--
--    WHILE (@@fetch_status = 0)
--    BEGIN
--        -- Add Unit Record (tblUnit)
--        INSERT INTO dbo.tblUnit([Name], UnitCategoryID, [Description])
--        VALUES (@ObjectName, @UNIT_CATEGORY_ID, @UNIT_DESCRIPTION)
--        
--        SET @NewUnitID = SCOPE_IDENTITY()
-- 
--        -- Add PointUnit Record (tblPointUnit)
--        INSERT INTO dbo.tblPointUnit(PointID, UnitID)
--        VALUES (@PointID, @NewUnitID)
--
--        FETCH NEXT FROM curMissingUnits INTO @PointID, @ObjectName
--    END
--
--    CLOSE curMissingUnits
--    DEALLOCATE curMissingUnits


    -- Add any Point Slices needed
    INSERT INTO dbo.tblPointSlice (PointID, IsRawData)
    SELECT  p.PointID
        ,   1 as IsRawData
    FROM    @UniquePoints AS up
    INNER JOIN dbo.tblPoint AS p 
        ON p.PointName = up.PointName
    LEFT JOIN dbo.tblPointSlice AS ps 
        ON ps.PointID = p.PointID
            AND ps.IsRawData = 1
    WHERE   ps.PointSliceId IS NULL

    -- Break out just the value related data, and filter out any duplicates
    INSERT INTO #tempVal(PointSliceID, UTCDateTime, IsActual, IsDigital, DigitalValue, FloatValue, Status)
    SELECT  
        ps.PointSliceID
    ,   psvt.UTCDateTime
    ,   psvt.IsReliable
    ,   CASE psvt.PointDataTypeID
            WHEN @BOOL_POINT_DT_ID      THEN @TRUE
            WHEN @DIGITAL_POINT_DT_ID   THEN @TRUE
            ELSE @FALSE
        END AS IsDigital
    ,   psvt.IntegerValue
    ,   psvt.FloatValue
    ,   psvt.RawStatus
    FROM    tblPointSliceValueTemp AS psvt
    INNER JOIN dbo.tblPoint AS p 
        ON p.PointName = (psvt.ObjectReference + '.' + psvt.DictionaryString)
    INNER JOIN dbo.tblPointSlice AS ps 
        ON ps.PointID = p.PointID AND ps.IsRawData = 1;

    -- Remove any duplicates (Same PointSliceId and UTCDateTime)
    WITH Dups AS
    (
        SELECT  *
            ,   ROW_NUMBER() OVER (PARTITION BY PointSliceId, [UtcDateTime] ORDER BY PointSliceID, [UtcDateTime]) AS rn
        FROM    #tempVal
    )
    DELETE
    FROM    Dups
    WHERE   rn > 1;
    
    -- Handle four sets of inserts/updates:
    --  IsDigital = 0, IsActual = 0 goes into tblOtherValueFloat
    UPDATE  ovf
    SET     
        OtherValue      = psvt.FloatValue
    ,   [Status]        = psvt.Status
    FROM    #tempVal AS psvt
    INNER JOIN dbo.tblOtherValueFloat AS ovf
        ON ovf.PointSliceID = psvt.PointSliceID
        AND ovf.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 0 AND psvt.IsActual = 0
    
    INSERT INTO dbo.tblOtherValueFloat (
            PointSliceID
        ,   [UTCDateTime]
        ,   OtherValue
        ,   ValueCategoryID
        ,   [Status]
    )
    SELECT  psvt.PointSliceID       AS PointSliceID
        ,   psvt.UTCDateTime        AS [UTCDateTime]
        ,   psvt.FloatValue         AS OtherValue
        ,   @UNRELIABLE_VAL_CAT_ID  AS ValueCategoryID
        ,   psvt.Status             AS Status
    FROM   #tempVal AS psvt
    LEFT JOIN dbo.tblOtherValueFloat AS ovf
        ON ovf.PointSliceID = psvt.PointSliceID
        AND ovf.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital  = 0 
        AND psvt.IsActual   = 0
        AND ovf.PointSliceID IS NULL
    
    --  IsDigital = 1, IsActual = 0 goes into tblOtherValueDigital
    UPDATE  ovd
    SET     
        OtherValue      = psvt.DigitalValue
    ,   [Status]        = psvt.Status
    FROM    #tempVal AS psvt
    INNER JOIN dbo.tblOtherValueDigital AS ovd
        ON ovd.PointSliceID = psvt.PointSliceID
        AND ovd.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 1 AND psvt.IsActual = 0

    INSERT INTO dbo.tblOtherValueDigital (
            PointSliceID
        ,   [UTCDateTime]
        ,   OtherValue
        ,   ValueCategoryID
        ,   [Status]
    )
    SELECT  psvt.PointSliceID       AS PointSliceID
        ,   psvt.UTCDateTime        AS [UTCDateTime]
        ,   psvt.DigitalValue       AS OtherValue
        ,   @UNRELIABLE_VAL_CAT_ID  AS ValueCategoryID
        ,   psvt.Status             AS Status
    FROM    #tempVal AS psvt
    LEFT JOIN dbo.tblOtherValueDigital AS ovd
        ON ovd.PointSliceID = psvt.PointSliceID
        AND ovd.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 1 
            AND psvt.IsActual = 0
            AND ovd.PointSliceID IS NULL

    --  IsDigital = 0, IsActual = 1 goes into tblActualValueFloat
    UPDATE  avf
    SET     
        ActualValue      = psvt.FloatValue
    FROM    #tempVal AS psvt
    INNER JOIN dbo.tblActualValueFloat AS avf
        ON avf.PointSliceID = psvt.PointSliceID
        AND avf.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 0 AND psvt.IsActual = 1

    INSERT INTO dbo.tblActualValueFloat (
            PointSliceID
        ,   [UTCDateTime]
        ,   ActualValue
    )
    SELECT  psvt.PointSliceID     AS PointSliceID
        ,   psvt.UTCDateTime    AS [UTCDateTime]
        ,   psvt.FloatValue     AS ActualValue
    FROM    #tempVal AS psvt
    LEFT JOIN dbo.tblActualValueFloat AS avf
        ON avf.PointSliceID = psvt.PointSliceID
        AND avf.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 0 
            AND psvt.IsActual = 1
            AND avf.PointSliceID IS NULL

    --  IsDigital = 1, IsActual = 1 goes into tblActualValueDigital
    UPDATE  avd
    SET     
        ActualValue      = psvt.DigitalValue
    FROM    #tempVal AS psvt
    INNER JOIN dbo.tblActualValueDigital AS avd
        ON avd.PointSliceID = psvt.PointSliceID
        AND avd.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 1 AND psvt.IsActual = 1

    INSERT INTO dbo.tblActualValueDigital (
            PointSliceID
        ,   [UTCDateTime]
        ,   ActualValue
    )
    SELECT  psvt.PointSliceID     AS PointSliceID
        ,   psvt.UTCDateTime    AS [UTCDateTime]
        ,   psvt.DigitalValue   AS ActualValue
    FROM    #tempVal AS psvt
    LEFT JOIN dbo.tblActualValueDigital AS avd
        ON avd.PointSliceID = psvt.PointSliceID
        AND avd.UTCDateTime = psvt.UTCDateTime
    WHERE   psvt.IsDigital = 1 
            AND psvt.IsActual = 1 
            AND avd.PointSliceID IS NULL;
            
    ENABLE TRIGGER dbo.trgInsertUpdatePointSlice ON dbo.tblPointSlice;            

    -- Cleanup the temp table
    EXECUTE spu_TruncatePointSliceValueTemp

    DROP TABLE #tempVal

    COMMIT TRANSACTION


  END TRY
------------------------------------------------------------------------------------------------------------------
--                      Exception Handling
------------------------------------------------------------------------------------------------------------------

  BEGIN CATCH

    --
    -- Rollback transaction if still active
    --
    IF  (XACT_STATE() <> 0)
      ROLLBACK TRANSACTION

    -- Drop the temp table, #tempVal, if it exists
    IF OBJECT_ID('tempdb..#tempVal') IS NOT NULL
    BEGIN
        DROP TABLE #tempVal
    END

    -- Cleanup the temp staging table
    EXECUTE spu_TruncatePointSliceValueTemp

    --
    -- Error raised
    --
    DECLARE             @ErrorMessage   NVARCHAR(max)
                      , @ErrorSeverity  INT
                      , @ErrorState     INT
                      , @ErrorProc      nvarchar(126)
                      , @ErrorLine      int
                      , @ErrorNumber    int
                      , @ErrorUserDef   int

    Set                 @ErrorUserDef = 50000             -- Error number if error has already be reraised
  
    -- get error info
    SELECT              @ErrorMessage   = Coalesce(ERROR_MESSAGE()  ,'NO ERROR MESSAGE')  
                      , @ErrorSeverity  = Coalesce(ERROR_SEVERITY() ,0)
                      , @ErrorState     = Coalesce(ERROR_STATE()    ,1)
                      , @ErrorProc      = Coalesce(ERROR_PROCEDURE(),'NO PROC NAME')
                      , @ErrorLine      = Coalesce(ERROR_LINE()     ,0) 
                      , @ErrorNumber    = Coalesce(ERROR_NUMBER()   ,@ErrorUserDef)

    If                  (@ErrorState = 0)   -- correct error state to allow reraise
      Set               @ErrorState = 1

    -- has error been reraised?
    If                  (@ErrorNumber = @ErrorUserDef)
    Begin 
      -- Error has been reraised already, continue to reraise              
      RAISERROR 
        (               @ErrorMessage           
                      , @ErrorSeverity 
                      , @ErrorState 
         )
    End Else Begin
      -- Error being caught for the first time
      --  add all error information to error message and reraise
      Set               @ErrorMessage
                      = N'ErrNo: ' + Cast(@ErrorNumber as nvarchar(15))  + N', '
                      + N'Proc: '  + @ErrorProc                          + N', '
                      + N'Line: '  + Cast(@ErrorLine   as nvarchar(15))    + N', '
                      + N'Error: ' + @ErrorMessage                       
      RAISERROR 
        (               @ErrorMessage           
                      , @ErrorSeverity 
                      , @ErrorState 
         )
    End
    --  return error number
    Return @ErrorNumber
  END CATCH
END
GO
/****** Object:  StoredProcedure [dbo].[spu_BulkInsertOtherDigitalSamples]    Script Date: 03/23/2015 13:36:46 ******/

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spu_BulkInsertOtherDigitalSamples]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[spu_BulkInsertOtherDigitalSamples]
GO

CREATE PROC spu_BulkInsertOtherDigitalSamples (@samples ntext)
AS
	DECLARE @hDoc int, 
		@bolOpen BIT
     
	SET @bolOpen = 0

	exec sp_xml_preparedocument @hDoc OUTPUT, @samples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END
	SET @bolOpen = 1 

	Insert Into tblOtherValueDigital(PointSliceID, UTCDateTime, OtherValue, Status, ValueCategoryId)
	Select *      
	FROM OPENXML(@hDoc, 'samples/sample', 1)  
	WITH (PointSliceID int, UTCDateTime datetime, ActualValue smallint, Status int , Category int) XMLSamples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END

ExitHandler:
	IF @bolOpen = 1 BEGIN
		EXEC sp_xml_removedocument @hDoc
	END
GO
/****** Object:  StoredProcedure [dbo].[spu_BulkInsertOtherAnalogSamples]    Script Date: 03/23/2015 13:36:46 ******/

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spu_BulkInsertOtherAnalogSamples]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[spu_BulkInsertOtherAnalogSamples]
GO

CREATE PROC spu_BulkInsertOtherAnalogSamples (@samples ntext)
AS
	DECLARE @hDoc int, 
		@bolOpen BIT
     
	SET @bolOpen = 0

	exec sp_xml_preparedocument @hDoc OUTPUT, @samples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END
	SET @bolOpen = 1 

	Insert Into tblOtherValueFloat(PointSliceID, UTCDateTime, OtherValue, Status, ValueCategoryId)
	Select *      
	FROM OPENXML(@hDoc, 'samples/sample', 1)  
	WITH (PointSliceID int, UTCDateTime datetime, ActualValue float, Status int, Category int) XMLSamples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END

ExitHandler:
	IF @bolOpen = 1 BEGIN
		EXEC sp_xml_removedocument @hDoc
	END
GO
/****** Object:  StoredProcedure [dbo].[spu_BulkInsertDigitalSamples]    Script Date: 03/23/2015 13:36:46 ******/

IF EXISTS (SELECT name FROM sysobjects WHERE name = 'spu_BulkInsertDigitalSamples')
	DROP PROCEDURE spu_BulkInsertDigitalSamples
GO

CREATE PROC spu_BulkInsertDigitalSamples (@samples ntext)
AS
	DECLARE @hDoc int, 
		@bolOpen BIT
     
	SET @bolOpen = 0

	exec sp_xml_preparedocument @hDoc OUTPUT, @samples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END
	SET @bolOpen = 1 

	Insert Into tblActualValueDigital(PointSliceID, UTCDateTime, ActualValue)
	Select *      
	FROM OPENXML(@hDoc, 'samples/sample', 1)  
	WITH (PointSliceID int  , UTCDateTime datetime, ActualValue smallint) XMLSamples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END

ExitHandler:
	IF @bolOpen = 1 BEGIN
		EXEC sp_xml_removedocument @hDoc
	END
GO

/****** Object:  StoredProcedure [dbo].[spu_BulkInsertAnalogSamples]    Script Date: 03/23/2015 13:36:46 ******/

IF EXISTS (SELECT name FROM sysobjects WHERE name = 'spu_BulkInsertAnalogSamples')
	DROP PROCEDURE spu_BulkInsertAnalogSamples
GO

CREATE PROC spu_BulkInsertAnalogSamples (@samples ntext)
AS
	DECLARE @hDoc int, 
		@bolOpen BIT
     
	SET @bolOpen = 0

	exec sp_xml_preparedocument @hDoc OUTPUT, @samples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END
	SET @bolOpen = 1 

	Insert Into tblActualValueFloat(PointSliceID, UTCDateTime, ActualValue)
	Select *      
	FROM OPENXML(@hDoc, 'samples/sample', 1)  
	WITH (PointSliceID int  , UTCDateTime datetime, ActualValue float) XMLSamples

	IF @@ERROR <> 0 BEGIN
		GOTO ExitHandler
	END

ExitHandler:
	IF @bolOpen = 1 BEGIN
		EXEC sp_xml_removedocument @hDoc
	END


GO
/****** Object:  StoredProcedure [dbo].[spStatisticalDataForOnePoint]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spStatisticalDataForOnePoint]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spStatisticalDataForOnePoint]
GO

CREATE PROCEDURE [dbo].[spStatisticalDataForOnePoint]
(
		@pointid int,
		@startdate datetime, -- Local Time
		@enddate datetime, -- Local Time
		@ReportingIntervalName nvarchar(10) = 'Month',
		@NumberOfReportingIntervals int = 1
)

AS
SET NOCOUNT ON

declare @timezoneid int

select @timezoneid=timezoneid from tblpoint where pointid=@pointid

select dbo.UTCToLocalDateTime(min(t1.ReportingPeriodStart),@timezoneId) as Start,
dbo.UTCToLocalDateTime(min(t1.ReportingPeriodEnd),@timezoneId) as "End",
avg(t2.ActualValue) as Average,
min(t2.Actualvalue) as Minimum,
max(t2.Actualvalue) as Maximum,
stDevp(t2.ActualValue) as StandardDeviation,
varp(t2.ActualValue) as Variance,
sum(t2.ActualValue) as Sum
from UTCDateTable(@startDate,@enddate,@TimeZoneId,
			@NumberOfReportingIntervals,@ReportingIntervalName) as t1
inner join tblActualValueFloat as t2 on t2.UTCDateTime >= t1.ReportingPeriodStart
				    and t2.UTCDateTime < t1.ReportingPeriodEnd
where t2.pointsliceid=(select pointsliceid from tblpointslice 
			where pointid=@pointid and israwdata=1)
and t2.UTCdatetime >= dbo.LocaltoUTCDateTime(@startdate,@timezoneid)
and t2.UTCdateTime <  dbo.LocaltoUTCDateTime(@enddate,@timezoneid)
group by t1.num
order by min(t1.ReportingPeriodStart);

	RETURN
GO
/****** Object:  StoredProcedure [dbo].[spRawDataforOnePoint]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spRawDataforOnePoint]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spRawDataforOnePoint]
GO

CREATE PROCEDURE [dbo].[spRawDataforOnePoint]

	(
		@pointid int,
		@startdate datetime, -- Local Time
		@enddate datetime -- Local Time
	)

AS
SET NOCOUNT ON

declare @timezoneid int

select @timezoneid=timezoneid from tblpoint where pointid=@pointid


select dbo.UTCToLocalDateTime(UTCdatetime,@timezoneid) as t,
Actualvalue as value
from tblActualValueFloat
where pointsliceid=(select pointsliceid from tblpointslice where pointid=@pointid and israwdata=1)
and UTCdatetime >= dbo.LocaltoUTCDateTime(@startdate,@timezoneid)
and UTCdateTime <  dbo.LocaltoUTCDateTime(@enddate,@timezoneid)
order by UTCdatetime
	RETURN
GO
/****** Object:  StoredProcedure [dbo].[spBulkInsertDigitalPoints]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spBulkInsertDigitalPoints]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spBulkInsertDigitalPoints]
GO

CREATE PROC [dbo].[spBulkInsertDigitalPoints]
  @filename nvarchar(400),
  @lowTimeMarker as datetime,
  @highTimeMarker as datetime,
  @PointSliceID int = 0
    AS
      declare @cmd nvarchar(500)
      declare @dbLowMarker datetime
      declare @dbHighMarker datetime

      -- Get the db low and high time markers
      select @dbLowMarker=LowTimeMarker,
                @dbHighMarker=HighTimeMarker
      from tblPointSlice
      where PointSliceID = @PointSliceID


      set @cmd = N'BULK INSERT tblActualValueDigital FROM ''' + @filename
      set @cmd = @cmd + N'''' + char(13) + ' WITH ( FIELDTERMINATOR='','', ROWTERMINATOR=''\n'', CHECK_CONSTRAINTS)'
      exec(@cmd)

-- The following tests are included because the table trigger does not fire during the bulk insert

      -- Remove any samples that are earlier than the bd low time marker
      IF  (@lowTimeMarker < @dbLowMarker)
        BEGIN
          delete from tblActualValueDigital 
                 where utcdatetime < @dbLowMarker and PointSliceID = @PointSliceID
          set @lowTimeMarker = @dbLowMarker
        END

      -- reset the db high time marker if required 
      IF @lowTimeMarker < @dbHighMarker
        BEGIN
          UPDATE tblPointSlice SET HighTimeMarker = @lowTimeMarker
                WHERE PointSliceID = @PointSliceID
        END
GO
/****** Object:  StoredProcedure [dbo].[spBulkInsertAnalogPoints]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spBulkInsertAnalogPoints]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spBulkInsertAnalogPoints]
GO
CREATE PROC [dbo].[spBulkInsertAnalogPoints]
  @filename nvarchar(400),
  @lowTimeMarker as datetime,
  @highTimeMarker as datetime,
  @PointSliceID int = 0
    AS
      declare @cmd nvarchar(500)
      declare @dbLowMarker datetime
      declare @dbHighMarker datetime

      -- Get the db low and high time markers
      select @dbLowMarker=LowTimeMarker,
                @dbHighMarker=HighTimeMarker
      from tblPointSlice
      where PointSliceID = @PointSliceID


      set @cmd = N'BULK INSERT tblActualValueFloat FROM ''' + @filename
      set @cmd = @cmd + N'''' + char(13) + ' WITH ( FIELDTERMINATOR='','', ROWTERMINATOR=''\n'', CHECK_CONSTRAINTS)'
      exec(@cmd)

-- The following tests are included because the table trigger does not fire during the bulk insert

      -- Remove any samples that are earlier than the bd low time marker
      IF  (@lowTimeMarker < @dbLowMarker)
        BEGIN
          delete from tblactualvaluefloat 
                 where utcdatetime < @dbLowMarker and PointSliceID = @PointSliceID
          set @lowTimeMarker = @dbLowMarker
        END

      -- reset the db high time marker if required 
      IF @lowTimeMarker < @dbHighMarker
        BEGIN
          UPDATE tblPointSlice SET HighTimeMarker = @lowTimeMarker
                WHERE PointSliceID = @PointSliceID
        END
GO
/****** Object:  StoredProcedure [dbo].[spArchiveDigitalData]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spArchiveDigitalData]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spArchiveDigitalData]
GO

CREATE PROCEDURE [dbo].[spArchiveDigitalData] 
@no_of_days int,
@bCondition int,
@AggInterval int,
@AggIntervalUOMID int

-- This date is used to archive sample data from the 
-- Historian DB into the ArchiveDest DB.
-- This process will only archive ALL slice data.
-- The low and high time markers are also adjusted as required.

AS
DECLARE @ArchiveDate  datetime
DECLARE @PointSliceID int
DECLARE @insert_error int
DECLARE @del_error int
DECLARE @update_error int

Set @ArchiveDate  = Dateadd(day, -(@no_of_days),CAST(ROUND((CAST(getdate() AS real)),0) as datetime))

if @AggInterval >0 
BEGIN																				--Aggregate pointslices with intervals
	DECLARE test_cursor CURSOR
	READ_ONLY
	FOR select PointSliceID from tblPointSlice 
	where IsRawData = @bCondition
		and RollUpInterval = @AggInterval 
		and RollUpIntervalUOMID = @AggIntervalUOMID
END
else
BEGIN
	DECLARE test_cursor CURSOR																--Raw data and All Aggregation pointslices
	READ_ONLY
	FOR select PointSliceID from tblPointSlice 
	where IsRawData = @bCondition
END

OPEN test_cursor

FETCH NEXT FROM test_cursor INTO @PointSliceID														--Fetch cursor
WHILE (@@fetch_status <> -1)																	--Fails
BEGIN
   IF (@@fetch_status <> -2)																	--Missing row
   BEGIN

   BEGIN TRAN

   print 'PointSliceID = ' + cast(@pointSliceID as char)														--Message

     -- Archive digital values
     if ((Select PointDataTypeID from tblPoint, tblPointSlice  where tblPoint.PointID = tblPointSlice.PointID
             and tblPointSlice.PointSliceID = @PointSliceID ) = (Select PointDataTypeID from tblPointDataType where PointDataTypeName = 'Digital'))
         BEGIN
         -- Move the sample data to the archive DB   
         Insert into ArchiveDest.DBO.tblActualValueDigital select * from tblActualValueDigital where utcdatetime < @ArchiveDate  and PointSliceID = @PointSliceID
         Select @insert_error = @@ERROR																--Check for error

         Delete from tblActualValueDigital where utcdatetime < @ArchiveDate and PointSliceID = @PointSliceID
         Select @del_error = @@ERROR																--Check for error

         -- Update the min/max datetime for the pointslice  
         update tblPointSlice set LowTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
         Select @update_error = @@ERROR															--Check for error

         if  (@ArchiveDate > (select HighTimeMarker from tblPointSlice where PointSliceID = @PointSliceID))
	begin
            update tblPointSlice set HighTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
            Select @update_error = @@ERROR															--Check for error
	end

         END 
   END

   FETCH NEXT FROM test_cursor INTO @PointSliceID														--Next cursor
   
   IF @insert_error = 0 AND @insert_error = 0 AND @update_error = 0 												--No errors commit transaction
       begin
       COMMIT TRAN
       end
   else																				--Errors rollback transaction
       begin
       ROLLBACK TRAN
       end

END

CLOSE test_cursor
DEALLOCATE test_cursor
GO
/****** Object:  StoredProcedure [dbo].[spArchiveData]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spArchiveData]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spArchiveData]
GO

CREATE PROCEDURE [dbo].[spArchiveData] 
@ArchiveDate datetime
-- This date is used to archive sample data from the 
-- Historian DB into the ArchiveDest DB.
-- This process will only archive ALL slice data.
-- The low and high time markers are also adjusted as required.

AS

DECLARE @PointSliceID int
DECLARE test_cursor CURSOR
READ_ONLY
FOR select PointSliceID from tblPointSlice --where IsRawData <> 0


OPEN test_cursor

FETCH NEXT FROM test_cursor INTO @PointSliceID
WHILE (@@fetch_status <> -1)
BEGIN
   IF (@@fetch_status <> -2)
   BEGIN

print 'PointSliceID = ' + cast(@pointSliceID as char)
     -- Archive digital values
     if ((Select PointDataTypeID from tblPoint, tblPointSlice  where tblPoint.PointID = tblPointSlice.PointID
             and tblPointSlice.PointSliceID = @PointSliceID ) = (Select PointDataTypeID from tblPointDataType where PointDataTypeName = 'Digital'))
         begin
         -- Move the sample data to the archive DB   
         Insert into ArchiveDest.DBO.tblActualValueDigital select * from tblActualValueDigital where utcdatetime < @ArchiveDate  and PointSliceID = @PointSliceID
         Delete from tblActualValueDigital where utcdatetime < @ArchiveDate and PointSliceID = @PointSliceID

         -- Update the min/max datetime for the pointslice  
         update tblPointSlice set LowTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
         if  (@ArchiveDate > (select HighTimeMarker from tblPointSlice where PointSliceID = @PointSliceID))
            update tblPointSlice set HighTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
         end

     -- Archive analog values
     if ((Select PointDataTypeID from tblPoint, tblPointSlice  where tblPoint.PointID = tblPointSlice.PointID
             and tblPointSlice.PointSliceID = @PointSliceID ) = (Select PointDataTypeID from tblPointDataType where PointDataTypeName = 'Analog'))
         begin
         -- Move the sample data to the archive DB   
         Insert into ArchiveDest.DBO.tblActualValueFloat select * from tblActualValueFloat where utcdatetime < @ArchiveDate and PointSliceID = @PointSliceID
         Delete from tblActualValueFloat where utcdatetime < @ArchiveDate and PointSliceID = @PointSliceID

         -- Update the min/max datetime for the pointslice  
         update tblPointSlice set LowTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
         if  (@ArchiveDate > (select HighTimeMarker from tblPointSlice where PointSliceID = @PointSliceID))
            update tblPointSlice set HighTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
         END
   
   end
   FETCH NEXT FROM test_cursor INTO @PointSliceID
END

CLOSE test_cursor
DEALLOCATE test_cursor
GO
/****** Object:  StoredProcedure [dbo].[spArchiveAnalogData]    Script Date: 03/23/2015 13:36:46 ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[spArchiveAnalogData]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
    drop procedure [dbo].[spArchiveAnalogData]
GO

CREATE PROCEDURE spArchiveAnalogData 
@no_of_days int,
@bCondition int,
@AggInterval int,
@AggIntervalUOMID int

-- This date is used to archive sample data from the 
-- Historian DB into the ArchiveDest DB.
-- This process will only archive ALL slice data.
-- The low and high time markers are also adjusted as required.

AS
Declare @ArchiveDate  datetime
DECLARE @PointSliceID int
DECLARE @insert_error int
DECLARE @del_error int
DECLARE @update_error int

Set @ArchiveDate  = Dateadd(day, -(@no_of_days),CAST(ROUND((CAST(getdate() AS real)),0) as datetime))

if @AggInterval > 0 																		--Aggregate pointslices with intervals
BEGIN
	DECLARE test_cursor CURSOR
	READ_ONLY
	FOR select PointSliceID from tblPointSlice 
	where IsRawData = @bCondition
		and RollUpInterval = @AggInterval 
		and RollUpIntervalUOMID = @AggIntervalUOMID
END
else
BEGIN
	DECLARE test_cursor CURSOR																--Raw data and All Aggregation pointslices
	READ_ONLY
	FOR select PointSliceID from tblPointSlice 
	where IsRawData = @bCondition
END

OPEN test_cursor

FETCH NEXT FROM test_cursor INTO @PointSliceID														--Fetch cursor
WHILE (@@fetch_status <> -1)																	--Fails
BEGIN
   IF (@@fetch_status <> -2)																	--Missing row
   BEGIN

   BEGIN TRAN

   print 'PointSliceID = ' + cast(@pointSliceID as char)														--Message

     -- Archive analog values
     if ((Select PointDataTypeID from tblPoint, tblPointSlice  where tblPoint.PointID = tblPointSlice.PointID
             and tblPointSlice.PointSliceID = @PointSliceID ) = (Select PointDataTypeID from tblPointDataType where PointDataTypeName = 'Analog'))
         BEGIN
         -- Move the sample data to the archive DB   
         Insert into ArchiveDest.DBO.tblActualValueFloat select * from tblActualValueFloat where utcdatetime < @ArchiveDate and PointSliceID = @PointSliceID
         Select @insert_error = @@ERROR																--Check for error

         Delete from tblActualValueFloat where utcdatetime < @ArchiveDate and PointSliceID = @PointSliceID
         Select @del_error = @@ERROR																--Check for error

         -- Update the min/max datetime for the pointslice  
         update tblPointSlice set LowTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
         Select @update_error = @@ERROR															--Check for error

         if  (@ArchiveDate > (select HighTimeMarker from tblPointSlice where PointSliceID = @PointSliceID))
	begin
            update tblPointSlice set HighTimeMarker = @ArchiveDate  where PointSliceID = @PointSliceID
            Select @update_error = @@ERROR															--Check for error
	end

         END
   END
   FETCH NEXT FROM test_cursor INTO @PointSliceID														--Next cursor

   IF @insert_error = 0 AND @insert_error = 0 AND @update_error = 0 												--No errors commit transaction
       begin
       COMMIT TRAN
       end
   else																				--Errors rollback transaction
       begin
       ROLLBACK TRAN
       end

END

CLOSE test_cursor
DEALLOCATE test_cursor  

GO
/****** Object:  StoredProcedure [dbo].[sp_InsertPoint]    Script Date: 03/23/2015 13:36:46 ******/

IF EXISTS (SELECT name FROM sysobjects WHERE name = 'sp_InsertPoint')
	DROP PROCEDURE sp_InsertPoint
GO

CREATE  PROCEDURE sp_InsertPoint
	@fqar 	 	    VARCHAR(400),
	@name		    VARCHAR(50),
	@type		    INT,
	@tz 		    INT,
	@units 		    INT,
	@enumSet	    INT,
	@PointID	    INT OUTPUT,
	@PointSliceID   INT OUTPUT
AS

    SET NOCOUNT ON

    DECLARE @UnitID             INT
    DECLARE @UofMID             INT
    DECLARE @timeZoneId         INT
    DECLARE @pointDataTypeId    INT
    DECLARE @Points             XML
    
    -- If the Unit Of Measure given does not exist, enter a dummy row for it.
    -- NOTE: We only care about storing the correct Unit Of Measure ID.
    IF NOT EXISTS (SELECT * FROM tblUnitOfMeasure WHERE UnitOfMeasureID = @units)
    BEGIN
	    --Insert a dummy row into the UnitOfMeasure table.
	    INSERT INTO tblUnitOfMeasure (UnitOfMeasureID, UnitOfMeasureName, DisplayNameShort, MeasureType)
	    VALUES (@units, 'Units ' + RTRIM(CONVERT(varchar(5), @units)), RTRIM(CONVERT(varchar(5), @units)), 'Other')

    END

    -- See if the Point is existing
    SELECT @PointID = tblPoint.[PointID]
    FROM   tblPoint
    WHERE  tblPoint.[PointName]  = CONVERT(nvarchar(400), @fqar)

    IF @PointID IS NULL 
      --we need to insert records because the FQAR is not found
      BEGIN
	    -- Insert the Point into the Point table.
	    INSERT INTO tblPoint (PointName, PointDataTypeID, TimeZoneID, UnitOfMeasureID, EnumSet)
	    VALUES (@fqar, @type, @tz, @units, @EnumSet)
    	
	    SELECT @PointID = IDENT_CURRENT('tblPoint')

	    -- Insert the Name into the Unit table.
	    INSERT INTO tblUnit (Name, UnitCategoryID, [Description])
	    VALUES (@name, 3, '(Added by Metasys III Trend)')
    	

	    SELECT @UnitID = IDENT_CURRENT('tblUnit')
    	
	    -- Insert into the PointUnit table so that the Point and Unit are related.
	    INSERT INTO tblPointUnit (PointID, UnitID)
	    VALUES (@PointID, @UnitID)

		-- Insert into the tblPointSlice table 
	    INSERT INTO tblPointSlice (PointID, IsRawData) 
	    VALUES (@PointID,'1')

	    SELECT @PointSliceID = IDENT_CURRENT('tblPointSlice')

      END
      --End of Insert
    ELSE
      -- Update existing records.
      BEGIN

      	    -- Point
  	        SELECT
                @PointDataTypeID = P.PointDataTypeID
              , @TimeZoneID = P. TimeZoneID
              , @UOfMID = P.UnitOfMeasureID
            FROM dbo.tblPoint P
            WHERE P.PointID = @PointID

            IF (@UofMId <> @units)
            BEGIN
               SET @Points = (SELECT @PointID AS [point] FOR XML RAW (N'row'), ROOT(N'root'))
               EXECUTE spu_PurgeRollupData @PointList = @Points
            END

            IF ( @TimeZoneID <> @tz OR
                 @UOfMID <> @Units OR
                 @PointDataTypeID <> @type )
                UPDATE dbo.tblPoint
                SET PointDataTypeID = @type
                  , TimeZoneID = @tz
                  , UnitOfMeasureID = @units
                  , EnumSet = @enumSet
                WHERE PointID = @PointID

            -- Unit
            SELECT
                @UnitID = UnitID
            FROM tblPointUnit
            WHERE PointID = @PointID

            -- If there's no tblPointUnit record, adding a new unit may leave
            --  an orphaned tblUnit record.
            IF ( @UnitID IS NULL )
            BEGIN

                INSERT INTO dbo.tblUnit ( Name, UnitCategoryID, [Description] )
                VALUES ( @Name, 3, N'(Added by Metasys III Trend)' )

                SELECT @UnitID = IDENT_CURRENT( 'tblUnit' )

                INSERT INTO dbo.tblPointUnit ( PointID, UnitID )
                VALUES ( @PointID, @UnitID )

            END
            ELSE
                UPDATE dbo.tblUnit
                SET [Name] = @Name
                WHERE UnitID = @UnitID
                  AND [Name] <> @Name

    		-- PointSlice
	        INSERT INTO dbo.tblPointSlice( PointID, IsRawData )
            SELECT @PointID, 1
            WHERE NOT EXISTS (  SELECT 1
                                FROM dbo.tblPointSlice
                                WHERE PointID = @PointID )

            IF (@@ROWCOUNT > 0)
                SET @PointSliceID = IDENT_CURRENT( 'tblPointSlice' )
            ELSE
                SELECT @PointSliceID = PS.PointSliceID
                FROM dbo.tblPointSlice PS
                WHERE PS.PointID = @PointID

      END
      --End of Update

COMPLETED:
GO

if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spu_GetPoints]') and OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_GetPoints]
GO

CREATE PROCEDURE spu_GetPoints
(
    @numberOfPoints AS INT
)
AS
    SET NOCOUNT ON

    IF (@numberOfPoints > 0)
    BEGIN
        SELECT TOP (@numberOfPoints) tblPoint.[PointID], tblPoint.[PointName], tblUnit.[Name], tblPoint.[PointDataTypeID],
                    tblPointSlice.[PointSliceID], tblPoint.[TimeZoneID], tblPoint.[UnitOfMeasureID], tblPoint.[EnumSet]
                    FROM tblPoint, tblPointSlice, tblUnit, tblPointUnit
                    WHERE tblPoint.[PointID] = tblPointSlice.[PointID]
                    AND tblPoint.[PointID] = tblPointUnit.[PointID] 
                    AND tblPointUnit.[UnitID] = tblUnit.[UnitID]
                    AND tblPointSlice.[IsRawData] = 1
    END
    ELSE
    BEGIN
        SELECT tblPoint.[PointID], tblPoint.[PointName], tblUnit.[Name], tblPoint.[PointDataTypeID],
                    tblPointSlice.[PointSliceID], tblPoint.[TimeZoneID], tblPoint.[UnitOfMeasureID], tblPoint.[EnumSet]
                    FROM tblPoint, tblPointSlice, tblUnit, tblPointUnit
                    WHERE tblPoint.[PointID] = tblPointSlice.[PointID]
                    AND tblPoint.[PointID] = tblPointUnit.[PointID] 
                    AND tblPointUnit.[UnitID] = tblUnit.[UnitID]
                    AND tblPointSlice.[IsRawData] = 1
    END
GO

if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spu_GetPoint]') and OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_GetPoint]
GO

CREATE PROCEDURE spu_GetPoint
(
    @FQAR AS nvarchar(400)
)
AS

    SET NOCOUNT ON

    SELECT tblPoint.[PointID], tblPoint.[PointDataTypeID],
           tblPoint.[TimeZoneID], tblPointSlice.[PointSliceID],
           tblUnit.[Name], tblPoint.[UnitOfMeasureID], tblPoint.[EnumSet]
           FROM tblPoint, tblPointSlice, tblUnit, tblPointUnit
           WHERE tblPoint.[PointName] = @FQAR
           AND tblPoint.[PointID] = tblPointSlice.[PointID]
           AND tblPoint.[PointID] = tblPointUnit.[PointID] 
           AND tblPointUnit.[UnitID] = tblUnit.[UnitID]
GO

if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spu_InsertSingleAnalogSample]') and OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_InsertSingleAnalogSample]
GO

CREATE PROCEDURE spu_InsertSingleAnalogSample
(
    @PointSliceID INT,
    @UTCDateTime DATETIME,
    @ActualValue REAL
)
AS
    SET NOCOUNT ON
    
    INSERT INTO dbo.tblActualValueFloat
    (PointSliceID, UTCDateTime, ActualValue)
    VALUES (@PointSliceID, @UTCDateTime, @ActualValue)
GO

if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spu_InsertSingleDigitalSample]') and OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_InsertSingleDigitalSample]
GO

CREATE PROCEDURE spu_InsertSingleDigitalSample
(
    @PointSliceID INT,
    @UTCDateTime DATETIME,
    @ActualValue SMALLINT
)
AS
    SET NOCOUNT ON
    
    INSERT INTO dbo.tblActualValueDigital
    (PointSliceID, UTCDateTime, ActualValue)
    VALUES (@PointSliceID, @UTCDateTime, @ActualValue)
GO

if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spu_InsertSingleDigitalSampleOther]') and OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_InsertSingleDigitalSampleOther]
GO

CREATE PROCEDURE spu_InsertSingleDigitalSampleOther
(
    @PointSliceID INT,
    @UTCDateTime DATETIME,
    @ValueCategoryID INT,
    @Status INT,
    @OtherValue SMALLINT
)
AS
    SET NOCOUNT ON
    
    INSERT INTO dbo.tblOtherValueDigital
    (PointSliceID, UTCDateTime, ValueCategoryID, Status, OtherValue)
    VALUES (@PointSliceID, @UTCDateTime, @ValueCategoryID, @Status, @OtherValue)
GO

if exists (select * from sys.objects where object_id = object_id(N'[dbo].[spu_InsertSingleAnalogSampleOther]') and OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
    drop procedure [dbo].[spu_InsertSingleAnalogSampleOther]
GO

CREATE PROCEDURE spu_InsertSingleAnalogSampleOther
(
    @PointSliceID INT,
    @UTCDateTime DATETIME,
    @ValueCategoryID INT,
    @Status INT,
    @OtherValue REAL
)
AS
    SET NOCOUNT ON
    
    INSERT INTO dbo.tblOtherValueFloat
    (PointSliceID, UTCDateTime, ValueCategoryID, Status, OtherValue)
    VALUES (@PointSliceID, @UTCDateTime, @ValueCategoryID, @Status, @OtherValue)
GO