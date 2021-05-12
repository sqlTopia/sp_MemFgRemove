CREATE OR ALTER PROCEDURE dbo.sp_MemFgRemove
(
        @DatabaseName SYSNAME
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Local helper variable
DECLARE @DbName SYSNAME = CAST(NEWID() AS NVARCHAR(36)),
        @SQL NVARCHAR(MAX);

CREATE TABLE    #Files
                (
                        type TINYINT NOT NULL,
                        file_id INT NOT NULL,
                        data_space_id INT NULL,
                        data_space_name SYSNAME NULL,
                        name SYSNAME NOT NULL,
                        file_name NVARCHAR(MAX) NOT NULL,
                        size BIGINT NULL,
                        growth BIGINT NULL,
                        rnk INT NULL,
                        command NVARCHAR(MAX) NULL
                );

SET     @SQL =  N'      SELECT          df.type,
                                        df.file_id,
                                        ds.data_space_id,
                                        ds.name AS data_space_name,
                                        df.name,
                                        df.physical_name AS file_name,
                                        df.size,
                                        df.growth
                        FROM            ' + QUOTENAME(@DatabaseName) + N'.sys.database_files AS df
                        LEFT JOIN       ' + QUOTENAME(@DatabaseName) + N'.sys.data_spaces AS ds ON ds.data_space_id = df.data_space_id;';

INSERT  #Files
        (
                type,
                file_id,
                data_space_id,
                data_space_name,
                name,
                file_name,
                size,
                growth
        )
EXEC    (@SQL);

UPDATE  #Files
SET     data_space_name =       CASE
                                        WHEN type = 0 AND data_space_name = N'PRIMARY' THEN N'ON PRIMARY '
                                        WHEN type = 0 THEN CONCAT(N'FILEGROUP ', QUOTENAME(data_space_name), N' ')
                                        WHEN type = 1 THEN N'LOG ON '
                                        ELSE QUOTENAME(data_space_name)
                                END,
        command = CONCAT(N'(NAME = N', QUOTENAME(name, N''''), N', FILENAME = N''', SUBSTRING(file_name, 1, LEN(file_name) - CHARINDEX(N'\', REVERSE(file_name))), N'\', NEWID(), N''', SIZE = 1024KB, FILEGROWTH = 1024KB)');

WITH cte
AS (
        SELECT  rnk,
                DENSE_RANK() OVER (ORDER BY CASE WHEN type = 0 THEN 0 WHEN type = 2 THEN 1 ELSE 2 END, data_space_id) AS grp
        FROM    #Files
)
UPDATE  cte
SET     rnk = grp;

-- Create dummy database with same filegroups and files (except memory optimized)
SET     @SQL = N'CREATE DATABASE ' + QUOTENAME(@DbName) + NCHAR(13) + NCHAR(10);

DECLARE @CurrID INT = 1,
        @StopID INT;

SELECT  @StopID = MAX(rnk)
FROM    #Files;

WHILE @CurrID <= @StopID
        BEGIN
                SELECT          @SQL += MIN(CASE WHEN rnk = 1 OR data_space_id IS NULL THEN N'' ELSE N',' END)
                                        + data_space_name
                                        + STRING_AGG(command, N', ') WITHIN GROUP (ORDER BY file_id)
                                        + NCHAR(13)
                                        + NCHAR(10)
                FROM            #Files
                WHERE           rnk = @CurrID
                                AND type IN (0, 1)
                GROUP BY        data_space_id,
                                data_space_name;

                SET     @CurrID += 1;
        END;

EXEC    (@SQL);

-- Detach original database
SET     @SQL = N'       ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        ALTER DATABASE ' + QUOTENAME(@DatabaseName) + ' SET MULTI_USER;
                        EXEC master.dbo.sp_detach_db @dbname = N' + QUOTENAME(@DatabaseName, N'''') + ', @skipchecks = ''true'';';

EXEC    (@SQL);

-- Change physical file names
SELECT  @SQL = STRING_AGG(CONCAT(N'ALTER DATABASE ', QUOTENAME(@DbName), N' MODIFY FILE (name = N', QUOTENAME(name, N''''), ', filename = N''', REPLACE(file_name, N'''', N''''''), N''');'), NCHAR(13) + NCHAR(10)) WITHIN GROUP (ORDER BY file_id)
FROM    #Files
WHERE   type IN (0, 1);

EXEC    (@SQL);

-- Reload dummy database with original files
SET     @SQL = N'       ALTER DATABASE ' + QUOTENAME(@DbName) + ' SET EMERGENCY;
                        ALTER DATABASE ' + QUOTENAME(@DbName) + ' SET ONLINE;';

EXEC    (@SQL);

-- Reset Service Broker GUID
SET     @SQL = N'ALTER DATABASE ' + QUOTENAME(@DbName) + N' SET NEW_BROKER WITH ROLLBACK IMMEDIATE;';

EXEC    (@SQL);

-- Remove memory optimized filegroup
SELECT  @SQL = STRING_AGG(CONCAT(N'ALTER DATABASE ', QUOTENAME(@DbName), N' REMOVE FILEGROUP ', data_space_name, N';'), NCHAR(13) + NCHAR(10))
FROM    #Files
WHERE   type = 2;

EXEC    (@SQL);

-- Rename dummy database to original name
SET     @SQL = CONCAT(N'EXEC master.sys.sp_rename @objname = N', QUOTENAME(@DbName, N''''), ', @newname = N', QUOTENAME(@DatabaseName, N''''), ', @objtype = N''DATABASE'';');

EXEC    (@SQL);
GO
