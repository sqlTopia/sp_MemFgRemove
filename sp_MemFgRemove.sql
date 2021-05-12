CREATE OR ALTER PROCEDURE dbo.sp_MemFgRemove
(
        @DatabaseName SYSNAME,
        @Debug BIT = 1
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Local helper variable
DECLARE @TemporaryDatabaseName SYSNAME = CAST(NEWID() AS NVARCHAR(36)),
        @SQL NVARCHAR(MAX);

-- Check if database exists
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name COLLATE DATABASE_DEFAULT = @DatabaseName)
        BEGIN
                RAISERROR(N'Database %s could not be found.', 16, 1, @DatabaseName);

                RETURN  -1000;
        END;

-- Check if memory tables exists
CREATE TABLE    #Tables
                (
                        table_id INT NOT NULL PRIMARY KEY CLUSTERED
                );

SET     @SQL = CONCAT(N'SELECT object_id FROM ', QUOTENAME(@DatabaseName), N'.sys.tables WHERE is_memory_optimized = 1;');

INSERT  #Tables
        (
                table_id
        )
EXEC    (@SQL);

IF EXISTS (SELECT * FROM #Tables)
        BEGIN
                RAISERROR(N'There are memory optimized tables in database %s.', 16, 1, @DatabaseName);

                RETURN  -2000;
        END;

-- Get information about files and data spaces
CREATE TABLE    #Files
                (
                        type TINYINT NOT NULL,
                        file_id INT NOT NULL,
                        data_space_type NCHAR(2) NULL,
                        data_space_name SYSNAME NULL,
                        logical_name SYSNAME NOT NULL,
                        physical_name NVARCHAR(MAX) NOT NULL,
                        size BIGINT NULL,
                        growth BIGINT NULL,
                        new_name NVARCHAR(MAX) NULL,
                        create_command NVARCHAR(MAX) NULL,
                        modify_command NVARCHAR(MAX) NULL
                );

SET     @SQL =  N'      SELECT          df.type,
                                        df.file_id,
                                        ds.type AS data_space_type,
                                        ds.name AS data_space_name,
                                        df.name AS logical_name,
                                        df.physical_name,
                                        df.size,
                                        df.growth
                        FROM            ' + QUOTENAME(@DatabaseName) + N'.sys.database_files AS df
                        LEFT JOIN       ' + QUOTENAME(@DatabaseName) + N'.sys.data_spaces AS ds ON ds.data_space_id = df.data_space_id;';

INSERT  #Files
        (
                type,
                file_id,
                data_space_type,
                data_space_name,
                logical_name,
                physical_name,
                size,
                growth
        )
EXEC    (@SQL);

IF EXISTS (SELECT * FROM #Files WHERE data_space_type = N'FD')
        BEGIN
                RAISERROR(N'There are filestream file group in database %s.', 16, 1, @DatabaseName);

                RETURN  -3000;
        END;

IF NOT EXISTS (SELECT * FROM #Files WHERE data_space_type = N'FX')
        BEGIN
                RAISERROR(N'There is no memory optimized file group in database %s.', 16, 1, @DatabaseName);

                RETURN  -4000;
        END;

-- Adjust data space name for later use
UPDATE  #Files
SET     data_space_name =       CASE
                                        WHEN type = 0 AND data_space_type = N'FG' AND data_space_name = N'PRIMARY' THEN CONCAT(N'CREATE DATABASE ' + QUOTENAME(@TemporaryDatabaseName), N' ON', NCHAR(13), NCHAR(10), N'PRIMARY ')
                                        WHEN type = 0 AND data_space_type = N'FG' THEN CONCAT(N', FILEGROUP ', QUOTENAME(data_space_name), N' ')
                                        WHEN type = 1 AND data_space_type IS NULL THEN N'LOG ON '
                                        WHEN type = 2 AND data_space_type = N'FD' THEN CONCAT(N', FILEGROUP ', QUOTENAME(data_space_name), N' CONTAINS FILESTREAM ')
                                        WHEN type = 2 AND data_space_type = N'FX' THEN QUOTENAME(data_space_name)
                                        ELSE NULL
                                END,
        new_name = CONCAT(SUBSTRING(physical_name, 1, LEN(physical_name) - CHARINDEX(N'\', REVERSE(physical_name))), N'\', NEWID());

IF EXISTS (SELECT * FROM #Files WHERE data_space_name IS NULL)
        BEGIN
                RAISERROR(N'Database %s have an unknown filegroup type.', 16, 1, @DatabaseName);

                RETURN  -5000;
        END;

-- Build commands for later use
UPDATE  #Files
SET     create_command =        CASE
                                        WHEN type = 0 THEN CONCAT(N'(NAME = N', QUOTENAME(logical_name, N''''), N', FILENAME = N''', REPLACE(new_name, N'''', N''''''), N''', SIZE = 1024KB, MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB)')
                                        WHEN type = 1 THEN CONCAT(N'(NAME = N', QUOTENAME(logical_name, N''''), N', FILENAME = N''', REPLACE(new_name, N'''', N''''''), N''', SIZE = 1024KB, MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB)')
                                        ELSE CONCAT(N'(NAME = N', QUOTENAME(logical_name, N''''), N', FILENAME = N''', REPLACE(new_name, N'''', N''''''), N''', MAXSIZE = UNLIMITED)')
                                END,
        modify_command = CONCAT(N'(NAME = N', QUOTENAME(logical_name, N''''), ', FILENAME = N''', REPLACE(physical_name, N'''', N''''''), N''');');

-- Create dummy database with same filegroups and files (except memory optimized)
SET     @SQL = CONCAT(N'CREATE DATABASE ', QUOTENAME(@TemporaryDatabaseName), NCHAR(13), NCHAR(10));

WITH cteCommands(id, data_space_type, command)
AS (
        SELECT          MIN(file_id) AS id,
                        data_space_type,
                        CONCAT(data_space_name, STRING_AGG(create_command, N', ') WITHIN GROUP (ORDER BY type, file_id)) AS command
        FROM            #Files
        WHERE           data_space_type = N'FG'
                        OR data_space_type = N'FD'
                        OR data_space_type IS NULL
        GROUP BY        data_space_type,
                        data_space_name
)
SELECT  @SQL =  STRING_AGG(Command, CONCAT(NCHAR(13), NCHAR(10))) WITHIN GROUP (ORDER BY CASE WHEN data_space_type = N'FG' THEN 1 WHEN data_space_type = N'FD' THEN 2 ELSE 3 END, id)
FROM            cteCommands;

IF @Debug = 1
        BEGIN
                PRINT   N'-- Create temporary database';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Detach original database
SET     @SQL = CONCAT(N'ALTER DATABASE ', QUOTENAME(@DatabaseName), ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;', NCHAR(13), NCHAR(10), N'ALTER DATABASE ', QUOTENAME(@DatabaseName), ' SET MULTI_USER WITH ROLLBACK IMMEDIATE;', NCHAR(13), NCHAR(10), N'EXEC master.dbo.sp_detach_db @dbname = N', QUOTENAME(@DatabaseName, N''''), ', @skipchecks = ''true'';');

IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Detach original database';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Rename physical file names
WITH cteNames(command)
AS (
        SELECT  CONCAT(N'ALTER DATABASE ', QUOTENAME(@TemporaryDatabaseName), N' MODIFY FILE ', modify_command)
        FROM    #Files
        WHERE   data_space_type = N'FG'
                OR data_space_type = N'FD'
                OR data_space_type IS NULL
)
SELECT  @SQL = STRING_AGG(Command, CONCAT(NCHAR(13), NCHAR(10)))
FROM    cteNames;

IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Rename physical file names';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Reload temporary database
SET     @SQL = CONCAT(N'ALTER DATABASE ', QUOTENAME(@TemporaryDatabaseName), ' SET EMERGENCY;', NCHAR(13), NCHAR(10), N'ALTER DATABASE ', QUOTENAME(@TemporaryDatabaseName) + ' SET ONLINE;');

IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Reload temporary database';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Reset Service Broker GUID
SET     @SQL = CONCAT(N'ALTER DATABASE ', QUOTENAME(@TemporaryDatabaseName), N' SET NEW_BROKER WITH ROLLBACK IMMEDIATE;');

IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Recreate Service Broker GUID';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Remove memory optimized filegroup
SELECT  @SQL = STRING_AGG(CONCAT(N'ALTER DATABASE ', QUOTENAME(@TemporaryDatabaseName), N' REMOVE FILEGROUP ', data_space_name, N';'), CONCAT(NCHAR(13), NCHAR(10)))
FROM    #Files
WHERE   data_space_type = N'FX';


IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Remove memory optimized filegroups';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Rename dummy database to original name
SET     @SQL = CONCAT(N'EXEC master.sys.sp_rename @objname = N', QUOTENAME(@TemporaryDatabaseName, N''''), ', @newname = N', QUOTENAME(@DatabaseName, N''''), ', @objtype = N''DATABASE'';');

IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Rename temporary database';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;

-- Clean up
WITH ctePaths(command)
AS (
        SELECT  CASE
                        WHEN data_space_type = N'FX' THEN CONCAT(N'EXEC master.sys.xp_cmdshell N''RD "', physical_name, N'" /S /Q'';')
                        WHEN data_space_type = N'FG' THEN CONCAT(N'EXEC master.sys.xp_cmdshell N''DEL "', new_name, N'" /F /Q'';')
                        ELSE CONCAT(N'EXEC master.sys.xp_cmdshell N''DEL "', new_name, N'" /F /Q'';')
                END AS command
        FROM    #Files
        WHERE   data_space_type = N'FX'
                OR data_space_type = N'FG'
                OR data_space_type IS NULL
)
SELECT  @SQL = STRING_AGG(command, CONCAT(NCHAR(13), NCHAR(10)))
FROM    ctePaths;


IF @Debug = 1
        BEGIN
                PRINT   N'';
                PRINT   N'-- Clean up';

                PRINT   @SQL;
        END;
ELSE
        BEGIN
                EXEC    (@SQL);
        END;
GO
