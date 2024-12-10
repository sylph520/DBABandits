CREATE PROCEDURE DropAllNonClusteredIndexes
AS
BEGIN
    DECLARE @tableName NVARCHAR(128)
    DECLARE @indexName NVARCHAR(128)
    DECLARE @sql NVARCHAR(MAX)

    DECLARE table_cursor CURSOR FOR
    SELECT t.name AS TableName, i.name AS IndexName
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.type_desc = 'NONCLUSTERED'

    OPEN table_cursor
    FETCH NEXT FROM table_cursor INTO @tableName, @indexName

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = 'DROP INDEX ' + QUOTENAME(@indexName) + ' ON ' + QUOTENAME(@tableName)
        EXEC sp_executesql @sql

        FETCH NEXT FROM table_cursor INTO @tableName, @indexName
    END

    CLOSE table_cursor
    DEALLOCATE table_cursor
END
