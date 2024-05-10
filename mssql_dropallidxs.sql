DECLARE @table_name NVARCHAR(128), @index_name NVARCHAR(128), @drop_index_sql NVARCHAR(MAX);

DECLARE index_cursor CURSOR FOR
SELECT t.name AS TableName, i.name AS IndexName
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
WHERE i.type_desc = 'NONCLUSTERED';

OPEN index_cursor;

FETCH NEXT FROM index_cursor INTO @table_name, @index_name;

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @drop_index_sql = ' DROP INDEX ' + QUOTENAME(@index_name) + 'ON'+ QUOTENAME(@table_name)  + ';';
	EXEC sp_executesql @drop_index_sql;
	FETCH NEXT FROM index_cursor INTO @table_name, @index_name;
END;

CLOSE index_cursor;
DEALLOCATE index_cursor;
