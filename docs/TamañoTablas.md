## Tamaño de tablas

```sql
SELECT
    t.name AS tabla,
    SUM(a.total_pages) * 8 / 1024.0 AS size_mb
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY t.name
ORDER BY size_mb DESC

```
