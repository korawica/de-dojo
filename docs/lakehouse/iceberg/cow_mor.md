# Copy-on-Write / Merge-on-Read (CoW-MoR)

**CoW**: File rewritten after DELETE/UPDATE query
**MoR**: Create pointer file for DELETE/UPDATE query, and merge with data files during
    read.

> Delete files are created within each partition depending on the data file from
> where the record is logically deleted or updated.

```sql
ALTER TABLE table_name
SET TBLPROPERTIES (
    'write.delete.mode' =   'copy-on-write',
    'write.update.mode' =   'merge-on-read',
    'write.merge.mode'  =   'merge-on-read'
);
```
