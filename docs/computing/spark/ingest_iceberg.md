# Iceberg

---

## Bronze Zone

...

---

## Silver Zone

On the silver zone, we recommend to use `MERGE` operation for writing data to the
table because it focused on the business part.

**Spark Configuration**:

```yaml
# Disable Sort-Merge Join (SMJ) to allow Spark to choose the best join strategy
spark.sql.join.preferSortMergeJoin: false

# Allow Storage-Partitioned Join (SPJ)
spark.sql.sources.v2.bucketing.enabled: true
spark.sql.requireAllClusterKeysForCoPartition: false
spark.sql.sources.v2.bucketing.pushPartValues.enabled: true
spark.sql.iceberg.planning.preserve-data-grouping: true
spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled: true

# NOTE: r7gd.4x and 12 worker nodes on r7gd.16x, totaling 4 x 12 x 16 = 768 cores
spark.sql.adaptive.advisoryPartitionSizeInBytes: 1024m  # Spark partition size after AQE
spark.sql.shuffle.partitions: 7680  # Multiple of number of cores and let AQE do its job
```

**Iceberg Table Properties**:

```sql
ALTER TABLE silver.table
SET TBLPROPERTIES (
  'write.spark.fanout.enabled' = 'true'
);
```

**References**:
- [Turbocharging Efficiency & Slashing Costs: Mastering Spark & Iceberg Joins with Storage-Partitioned](https://medium.com/expedia-group-tech/turbocharge-efficiency-slash-costs-mastering-spark-iceberg-joins-with-storage-partitioned-join-03fdc1ff75c0)
