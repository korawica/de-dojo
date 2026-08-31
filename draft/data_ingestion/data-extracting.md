# Data Extracting

This section will cover way to extract data from source system to the data lake
on the **Bronze** zone.

For the data extracting, it can divide into 3 categories:

**Push-based**:

    The source system will push the data to the data lake, for example using
    CDC (Change Data Capture), batch job, or using schedule API to push the data.

**Pull-based**:

    The data platform will pull the data from the source system, for example using
    batch job or using API to pull the data.

**Steam-based**:

    The data platform will implement message broker for receive data from plublisher,
    and then the data will be processed in near real time before sink to data lake
    or comsume from consumer application.

---

## Practices to Bronze Zone

We recomend to use _Apache Spark_ for the data extracting, because it can handle
large amount of data, and inferencing schema from the source system.

!!! tip "Column Sensitivity"

    After running Spark, we should to set Spark handle the case sensitivity for
    the column name, because some source system support case sensitive column name.

    ```yaml
    spark_conf:
      spark.sql.caseSensitive: true
    ```

- Set up the source system credential like Google Service Account for GCP, user/password
  for database, or API key for API.

### Load data

1.  Load the data from ETL partition

    1. For pull-based from storage

       - Example hourly: `source/year=%Y/month=%m/day=%d/hour=%H/`

    2. For pull-based from database

       - query:
       
         ```sql
         SELECT
            *
         WHERE
                updated_at >= '{{ data_interval_start | utc | fmt('%Y-%m-%d %H:%M:%S') }}'
            AND updated_at <  '{{ data_interval_end | utc | fmt('%Y-%m-%d %H:%M:%S') }}'
         ```

2.  Deduplicate with exactly duplicate without considering business key

3.  Flatten all columns without array of struct or map of struct types.

    This is because the data lake on the Bronze zone is used for the raw data, and we want to
    keep the data as close as possible to the source system but able to track schema evolution.

    ```yaml
    schema:
      Customer:
        ID: int
        Name: string
        email: string
        address:
          Street: string
          City: string
          รหัสไปษณีย์: string
      events: >-
        array<struct<
          id: int,
          name: string,
          timestamp: timestamp
        >>
    ```

    After flattening, the schema will be:

    ```yaml
    schema:
      customer__ID: int
      customer__Name: string
      customer__email: string
      customer__address__Street: string
      customer__address__City: string
      customer__address__รหัสไปษณีย์: string
      events: >-
        array<struct<
          id: int,
          name: string,
          timestamp: timestamp
        >>
    ```
    
    !!! tip

        I recommend to use double underscore `__` as the separator for flattening,
        because it is less likely to be used in the column name, and it can be easily
        split back to the original structure if needed.

4.  Rename column to snake case but still keep ascii characters

    ```yaml
    schema:
      customer__id: int
      customer__name: string
      customer__email: string
      customer__address__street: string
      customer__address__city: string
      customer__address__รหัสไปษณีย์: string
      events: >-
        array<struct<
          id: int,
          name: string,
          timestamp: timestamp
        >>
    ```

5.  Try-casting the column with specific data type and split data to good and bad.

    Schema is provided as a DDL string (e.g. `"customer__id: int, customer__name: string"`).
    Columns not in the schema are dropped. Rows where any column fails to cast go to the bad
    dataframe with a `_cast_errors` column listing each failure as `{col, raw_value}`.

    ```python
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    def split_by_cast(
        df: DataFrame,
        schema_ddl: str,
    ) -> tuple[DataFrame, DataFrame]:
        # Parse "col: type" DDL into Spark's "col type" format
        schema = StructType.fromDDL(schema_ddl.replace(": ", " "))
        schema_fields = {f.name: f.dataType for f in schema.fields}
        target_cols = [name for name in schema_fields if name in df.columns]

        # Single pass: compute all casts alongside original columns
        cast_exprs = [
            F.try_cast(F.col(name), dtype).alias(f"__cast_{name}")
            for name, dtype in schema_fields.items()
            if name in df.columns
        ]

        # Error element: null when cast succeeds, struct when it fails
        error_elements = [
            F.when(
                F.col(name).isNotNull() & F.col(f"__cast_{name}").isNull(),
                F.struct(
                    F.lit(name).alias("col"),
                    F.col(name).cast("string").alias("raw_value"),
                ),
            )
            for name in target_cols
        ]

        # Cache before split to avoid recomputing the pipeline twice
        intermediate = (
            df.select(*[F.col(c) for c in df.columns], *cast_exprs)
            .withColumn(
                "_cast_errors",
                F.filter(F.array(*error_elements), lambda x: x.isNotNull()),
            )
            .cache()
        )

        good_df = intermediate.filter(F.size("_cast_errors") == 0).select(
            *[F.col(f"__cast_{name}").alias(name) for name in target_cols]
        )

        bad_df = intermediate.filter(F.size("_cast_errors") > 0).select(
            *[F.col(name) for name in target_cols],
            F.col("_cast_errors"),
        )

        return good_df, bad_df
    ```

    Example usage:

    ```python
    schema_ddl = """
        customer__id: int,
        customer__name: string,
        customer__email: string,
        customer__address__street: string,
        customer__address__city: string
    """

    good_df, bad_df = split_by_cast(df, schema_ddl)

    # bad_df schema (original string values + errors):
    # customer__id: string, ..., _cast_errors: array<struct<col: string, raw_value: string>>
    #
    # Example bad row:
    # customer__id="abc", _cast_errors=[{col: "customer__id", raw_value: "abc"}]
    ```
    
6.  Good data will be written to the Bronze zone, while bad data will be written
    to the Quarantine zone.

    1. Good Data

        ```python
        (
            good_df
            .withColumn("dp_partition", F.lit("{{ data_interval_start | utc | fmt('%Y-%m-%dT%H:00:00Z') }}").cast("timestamp"))
            .withColumn(
                "__audit",
                F.struct(
                    F.current_timestamp().alias("loaded_at"),
                    F.lit("{{ run_id }}").alias("loaded_by"),             
                    F.lit("source/{{ data_interval_start | utc | fmt('year=%Y/month=%m/day=%d/hour=%H') }}").alias("loaded_from"),
                )
            )
            .write.format("iceberg")
            .partitionBy("dp_partition")
            .save("gs://data-lake/bronze/customer/")
        )
        ```
       
        !!! tip "Bronze Data Type"
            
            For the Bronze zone, we recommend to use string data type for all columns,
            because it can handle any data type from the source system and it can
            be easily casted to the desired data type in the Silver zone.
    
    2. Bad Data

       ```python
       (
           bad_df
           .withColumn("dp_partition", F.lit("{{ data_interval_start | utc | fmt('%Y-%m-%dT%H:00:00Z') }}").cast("timestamp"))
           .write.format("iceberg")
           .partitionBy("dp_partition")
           .save("gs://data-lake/quarantine/customer/")
       )
       ```

   !!! tip "Iceberg Table Properties"

       For the Bronze zone, we recommend to set the following Iceberg table properties
       for better performance and cost optimization:

       ```sql
       ALTER TABLE bronze.customer
       SET TBLPROPERTIES (
           'write.format.default' = 'parquet',
           'write.parquet.compression-codec' = 'zstd',
           'write.parquet.compression-level' = '5',
           'write.parquet.enable-dictionary' = 'true',
           'write.parquet.dictionary-page-size' = '1048576',  -- 1MB
           'write.parquet.data-page-size' = '1048576',  -- 1MB
           'write.parquet.writer-version' = '2.0'
       );
       ```
