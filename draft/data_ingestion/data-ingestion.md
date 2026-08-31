# Data Ingestion Practice

This document defines **how data moves between zones** — the patterns, rules, and steps
that apply at each zone boundary. For zone definitions, ownership, retention, and compliance
policies, see [Data Zoning Design](../data-zoning-design.md).

---

## Common Standards

The following standards apply to **every ingestion flow** regardless of source or target zone.

### Sensitive Column Review

Before any new source is ingested, DE reviews its schema during the pipeline onboarding PR and
classifies columns according to PCI DSS, PDPA, and GDPR scope.

- Column selection is stored in the pipeline's own configuration (YAML/JSON per pipeline).
- Encryption and tokenization are applied via **Apache Spark** before data moves beyond the Raw zone.
- PCI data (card numbers, CVV, track data) → **tokenized**
- PII data (name, address, national ID) → **masked or pseudonymized**
- Changes to column classification require a reviewed and approved PR.

> Raw zone is the only zone where plain sensitive data may exist. All downstream zones receive
> encrypted or masked values. See [Data Zoning Design § Compliance](../data-zoning-design.md).

### Audit Column (`_dp_audit`)

Every table in Bronze and above carries a `_dp_audit` STRUCT column. The field names differ
depending on whether the flow produces a first-insert (1-to-1) or an upsert (1-to-many) record.

| Flow Type                | Field                  | Value                                           |
|--------------------------|------------------------|-------------------------------------------------|
| **1-to-1 (insert-only)** | `loaded_at TIMESTAMP`  | Pipeline start time at first insert             |
|                          | `loaded_by STRING`     | Pipeline run ID at first insert                 |
|                          | `loaded_from STRING`   | Source file path or stream identifier           |
| **1-to-many (upsert)**   | `updated_at TIMESTAMP` | Pipeline start time at the update operation     |
|                          | `updated_by STRING`    | Pipeline run ID at the update operation         |
|                          | `updated_from STRING`  | Source file path or stream identifier at update |

Use `loaded_*` for append-only data models (transactions, CDC). Use `updated_*` wherever the
pipeline issues a MERGE and a record's non-key fields can change after the first write.

### Partition Column — `dp_partition` and `event_timestamp`

These are **two separate columns** that exist in every Bronze and Silver table:

| Column            | Value                      | Purpose                                                                     |
|-------------------|----------------------------|-----------------------------------------------------------------------------|
| `dp_partition`    | ETL pipeline start time    | Physical partition key — used only for partition pruning                    |
| `event_timestamp` | Original source event time | Business time — used in all downstream filters, aggregations, and SCD logic |

**Late-arriving data always writes to the current ETL partition** (`dp_partition`), regardless of
when the event originally occurred. Downstream Silver and Gold pipelines always use `event_timestamp`
for business-time logic, never `dp_partition`.

```text
Source record with event_time = 2026-05-20 08:00
Pipeline runs at                2026-05-22 14:00
→ dp_partition    = 2026-05-22 14:00  (physical partition written to)
→ event_timestamp = 2026-05-20 08:00  (business time preserved from source)
```

### Dataplex Registration

Dataplex automatically scans Cloud Storage and BigQuery, so new tables are discovered without
manual registration. However, DEs are responsible for two actions at source onboarding:

1. **Apply data classification tags** on sensitive columns (PCI, PII, GDPR-scoped) so Dataplex
   quality rules and data lineage reports correctly reflect sensitivity scope.
2. **Verify lineage is captured** after the first successful pipeline run — confirm that the
   zone-to-zone lineage edge appears in Dataplex before handing off the pipeline.

> Browsing the Dataplex catalog does not grant data access — all Access zone RLS/CLS controls
> remain enforced regardless of catalog visibility.

### Right-to-Erasure

> **Not yet implemented — TBD.**  
> The erasure mechanism for Iceberg tables (Bronze/Silver) and BigQuery native tables (Gold)
> has not been finalized. Candidates under evaluation:
>
> - **Iceberg row-level delete** with scheduled compaction (Bronze/Silver)
> - **Column nullification** for pseudonymization without row removal
> - **BigQuery DML `DELETE`** for Gold native tables
>
> When building pipelines that process PDPA or GDPR-scoped data, leave a clearly marked
> `TODO: erasure hook` comment at the write step so it can be wired in once the mechanism is decided.
> Raw zone data is automatically resolved by the 60-day retention policy without special action.

---

### Staging Tables

Use a staging table whenever a transformation cannot be expressed safely in a single write to the
target zone — for example, a multi-step SCD2 diff, a complex MERGE with intermediate aggregations,
or a join across multiple Bronze sources before Silver write.

**Naming:** `staging.stg_{target_zone}_{purpose}` (e.g., `staging.stg_silver_mst_customer_scd2`)

**Rules:**

- Staging tables are **truncated or dropped at the start of each pipeline run** before being
  repopulated — they never carry data from a previous run.
- Staging tables are **never exposed to end users** and must not be registered in Dataplex as
  a discovery target.
- If a pipeline fails after writing to Staging but before writing to the target zone, the next
  run's truncate step cleans up the orphaned staging data automatically.

**When to use:**

| Scenario                                   | Use Staging?                                                      |
|--------------------------------------------|-------------------------------------------------------------------|
| Simple append or partition overwrite       | No — write directly                                               |
| SCD2 diff (compare snapshot with previous) | Yes — stage the diff result before the MERGE                      |
| SCD3 MERGE with complex CASE logic         | Situational — use staging if the MERGE source needs pre-filtering |
| Multi-source join before Silver write      | Yes — join to staging first, then MERGE to Silver                 |

---

### Schema Evolution Policy

Schema checks run automatically on every pipeline execution before the write step.

| Change Type                                                          | Action                                                             |
|----------------------------------------------------------------------|--------------------------------------------------------------------|
| Add new column                                                       | **Allowed** — pipeline alerts the on-call DE before writing        |
| Safe type promotion (e.g., `STRING` → `TIMESTAMP`, `INT` → `BIGINT`) | **Allowed**                                                        |
| Breaking type change (e.g., `INT` → `STRING`)                        | **Blocked** — pipeline fails; manual review required               |
| Drop existing column                                                 | **Blocked** — pipeline fails; column must be deprecated explicitly |

### Pipeline Idempotency

Every pipeline step must be **safely re-runnable** — a second execution of the same run must
produce the same result as the first. This is non-negotiable for reliability at retail scale.

| Write Mode                          | Idempotent?                  | Why                                                                                                                                  |
|-------------------------------------|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| Overwrite partition (Raw→Bronze)    | Yes                          | Re-running overwrites the same partition; no duplication                                                                             |
| Iceberg MERGE INTO (Bronze→Silver)  | Yes — if MERGE key is stable | A second MERGE on the same key produces the same row state                                                                           |
| BigQuery MERGE (Silver→Gold)        | Yes — if MERGE key is stable | Same reasoning as above                                                                                                              |
| Truncate + load (hot native tables) | Yes                          | Truncate guarantees a clean slate each run                                                                                           |
| SCD2 insert                         | Conditional                  | Re-running after partial failure may insert a duplicate SCD2 version; use staging to pre-deduplicate the diff before the SCD2 insert |

**Recovery rule:** if a pipeline crashes after a partial write, the safest recovery is to
re-run the full pipeline from the start of that `dp_partition` window — do not attempt to
resume from the failed step. For multi-step pipelines, use the Staging pattern to ensure the
completed diff or intermediate result is fully committed before the target zone write begins.

---

### Load Mode — Initial Load vs. Incremental

Every pipeline must explicitly support two modes, controlled by a `load_mode` parameter.
Both modes use the same schema and write steps — they differ only in the source query scope.

| Mode              | When Used                                                      | Source Query Scope                                   |
|-------------------|----------------------------------------------------------------|------------------------------------------------------|
| **`initial`**     | First-time onboarding of a new source; full historical extract | Full table or full topic replay from earliest offset |
| **`incremental`** | All subsequent scheduled runs                                  | Delta only — since last successful `dp_partition`    |

**Initial load rules:**

- Run during off-peak hours to avoid competing with live pipelines on shared Spark clusters.
- Use a larger `dp_partition` granularity (daily) to reduce the number of Iceberg snapshots created.
- After initial load completes, switch `load_mode` to `incremental` before the first scheduled run.
- Validate record counts and Bronze Quarantine rates before promoting to incremental.
- For large sources (> 500 GB), load in month-by-month batches — do not attempt a single full extract.

> Never run `initial` mode against a Silver or Gold table that already has downstream consumers.
> Coordinate a maintenance window or use a shadow table (`_initial` suffix) and swap after validation.

---

### CDC Delete Propagation

Source systems emit three CDC operation types. The ingestion pipeline must handle all three —
ignoring `DELETE` causes deleted records to persist in Silver and Gold indefinitely.

| CDC Operation  | `op` Value          | Handling                                                                                                                                        |
|----------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Insert         | `I`                 | Standard MERGE `WHEN NOT MATCHED THEN INSERT`                                                                                                   |
| Update         | `U`                 | Standard MERGE `WHEN MATCHED THEN UPDATE`                                                                                                       |
| Hard Delete    | `D`                 | MERGE `WHEN MATCHED THEN DELETE` — physically removes the row from the Iceberg table                                                            |
| Soft Delete    | `is_deleted = true` | MERGE `WHEN MATCHED THEN UPDATE` — sets `is_deleted = true`, `deleted_at = event_timestamp`; row is retained but excluded from all Gold queries |

**Choosing hard vs. soft delete at the Silver layer:**

|                               | Hard Delete                             | Soft Delete                                                          |
|-------------------------------|-----------------------------------------|----------------------------------------------------------------------|
| Storage                       | Smaller — deleted rows removed          | Larger — rows retained with flag                                     |
| Historical accuracy           | Lost — cannot query "what existed at T" | Preserved — flag allows point-in-time queries                        |
| Compliance (right-to-erasure) | Simplifies PDPA/GDPR erasure            | Requires additional erasure step to nullify PII on soft-deleted rows |
| Retail use case               | Voided transactions, test orders        | Cancelled orders, deactivated products, suspended accounts           |

**Bronze layer — preserve all operations:**

Bronze always retains the raw CDC record including `op` field. Never apply hard deletes in Bronze.
This ensures Raw-to-Bronze reprocessing can reconstruct any Silver state.

**Silver MERGE with delete handling:**

```sql
MERGE INTO silver.{table} AS target
USING bronze_cdc AS incoming
ON target.{pk} = incoming.{pk}

-- Hard delete: remove the row
WHEN MATCHED AND incoming.op = 'D' THEN DELETE

-- Update: apply changes
WHEN MATCHED AND incoming.op = 'U' THEN UPDATE SET
    {non_key_fields},
    _dp_audit.updated_at   = :pipeline_start_time,
    _dp_audit.updated_by   = :pipeline_run_id,
    _dp_audit.updated_from = :source_file

-- Insert
WHEN NOT MATCHED AND incoming.op != 'D' THEN INSERT (...)
VALUES (...);
```

> For soft-delete sources, replace the `WHEN MATCHED AND op = 'D' THEN DELETE` clause with:
> `WHEN MATCHED AND incoming.is_deleted = TRUE THEN UPDATE SET is_deleted = TRUE, deleted_at = incoming.event_timestamp`.
> Gold queries must always filter `WHERE is_deleted IS NOT TRUE`.

---

## Source Ingestion Flows

These flows describe how data enters the platform from an external source.

### Source → Raw Zone

Raw zone ingestion is fully automated and managed by the **source system or sink service**.
The DE team does not operate these pipelines.

| Source Type | Cloud Storage Path | BigQuery Table |
| --- | --- | --- |
| **Kafka / Pub/Sub** | `{topic}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{topic}-{partition}-{offset}.jsonl.gz` | `{topic}(value JSON, timestamp TIMESTAMP, offset INTEGER)` |
| **Job Schedule** | `{source}/year={yyyy}/month={mm}/day={dd}/{source}-{round}-{timestamp}.csv` | `{source}(value JSON, timestamp TIMESTAMP, round INTEGER)` |

No transformation is applied. Data lands exactly as received.

> **Retention:** Raw zone data is kept for **60 days** then transitioned to cold archive storage
> automatically via a GCS lifecycle policy — no pipeline action is needed. Data beyond 60 days
> is still retrievable from archive for reprocessing but incurs retrieval latency and cost.
> See [Data Zoning Design § Raw](../data-zoning-design.md).

---

### Source → Bronze Zone (Direct Ingest)

Used when the source system is trusted and DE can apply the [Raw → Bronze](#raw--bronze) steps
directly, bypassing a separate Raw zone landing.

The same steps as [Raw → Bronze](#raw--bronze) apply in full — sensitive column review,
deduplication, flattening, audit column, schema checks, and partition strategy.

---

### Source → Shared Zone

Used for trusted cross-domain sources that do not need a DE-managed pipeline.
The Shared object is registered as `shared.{object_source_name}`.

| Object Type | Naming | When to Use |
| --- | --- | --- |
| **BigQuery Clone** | `cln_{source}` | Source owner does not want to manage access control on their side |
| **Authorized View** | `vw_{source}` | Source owner wants to retain access control on their side |

---

### Source → Raw Zone (API / Webhook)

**Owner:** Data Engineering  
**Engine:** Cloud Run Jobs (polling) or Cloud Run service (webhook receiver)

Used when the source system exposes data via a REST API or pushes events via webhook callback,
rather than publishing to Kafka / Pub/Sub.

**Two sub-patterns:**

| Sub-pattern | Trigger | Landing Path |
| --- | --- | --- |
| **Polling (pull)** | Scheduled Cloud Run Job (Airflow-triggered) | `{source}/year={yyyy}/month={mm}/day={dd}/{source}-{timestamp}-{uuid}.jsonl.gz` |
| **Webhook (push)** | HTTP POST from source system to Cloud Run service | `{source}/year={yyyy}/month={mm}/day={dd}/{source}-{event_id}.jsonl.gz` |

**Polling pattern steps:**

1. **Auth** — authenticate with the source API using a service account key stored in Secret Manager.
   Never hardcode credentials in pipeline code or config files.
2. **Paginate** — fetch pages until the API returns no `next_page_token` or equivalent cursor.
   Store the cursor or `last_ingested_at` value in a GCS state file:
   `gs://{domain}-raw-{env}/state/{source}/cursor.json`.
3. **Write** — append each page as a compressed JSONL file to the Raw zone path above.
   Use `uuid4()` as the file suffix to avoid collisions on retries.
4. **Deduplicate on retry** — if the job retries, re-written files with new UUIDs are
   deduplicated during Raw → Bronze exact-deduplication using `source_id + record_hash`.

**Webhook pattern steps:**

1. **Auth** — validate the incoming webhook signature (HMAC-SHA256 or equivalent) in the
   Cloud Run service before accepting the payload. Reject unauthenticated requests with `401`.
2. **Buffer** — the Cloud Run service writes each webhook payload to Pub/Sub, not directly
   to GCS, to avoid data loss if GCS is temporarily unavailable.
3. **Sink** — a Pub/Sub subscription sink (Dataflow or Cloud Run consumer) writes buffered
   events to the Raw zone path. From this point the pipeline follows the standard
   [Source → Raw Zone](#source--raw-zone) flow.

**`source_id` for API sources:**

| Sub-pattern | `source_id` Value |
| --- | --- |
| Polling | `{source}-{api_cursor_value}` — the unique page/batch identifier returned by the API |
| Webhook | `{source}-{webhook_event_id}` — the event ID extracted from the payload envelope |

> Never use `last_ingested_at` timestamps as `source_id` — clock skew between caller and
> receiver can cause two API responses at the "same" timestamp to collide. Always use an
> API-provided unique identifier or generate a UUID.

---

## Zone-to-Zone Flows

### Raw → Bronze

**Owner:** Data Engineering  
**Engine:** Apache Spark

This is the primary ingestion path for batch and Lambda Architecture pipelines.

**Steps:**

1. **Sensitive column review** — encrypt or tokenize columns identified during source onboarding
   (per pipeline config; see [Sensitive Column Review](#sensitive-column-review)).

2. **Exact deduplication** — remove exact duplicate records introduced by source retries.
   Dedup key: `source_id + record_hash` (hash over all payload fields).

   `source_id` identifies the exact origin of the record at the source level:

   | Source Type | `source_id` Value |
   | --- | --- |
   | Kafka / Pub/Sub | `{topic}-{partition}-{offset}` |
   | Job Schedule | `{source}-{round}-{timestamp}` (derived from the filename) |
   | Direct DB extract | `{source}-{primary_key}` |

   `record_hash` is a hash of all payload fields. Two records are considered exact duplicates
   only when both `source_id` and `record_hash` match — this prevents records with the same
   origin identifier but different payloads from being silently deduplicated.

3. **Struct flattening** — flatten nested struct fields to top-level columns.
   Arrays of structs are kept as-is (not exploded at this stage).

4. **Column rename** — rename all columns to `snake_case`.
   Source column names in non-ASCII (e.g., Thai) are retained without special characters.

5. **Audit column** — apply `_dp_audit` using the `loaded_*` variant
   (insert-only; Bronze never updates records it has already written).

6. **Schema evolution check** — validate schema change against the policy above.
   Pipeline fails and alerts on-call DE for any blocked change type.

7. **Write** — overwrite the current `dp_partition` using Iceberg's overwrite-partition mode.
   Partition scope is set by pipeline schedule frequency:
   - Hourly pipelines: `hours(dp_partition)`
   - Daily pipelines: `days(dp_partition)`

8. **BigLake registration** — create or refresh the BigLake external table over the Iceberg files
   with `require_partition_filter = true`.

> Records that fail schema or structural checks (missing required fields, type mismatches) are routed
> to the **Quarantine zone** and never silently dropped.
> See [Data Zoning Design § Quarantine](../data-zoning-design.md).

#### Late Data Window

By default, the Raw → Bronze pipeline writes all records to the current `dp_partition` regardless
of their `event_timestamp`. The **late data window** defines how far back Silver pipelines must
scan Bronze to catch records whose `event_timestamp` falls behind the current partition.

**Late data SLA by source type:**

| Source Type | Expected Delivery Latency | Late Data Window |
| --- | --- | --- |
| Kafka / Pub/Sub (real-time) | < 5 min | 2 hours |
| CDC (Debezium) | < 15 min | 4 hours |
| API Polling | < 1 hour | 1 day |
| Job Schedule (batch) | < 2 hours | 1 day |
| Webhook | < 2 hours | 1 day |

Set the window per pipeline in pipeline config: `late_data_window_hours`. Silver pipelines use
this value to compute the Bronze read range:

```python
bronze_read_start = pipeline_start_time - timedelta(hours=late_data_window_hours)
```

If a record arrives later than its configured window (e.g., a 3-day-old event on a source with
a 1-day window), the pipeline routes it to the **Quarantine zone** with reason
`LATE_ARRIVAL_EXCEEDED`. This prevents Silver and Gold from being silently contaminated by
ancient out-of-order data.

> For SCD2 pipelines, late arrivals within the window produce retroactive version inserts. This
> is expected — the `valid_from` / `valid_to` range will correctly represent the historical state.

#### Iceberg Compaction

Raw → Bronze writes use Iceberg's overwrite-partition mode, which produces one or more Parquet
files per pipeline run per partition. Frequent small writes accumulate into many small files
that degrade read performance ("small file problem"). A dedicated compaction job consolidates
these into optimally-sized files.

**Compaction schedule:**

| Bronze Table Type | Compaction Frequency | Target File Size |
| --- | --- | --- |
| Hourly pipeline | Daily, after the batch window closes | 256 MB |
| Daily pipeline | Weekly | 512 MB |
| High-frequency streaming checkpoint | Every 6 hours | 128 MB |

Compaction is a **separate Spark job** — it must never run concurrently with an active Raw →
Bronze write on the same partition. Use Airflow sensor dependencies or Iceberg's lock manager
to prevent concurrent writes.

```python
spark.sql(f"""
    CALL {catalog}.system.rewrite_data_files(
        table   => '{database}.{table}',
        strategy => 'binpack',
        options  => map(
            'target-file-size-bytes',             '{target_size_bytes}',
            'min-file-size-bytes',                '{int(target_size_bytes * 0.75)}',
            'max-concurrent-file-group-rewrites', '5'
        ),
        where => "dp_partition >= '{compaction_start}'"
    )
""")
```

After compaction, expire orphaned snapshot metadata older than the retention window:

```python
spark.sql(f"""
    CALL {catalog}.system.expire_snapshots(
        table       => '{database}.{table}',
        older_than  => TIMESTAMP '{expire_before}',
        retain_last => 2
    )
""")
```

> Never set `retain_last < 2`. Keeping at least two snapshots allows the pipeline to roll back
> one version on a failed run without losing the previous stable state.

---

### Raw → Gold (Lambda Hot Path)

**Owner:** Data Engineering  
**Engine:** BigQuery SQL

Used for the **hot data layer** in a Lambda Architecture — delivers near-real-time data to Gold
while the Bronze batch pipeline processes the cold (historical) layer.

**Steps:**

1. **Sensitive column review** — same as Raw → Bronze.

2. **BigLake external table** — create an external table directly over the raw Cloud Storage files
   using a BigLake connection. Named `raw.ext_{source}_curr_{freq}`.

    ```sql
    CREATE OR REPLACE EXTERNAL TABLE
      `{project}.raw.ext_{source}_curr_hour`
    (
      `timestamp` TIMESTAMP,
      `value`     JSON,
      `key`       STRING,
      `offset`    INTEGER
    )
    CONNECTION `{project}.{region}.{connection_name}`
    OPTIONS (
      format = 'JSON',
      uris   = ['gs://{bucket}/{source}/year=yyyy/month=mm/day=dd/hour=hh/*.jsonl.gz']
    );
    ```

3. **Hot native table** — load a BigQuery native table from the external table using truncate.
   Keeps only the current window (hour or day). Naming: `{source}_curr_hour` or `{source}_curr_day`.

    Optionally maintain a `ref_pk_{source}` table containing only the primary keys of the
    current hot window — this is used in step 5 to exclude hot records from the cold query.

4. **Audit column** — apply `_dp_audit` using the `loaded_*` variant.

5. **Materialized view** — combine cold (Gold batch table) and hot (current window) data into a
   single `mvw_` view. The view is intentionally not auto-refreshed (`enable_refresh = false`)
   so it is queried on demand at the latest state.

    Two deduplication strategies are available. Use **Qualify** when both cold and hot datasets
    share the same schema and a single pass is acceptable. Use **Left Join** when the hot dataset
    has a different projection from cold, or you need to minimize the cold scan.

<!-- markdownlint-disable MD046 -->
    === "Qualify Option"

        ```sql
        CREATE OR REPLACE MATERIALIZED VIEW
          `{project}.gold.mvw_{usecase}_prev_day_to_curr_hour`
        CLUSTER BY {search_keys}
        OPTIONS (enable_refresh = false)
        AS
        WITH cold AS (
          SELECT *
          FROM `{project}.gold.{target_table}`
          WHERE {partition_key} >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        )
        SELECT *
        FROM (
          SELECT * FROM `{project}.raw.ext_{source}_curr_hour`  -- hot
          UNION ALL
          SELECT * FROM cold                                     -- cold
        )
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY {primary_keys}
          ORDER BY event_timestamp DESC
        ) = 1;
        ```

    === "Left Join Option"

        ```sql
        CREATE OR REPLACE MATERIALIZED VIEW
          `{project}.gold.mvw_{usecase}_prev_day_to_curr_hour`
        CLUSTER BY {search_keys}
        OPTIONS (enable_refresh = false)
        AS
        WITH cold AS (
          SELECT *
          FROM `{project}.gold.{target_table}`
          WHERE {partition_key} >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        )
        -- Hot records (current window)
        SELECT * FROM `{project}.raw.ext_{source}_curr_hour`
        UNION ALL
        -- Cold records not already covered by the hot window
        SELECT c.*
        FROM cold AS c
          LEFT JOIN `{project}.gold.ref_pk_{source}` AS ref USING (pk)
        WHERE ref.pk IS NULL;
        ```
<!-- markdownlint-enable MD046 -->

---

### Bronze → Silver

**Owner:** Data Engineering + Analytics Engineering  
**Engine:** Apache Spark (Iceberg MERGE INTO)

This is the **core transformation step** where data acquires business semantics.
All column renames, business-key deduplication, and data model shaping happen here.

**Steps:**

1. **Audit column** — apply `_dp_audit`:
   - Insert-only models → `loaded_*` variant
   - Upsert models → `updated_*` variant

2. **Business-key deduplication** — after Bronze's exact-dedup, Silver applies a second pass
   using the business primary key. Keep the record with the latest `event_timestamp`;
   superseded duplicates are routed to Quarantine with reason `DUPLICATE_BUSINESS_KEY`.

3. **Write** — Spark issues an Iceberg MERGE INTO using the business primary key.

4. **BigLake registration** — create or refresh the BigLake external table with
   `require_partition_filter = true`.

**Data Models:**

| Model | Silver Prefix | Write Mode | Partition Strategy | Notes |
| --- | --- | --- | --- | --- |
| **Pure Transaction** (no mutable fields) | `txn_` | Upsert by transaction ID | `hours(created_at)` or `hours(last_modified_date)` | Records are immutable once written |
| **Transaction with Update Key** | `txn_` | Upsert by transaction ID + `updated_at` / `last_modified_date` | `hours(created_at)` | Use update timestamp to resolve latest version |
| **Master Data** | `mst_` | Upsert by entity key | None required | Flat current-state table |
| **Master Data (SCD2)** | `mst_{name}_scd2` | SCD2 via hash of concatenated attribute columns | `days(valid_from)` | Carries `valid_from`, `valid_to`, `is_current`; see SCD2 trigger rules below |
| **State Data** | `stt_` | Upsert by entity key | Bucket or none on main key (e.g., `branch_code`) | Plain current-state; no history needed |
| **State Data (SCD3)** | `stt_` | Upsert with compare-on-merge | Bucket or none on main key | Tracks current and one previous value per designated attribute; see below |
| **Snapshot Data** | `snp_` | Append-only insert with `snapshot_date` | `days(snapshot_date)` or `months(snapshot_date)` | Point-in-time full extract; all history retained as immutable rows; use `prd_` instead when records must be mutable within a period |
| **Period Snapshot** | `prd_` | Upsert by (entity key + `period_month`) while open; rejected after close | `months(period_month)` | Mutable within the period, immutable after period close; see period snapshot rules below |
| **Accumulating Snapshot** | `acc_` | Upsert in-place by process entity key; milestone timestamps set once (first-write-wins) | None or `months({creation_stage}_at)` | Tracks lifecycle stage milestones on a single row; see accumulating snapshot rules below |
| **Exploded Child** | `{parent_prefix}_{child}` | Append-only insert or upsert by (parent key + child key); written in the same pipeline step as the parent | Same as parent table | Produced by exploding a Bronze array-of-structs into child rows; parent delete must cascade; see exploded child rules below |
| **CDC Data** | `cdc_` | Upsert; include `updated_at` in primary key | None | Retain all change records |

#### Master Data with SCD2 — Trigger Rules

`mst_{name}_scd2` tables are populated by one of two Bronze input types. The choice is made at
source onboarding and determines how the pipeline detects a change.

| Bronze Input | Silver Trigger | How Change Is Detected |
| --- | --- | --- |
| `cdc_{source}` | New CDC record arrives with changed fields | Pipeline reads CDC `op = UPDATE` records; each changed row inserts a new SCD2 version — no diff required |
| `snp_{source}` | Full snapshot lands each run | Pipeline compares current snapshot against the previous version using an attribute hash; rows where the hash differs produce a new SCD2 version |

**SCD2 column standard** (both trigger types use the same output schema):

| Column | Type | Description |
| --- | --- | --- |
| `valid_from` | TIMESTAMP | `event_timestamp` when this version became active |
| `valid_to` | TIMESTAMP | `event_timestamp` when superseded; `NULL` = current version |
| `is_current` | BOOLEAN | `true` for the active record |

> When using the snapshot-diff trigger, always hash only business attribute columns — exclude
> audit columns and `dp_partition` from the hash to avoid false-positive change detection.

---

#### State Data with SCD3

Use SCD3 when the consumer needs to know "what changed from" without the full row-history
overhead of SCD2. The pattern tracks **one level of history** per designated attribute.

**Column Triple (per tracked attribute):**

| Column | Type | On First Insert | On Subsequent Change |
| --- | --- | --- | --- |
| `curr_{attr}` | source type | Incoming value | Incoming value |
| `prev_{attr}` | source type | `NULL` | Previous `curr_{attr}` |
| `{attr}_changed_at` | TIMESTAMP | `NULL` | `event_timestamp` of the change |

Only columns explicitly designated as "tracked" during source onboarding get this triple.
All other columns on the entity remain as plain fields.

**MERGE Logic:**

The MERGE fires when any tracked column changes (`IS DISTINCT FROM` handles NULL comparisons).
Each `{attr}_changed_at` updates only when its own column's value changes — use a CASE per column.

```sql
MERGE INTO silver.stt_{entity} AS target
USING source AS incoming
ON target.{pk} = incoming.{pk}

WHEN MATCHED AND (
    incoming.{attr_1} IS DISTINCT FROM target.curr_{attr_1}
 OR incoming.{attr_2} IS DISTINCT FROM target.curr_{attr_2}
) THEN UPDATE SET
    prev_{attr_1}       = target.curr_{attr_1},
    curr_{attr_1}       = incoming.{attr_1},
    {attr_1}_changed_at = CASE
                            WHEN incoming.{attr_1} IS DISTINCT FROM target.curr_{attr_1}
                            THEN incoming.event_timestamp
                            ELSE target.{attr_1}_changed_at
                          END,

    prev_{attr_2}       = target.curr_{attr_2},
    curr_{attr_2}       = incoming.{attr_2},
    {attr_2}_changed_at = CASE
                            WHEN incoming.{attr_2} IS DISTINCT FROM target.curr_{attr_2}
                            THEN incoming.event_timestamp
                            ELSE target.{attr_2}_changed_at
                          END,

    _dp_audit.updated_at   = :pipeline_start_time,
    _dp_audit.updated_by   = :pipeline_run_id,
    _dp_audit.updated_from = :source_file

WHEN NOT MATCHED THEN INSERT (
    {pk},
    curr_{attr_1}, prev_{attr_1}, {attr_1}_changed_at,
    curr_{attr_2}, prev_{attr_2}, {attr_2}_changed_at,
    _dp_audit
) VALUES (
    incoming.{pk},
    incoming.{attr_1}, NULL, NULL,
    incoming.{attr_2}, NULL, NULL,
    struct(:pipeline_start_time, :pipeline_run_id, :source_file)
);
```

**When to choose SCD3 over SCD2:**

| | SCD2 | SCD3 |
| --- | --- | --- |
| History depth | Full (all versions as rows) | One previous value per column |
| Query pattern | "Show me all states over time" | "Show me what changed most recently" |
| Storage cost | Grows with every change | Fixed — two extra columns per tracked attr |
| Typical use | Master data (customer, product) | State data (order status, assignment) |

> For Clustering: apply Z-order on columns matching the Gold zone's expected `JOIN`,
> `WHERE`, and `GROUP BY` patterns. Analytics Engineers define these per use case.

---

#### Period Snapshot (`prd_`)

Use the Period Snapshot model when records belong to a discrete business period (month, fiscal
week, quarter), can be updated freely **within** the open period, and must be frozen once the
period closes.

**Retail examples:**

| Use Case | Period Granularity | What Changes During Period |
| --- | --- | --- |
| Monthly installment payment schedule | Month | Amount, due dates, status revised by finance |
| Monthly promotional allocation list | Month | SKU × branch × promotion reassigned |
| Monthly store operational targets | Month | Target quantities adjusted before month start |
| Monthly planogram assignment | Month | Store layout revised until execution date |
| Periodic supplier price list | Month / Quarter | Prices negotiated until contract lock |

This differs from `snp_` (point-in-time append — no mutation allowed after write) and `mst_`
(single current-state row — no period retention).

**Required columns (in addition to entity key):**

| Column | Type | Description |
| --- | --- | --- |
| `period_month` | DATE | First day of the period — always `DATE_TRUNC(event_date, MONTH)` |
| `is_period_closed` | BOOLEAN | `false` while the period is open; `true` after period close |
| `period_closed_at` | TIMESTAMP | `NULL` until closed; pipeline start time at close |

**Partition strategy:** `months(period_month)`

**Write mode:** Upsert by (entity key + `period_month`) while open; updates to closed periods
are routed to Quarantine with reason `PERIOD_ALREADY_CLOSED`.

**`update` mode MERGE (normal incremental run):**

```sql
MERGE INTO silver.prd_{entity} AS target
USING incoming AS source
ON  target.{pk}           = source.{pk}
AND target.period_month   = source.period_month

-- Reject updates to closed periods; pipeline routes record to Quarantine
WHEN MATCHED AND target.is_period_closed = TRUE THEN
    -- no-op here; the pipeline pre-filters closed records to Quarantine before the MERGE

-- Update open-period record
WHEN MATCHED AND target.is_period_closed = FALSE THEN UPDATE SET
    {non_key_fields},
    _dp_audit.updated_at   = :pipeline_start_time,
    _dp_audit.updated_by   = :pipeline_run_id,
    _dp_audit.updated_from = :source_file

-- Insert new record for this period
WHEN NOT MATCHED THEN INSERT (
    {pk}, period_month, is_period_closed, period_closed_at, {non_key_fields}, _dp_audit
) VALUES (
    source.{pk}, source.period_month, FALSE, NULL, {source_non_key_fields},
    struct(:pipeline_start_time, :pipeline_run_id, :source_file)
);
```

**`close` mode — period close step (run once at period end):**

```sql
UPDATE silver.prd_{entity}
SET
    is_period_closed       = TRUE,
    period_closed_at       = :pipeline_start_time,
    _dp_audit.updated_at   = :pipeline_start_time,
    _dp_audit.updated_by   = :pipeline_run_id
WHERE
    period_month        = :closing_period_month
    AND is_period_closed = FALSE;
```

**Rules:**

- The period close job must be a **dedicated Airflow task** with a sensor confirming the last
  update run for that period has completed before the close step fires.
- After closure, any source update for a closed `period_month` goes to **Quarantine** with
  reason `PERIOD_ALREADY_CLOSED` — never silently discard; it may indicate a source data issue.
- `period_month` must always be the first day of the month
  (`DATE_TRUNC(event_date, MONTH)`), not the actual event date, for clean partition alignment.
- For sub-monthly periods (fiscal week, 10-day cycle), replace `period_month` with
  `period_start DATE` and adjust the partition strategy to `days(period_start)`.
- If the source delivers a **full replacement** of the period's records each run (not a delta),
  use a staging table to diff against the previous write before issuing the MERGE — prevents
  false deletes of records not present in the latest batch.

**Gold query patterns:**

| Query intent | Filter |
| --- | --- |
| Closed-period snapshot (e.g., April final) | `WHERE period_month = '2026-04-01' AND is_period_closed = TRUE` |
| Current live period | `WHERE period_month = DATE_TRUNC(CURRENT_DATE(), MONTH) AND is_period_closed = FALSE` |
| Period-over-period trend | `GROUP BY period_month ORDER BY period_month` |

> When building a Gold fact or mart from a `prd_` Silver table, always join dimensions using
> `period_month` — never use `CURRENT_DATE()`. This keeps closed-period snapshots stable and
> reproducible across report refreshes.

---

#### Accumulating Snapshot (`acc_`)

Use the Accumulating Snapshot model when tracking a business **process** through a defined
sequence of lifecycle stages. Each process entity has **one row**, and the pipeline adds or
updates milestone timestamps as the process advances through stages.

**Retail examples:**

| Use Case | Lifecycle Stages |
| --- | --- |
| Order fulfillment | `ordered → allocated → picked → packed → dispatched → delivered → returned` |
| Goods receiving | `po_created → arrived → inspected → stocked` |
| Installment application | `applied → scored → approved → disbursed → active → settled` |
| Promotion campaign | `planned → approved → live → ended → evaluated` |

This is distinct from SCD3:
- **SCD3** tracks *which value changed* on a mutable attribute (e.g., order status changed from
  `PENDING` to `SHIPPED`).
- **Accumulating Snapshot** records *when each stage was first reached*, as immutable timestamps
  on the same row — durations between stages (`dispatched_at - ordered_at`) are the primary
  analytical value.

**Required columns (in addition to entity key):**

For each stage in the lifecycle, add a milestone pair:

| Column | Type | Value |
| --- | --- | --- |
| `{stage}_at` | TIMESTAMP | Pipeline start time when this stage was first recorded; `NULL` if not yet reached |
| `{stage}_by` | STRING | Pipeline run ID at the stage write (for lineage) |

Add a summary column for the current position in the lifecycle:

| Column | Type | Value |
| --- | --- | --- |
| `current_stage` | STRING | Name of the latest reached stage (`'dispatched'`, `'delivered'`, etc.) |

**MERGE Logic (first-write-wins on milestone columns):**

```sql
MERGE INTO silver.acc_{entity} AS target
USING incoming AS source
ON target.{pk} = source.{pk}

WHEN MATCHED THEN UPDATE SET
    -- Milestone columns: set once, never overwritten (COALESCE preserves first value)
    ordered_at      = COALESCE(target.ordered_at,    source.ordered_at),
    ordered_by      = COALESCE(target.ordered_by,    source.ordered_by),
    allocated_at    = COALESCE(target.allocated_at,  source.allocated_at),
    allocated_by    = COALESCE(target.allocated_by,  source.allocated_by),
    dispatched_at   = COALESCE(target.dispatched_at, source.dispatched_at),
    dispatched_by   = COALESCE(target.dispatched_by, source.dispatched_by),
    delivered_at    = COALESCE(target.delivered_at,  source.delivered_at),
    delivered_by    = COALESCE(target.delivered_by,  source.delivered_by),
    returned_at     = COALESCE(target.returned_at,   source.returned_at),
    returned_by     = COALESCE(target.returned_by,   source.returned_by),
    -- Current stage always reflects the latest known position
    current_stage   = source.current_stage,
    _dp_audit.updated_at   = :pipeline_start_time,
    _dp_audit.updated_by   = :pipeline_run_id,
    _dp_audit.updated_from = :source_file

WHEN NOT MATCHED THEN INSERT (
    {pk},
    ordered_at, ordered_by,
    allocated_at, allocated_by,
    dispatched_at, dispatched_by,
    delivered_at, delivered_by,
    returned_at, returned_by,
    current_stage,
    _dp_audit
) VALUES (
    source.{pk},
    source.ordered_at, source.ordered_by,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    source.current_stage,
    struct(:pipeline_start_time, :pipeline_run_id, :source_file)
);
```

**Rules:**

- All milestone columns use `COALESCE(target.col, source.col)` — once a timestamp is set, it is
  permanent. If a stage must be correctable, file a backfill request; do not overwrite inline.
- `current_stage` is the **only mutable summary column** — it always reflects the latest stage
  sent by the source.
- If a process stage can be **re-entered** (e.g., a returned item that is re-dispatched), do not
  use Accumulating Snapshot. Use SCD2 or a `txn_` model with a stage key instead.
- Partition on `months({creation_stage}_at)` only when the table is large (> 100M rows) and
  queries reliably filter by the creation date.

**When to choose:**

| | SCD3 | Accumulating Snapshot |
| --- | --- | --- |
| Models | Attribute value change | Process stage progression |
| History kept | One previous value per column | All stage timestamps (immutable) |
| Row count | One per entity | One per entity |
| Primary query | "What changed to what?" | "How long did each stage take?" |
| Typical use | Order status, store assignment | Fulfillment lifecycle, application workflow |

---

#### Exploded Child Table (`{parent_prefix}_{child}`)

Use the Exploded Child model when a Bronze source contains an **array of structs** that must be
normalised to individual rows in Silver. The Bronze step intentionally preserves arrays as-is;
the Silver step is where explosion happens.

**Retail examples:**

| Parent Table | Child Table | Array Column Exploded |
| --- | --- | --- |
| `txn_order` | `txn_order_line` | `line_items[]` — one row per SKU in the order |
| `txn_receipt` | `txn_receipt_payment` | `payments[]` — one row per payment method |
| `mst_product` | `mst_product_barcode` | `barcodes[]` — one row per barcode / EAN |
| `snp_promotion` | `snp_promotion_sku` | `eligible_skus[]` — one row per eligible product |

**Primary key:** always a composite of `(parent_pk + child_key)`. The `child_key` is either:
- An explicit field from the struct (e.g., `line_no`, `barcode`), or
- An array index (`_pos INTEGER`) generated during explosion when the struct has no natural key.

Prefer an explicit field — positional indexes are fragile if the source reorders array elements.

**Explosion in Spark:**

```python
from pyspark.sql import functions as F

child_df = (
    parent_bronze_df
    .select(
        F.col("{parent_pk}"),
        F.posexplode(F.col("{array_col}")).alias("_pos", "_item")
    )
    .select(
        F.col("{parent_pk}"),
        F.col("_item.{child_key}"),
        F.col("_item.{field_1}"),
        F.col("_item.{field_2}"),
    )
)
```

**CDC delete cascade:**

When the parent record is hard-deleted, all its child rows must also be deleted. The Silver MERGE
for the child table must handle the parent's `op = 'D'` CDC record:

```sql
-- In the child MERGE, match on parent_pk alone to cascade parent deletes
WHEN MATCHED AND incoming_parent.op = 'D' THEN DELETE
```

For soft-delete parents, set `is_deleted = TRUE` on all child rows where `parent_pk` matches
the soft-deleted parent.

**Rules:**

- Always write parent and child tables in the **same pipeline transaction** (same Spark job,
  sequential writes). Never write child rows without first confirming the parent write succeeded.
- Never explode arrays in Bronze — keep them as-is. Explosion in Bronze prevents reliable
  reprocessing because array-indexed child rows cannot be cleanly merged on replay.
- If the same array element can appear in multiple parent records (e.g., a shared barcode),
  model it as a bridge table (`brd_`) at Gold instead of an exploded child at Silver.
- Partition the child table identically to the parent table so that partition pruning works
  consistently when joining parent to child.

---

### Bronze → Gold (Trusted Reference Shortcut)

**Owner:** Data Engineering  
**Engine:** BigQuery SQL or Spark

Used when the source data is already business-ready (e.g., a reference table, a lookup table, or
a report pre-built by the source system) and no business-rule transformation is needed in Silver.

Apply the same audit column and BigLake registration steps as Bronze → Silver.
Label the target table with the appropriate Gold prefix (`ref_`, `dim_`, etc.).

---

### Bronze/Silver → Kappa Checkpoint

**Owner:** Data Engineering (Kappa pipelines)  
**Engine:** Spark StructuredStreaming

Used when a **Kappa Architecture** pipeline needs to checkpoint its state at an intermediate zone
before writing to the final target. The pipeline reads from a streaming source (Kafka, Pub/Sub)
without schema inference and writes directly to the target zone.

Checkpoint state is stored at:

```text
gs://{domain}-{zone}-{env}{hash}/checkpoint/{source}
```

The same deduplication, audit column, and schema standards above apply at whichever zone
the checkpoint writes to.

#### Watermark Policy

Kappa pipelines process an unbounded stream of events. A **watermark** defines the point in
event-time beyond which the pipeline considers late data unlikely to arrive, allowing Spark
StructuredStreaming to finalise windows and emit results.

```python
stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic)
    .load()
    .withWatermark("event_timestamp", watermark_delay)
)
```

**Recommended watermark delay by source type:**

| Source Type | Watermark Delay | Rationale |
| --- | --- | --- |
| Pub/Sub (real-time POS) | `10 minutes` | Network jitter; short delivery guarantee |
| Kafka (high-throughput inventory) | `30 minutes` | Consumer lag under peak retail load |
| Webhook (external partner) | `2 hours` | Partner retries and delivery uncertainty |
| CDC (Debezium, MySQL) | `15 minutes` | Replication lag on primary → replica |

**Rules:**

- Set the watermark delay to **2× the 99th-percentile source latency**, measured over a 30-day
  rolling window. Review and adjust quarterly or after any source infrastructure change.
- If the watermark is too tight, late records are **silently dropped** by Spark StructuredStreaming.
  Monitor `numDroppedDuplicates` and `numLateRecordsDropped` streaming metrics in Cloud Monitoring.
- Emit watermark value as a custom metric (`dp_stream_watermark_lag_seconds`) and alert when the
  watermark stalls — this indicates the stream has halted or is severely lagged.
- The checkpoint stores the latest committed watermark alongside Kafka offsets. A pipeline restarted
  from checkpoint resumes from the last committed watermark — it does **not** reprocess already-
  committed event-time windows. To reprocess a committed window, use the [Backfill](#backfill)
  pattern below.

---

### Backfill

**Owner:** Data Engineering  
**Engine:** Apache Spark (batch) or Spark StructuredStreaming (replay)

A backfill is a deliberate reprocessing of a historical time range. It is triggered when:

- A bug in transformation logic produced incorrect downstream data.
- A new column or business rule must be applied retroactively.
- A source system replayed events from an earlier period.
- A Kappa pipeline's committed watermark window must be corrected.

**Backfill types:**

| Type | When to Use | Approach |
| --- | --- | --- |
| **Partition rewrite** | Raw → Bronze; incorrect data in one or more `dp_partition` windows | Drop and re-create affected Iceberg partitions using `load_mode=initial` scoped to the target date range |
| **Shadow table backfill** | Bronze → Silver or Silver → Gold; downstream consumers are live | Write to a `_backfill`-suffixed table, validate, then atomically swap (rename) |
| **Kappa replay** | Kappa pipeline committed incorrect windows | Reset checkpoint and replay Kafka from the target offset range in batch mode |
| **Full historical reload** | New source onboarding with > 12 months of history | Follow `load_mode=initial` rules; use month-by-month batches |

**Standard backfill runbook:**

1. **Scope** — identify the exact `dp_partition` range and affected tables. Confirm no active
   pipelines are writing to those partitions.
2. **Communicate** — post to `#data-platform` with affected tables, window, and ETA. Tag
   downstream AE/consumers so they know reports may be inconsistent during the window.
3. **Pause pipelines** — disable the Airflow DAG for the affected pipeline to prevent new
   incremental runs from overlapping with the backfill.
4. **Execute** — run the pipeline with:
   ```
   load_mode=initial
   backfill_start=<yyyy-mm-dd>
   backfill_end=<yyyy-mm-dd>
   ```
   Use a larger Spark cluster than the standard incremental run — backfills should complete
   within a bounded time window.
5. **Validate** — compare row counts and key metrics against expected values before resuming
   downstream pipelines. Check the Quarantine zone for unexpected error rates.
6. **Resume** — re-enable the Airflow DAG. Verify the first incremental run picks up correctly
   from the last `dp_partition` of the backfill range.
7. **Log** — record the backfill in the pipeline oncall log with: trigger reason, date range,
   rows affected, and validation outcome.

**Kappa checkpoint reset:**

```bash
# 1. Stop the streaming job first
# 2. Delete the checkpoint directory
gsutil -m rm -r gs://{domain}-{zone}-{env}{hash}/checkpoint/{source}

# 3. Identify the Kafka offset for the replay start time using the broker tool
# 4. Restart the job with explicit starting offsets:
#   .option("startingOffsets", '{"topic":{"0":<offset>,"1":<offset>,...}}')
```

> **Never delete a checkpoint while the streaming job is running.** A running job that loses its
> checkpoint will restart from the earliest available Kafka offset by default, potentially
> reprocessing and duplicating months of data.

**Shadow table swap (for live Silver / Gold tables):**

```text
1. Create {table}_backfill  (same schema as {table})
2. Run pipeline writing to {table}_backfill
3. Validate {table}_backfill vs {table} — row counts, nulls, key business metrics
4. Atomically swap:
     ALTER TABLE {table}          RENAME TO {table}_old;
     ALTER TABLE {table}_backfill RENAME TO {table};
5. Confirm downstream queries run correctly against {table}
6. Drop {table}_old after a 24-hour observation window
```

---

### Silver → Gold

**Owner:** Analytics Engineering  
**Engine:** BigQuery SQL (MERGE or INSERT OVERWRITE)

This step produces the consumption-ready data mart layer.

**Steps:**

1. **Audit column** — apply `_dp_audit`:
   - Fact / OBT / Report → `loaded_*` variant
   - Dimension SCD2 → `updated_*` variant

2. **Write** — BigQuery native table (no Iceberg). Apply clustering aligned to the expected
   query pattern (`JOIN`, `WHERE`, `GROUP BY` columns for this mart).

3. **BigLake is not used for Gold native tables** — Gold tables are read directly via BigQuery.
   Materialized views (`mvw_`) and logic views (`vw_`) are created on top of native tables where needed.

**Business Models:**

| Model | Prefix | Description |
| --- | --- | --- |
| **Fact** | `fct_` | Measurable business events; append-only or upsert by surrogate key |
| **Fact (SCD2)** | `fct_{name}_scd2` | Historical fact records with valid-from/valid-to range |
| **Periodic Snapshot Fact** | `fct_snp_` | Time-triggered measurement per entity per period (e.g., daily inventory level); partition overwrite by `snapshot_date`; see periodic snapshot fact rules below |
| **Dimension** | `dim_` | Current-state dimension with surrogate key; no history tracked; use when changes occur but prior values are not needed; see flat dimension rules below |
| **Dimension (SCD2)** | `dim_{name}_scd2` | Slowly changing attributes; hash-based change detection |
| **Reference** | `ref_` | Lookup and code tables; typically full overwrite |
| **Bridge** | `brd_` | Resolves many-to-many relationships between a fact and a dimension; may carry a weight/allocation factor and optional validity range; see bridge table rules below |
| **Point-in-Time** | `pit_` | Pre-resolves SCD2 surrogate keys for an entity at each snapshot date; eliminates range joins at query time; see PIT table rules below |
| **Mart** | `mrt_` | Subject-area aggregations combining multiple facts and dimensions for a domain; e.g., `mrt_sales_daily` |
| **One-Big-Table** | `obt_` | Pre-joined wide table optimized for direct BI consumption with no further joins |
| **Report** | `rpt_` | Pre-aggregated summary datasets aligned to a specific report or dashboard |

Use [time-window suffixes](../data-zoning-design.md) on table names where the mart represents
a bounded time window (e.g., `fct_sales_curr_day`, `rpt_revenue_prev_month`).

#### Periodic Snapshot Fact (`fct_snp_`)

Use the Periodic Snapshot Fact when a measurement must exist for **every entity for every
time period**, regardless of whether a business event occurred during that period. The row is
produced by the pipeline run itself, not triggered by source activity.

This is distinct from `fct_` (event-driven — a row exists only when something happened) and
`mrt_` (aggregation — derived from other facts).

**Retail examples:**

| Table | Grain | Why Snapshot Fact? |
| --- | --- | --- |
| `fct_snp_inventory_daily` | store × product × day | Stock level exists every day even with no sales |
| `fct_snp_account_balance_daily` | account × day | Balance must be recorded daily for reconciliation |
| `fct_snp_headcount_weekly` | branch × week | Headcount reported per week regardless of changes |
| `fct_snp_promotion_coverage_daily` | promotion × store × day | Coverage must show 0 on inactive days, not be absent |

**Write mode:** `INSERT OVERWRITE` by `snapshot_date` partition. Each pipeline run rebuilds
exactly one date partition — the current run's `snapshot_date`.

**Required columns:**

| Column | Type | Description |
| --- | --- | --- |
| `snapshot_date` | DATE | The date of this measurement; partition key |
| `{entity_keys}` | STRING / INT | Business keys of the entities at this grain |
| `{metric_columns}` | NUMERIC | The measured values at snapshot time |

**Rules:**

- Always include a row for **every entity in scope** on every `snapshot_date`, even when the
  metric value is zero. A missing row is invisible to a dashboard; a zero row is explicit.
- Source for the snapshot fact is usually the corresponding Silver `snp_` or `prd_` table, or
  a join of Silver master and state tables taken at a point in time.
- Do not use SCD2 range joins in the Gold query — use a `pit_` table or snapshot the
  dimension values into the fact row directly at write time.
- Partition by `days(snapshot_date)`; cluster by the most selective entity key for the domain
  (e.g., `store_code` for inventory, `account_id` for balance).

---

#### Flat Dimension (`dim_`)

Use the Flat Dimension when a dimension entity changes over time but **prior attribute values
are not needed** by downstream consumers — only the current state matters. This is a SCD Type 1
dimension: changes overwrite the previous value in-place.

This is distinct from:
- `ref_` — code/lookup tables with no surrogate key and no notion of a tracked business entity
- `dim_{name}_scd2` — history-tracked dimension where old attribute values must be preserved

**Retail examples:**

| Table | Key | Typical Change Pattern |
| --- | --- | --- |
| `dim_store` | `store_code` | Store name, format, or region changes; no need for history |
| `dim_employee` | `employee_id` | Role or branch changes; current assignment is what matters |
| `dim_supplier` | `supplier_code` | Contact details updated; no historical audit needed |
| `dim_date` | `date_key` | Stable calendar dimension; never changes |

**Required columns:**

| Column | Type | Description |
| --- | --- | --- |
| `{dim}_sk` | STRING | Surrogate key — `MD5({business_key})` or a sequence; used as the join key in fact tables |
| `{business_key}` | STRING | The natural key from the source system |
| `{attribute_columns}` | various | Descriptive attributes; overwritten on change (SCD1) |
| `is_active` | BOOLEAN | `false` if the entity has been deactivated in the source |

**Write mode:** MERGE by `{business_key}` — inserts new entities and overwrites attributes on
change. No history rows are created.

**Rules:**

- The surrogate key (`{dim}_sk`) must be **stable and deterministic** — use `MD5({business_key})`
  rather than a sequence, so it survives full reloads without orphaning fact foreign keys.
- If any consumer asks "what was this attribute last month?", use `dim_{name}_scd2` instead.
  The decision between `dim_` and `dim_{name}_scd2` is made at source onboarding and is hard
  to reverse once fact tables are built against the surrogate key.
- Flat dimensions are owned by **Analytics Engineering**. The corresponding Silver source is
  typically a `mst_` table.

---

#### Bridge Table (`brd_`)

Use a Bridge Table when a fact must relate to a dimension with **many-to-many cardinality** —
where a single foreign key on the fact row cannot represent all associated dimension members.

**Retail examples:**

| Use Case | Fact | Dimension | Why Bridge? |
| --- | --- | --- | --- |
| Multi-category product | `fct_sales` | `dim_product_category` | One product belongs to multiple categories |
| Multi-promotion order | `fct_order` | `dim_promotion` | One order redeems multiple promotions |
| Multi-segment customer | `fct_customer_activity` | `dim_customer_segment` | One customer qualifies for multiple segments |
| Store × region hierarchy | `fct_sales` | `dim_region` | One store appears at multiple hierarchy levels |

**Table structure:**

| Column | Type | Description |
| --- | --- | --- |
| `{fact_entity_key}` | STRING | Business key of the fact-grain entity (e.g., `product_code`, `order_id`) |
| `{dim_entity_key}` | STRING | Business key of the associated dimension member |
| `weight` | DECIMAL(10,6) | Optional — allocation factor; must sum to `1.0` across all dim members for the same fact key when present |
| `valid_from` | TIMESTAMP | Optional — when this association began; `NULL` if the relationship has no time bound |
| `valid_to` | TIMESTAMP | Optional — when this association ended; `NULL` if still active |

**Write mode:** Full overwrite (`INSERT OVERWRITE`) for static many-to-many relationships.
For time-bounded bridges (associations that change over time), MERGE by
`({fact_entity_key} + {dim_entity_key} + valid_from)`.

**Rules:**

- Primary key is always `({fact_entity_key} + {dim_entity_key})` — or
  `({fact_entity_key} + {dim_entity_key} + valid_from)` for time-bounded bridges.
- When `weight` is present, validate `SUM(weight) = 1.0` per `{fact_entity_key}` before
  writing. A bridge with weights that don't sum to 1.0 silently distorts any metric
  that multiplies by weight.
- Never expose raw metric values by joining a bridge without dividing by weight (or summing
  after multiplication). Document the expected join pattern on the table as a comment.
- For time-bounded bridges, query pattern is:
  ```sql
  JOIN brd_{name} AS b
    ON f.{entity_key}  = b.{fact_entity_key}
   AND f.event_date BETWEEN b.valid_from AND COALESCE(b.valid_to, '9999-12-31')
  ```
- Bridge tables are built by Analytics Engineering from Silver master data or snapshot tables.
  They are not ingested directly from a source system.

---

#### Point-in-Time Table (`pit_`)

Use a Point-in-Time (PIT) table to **pre-resolve SCD2 surrogate keys** for an entity at each
snapshot date. Without a PIT table, joining a fact to N SCD2 dimensions requires N range joins
(`event_date BETWEEN valid_from AND valid_to`), which are expensive at BigQuery scale.

**How it works:**

For each entity × snapshot date combination, the PIT table stores which SCD2 surrogate key was
valid on that date for each tracked dimension. The fact table joins to the PIT on
`(entity_key + event_date)`, then joins each SCD2 dimension on its surrogate key — simple
equality joins instead of range joins.

```text
Without PIT:  fct JOIN dim_customer_segment_scd2 ON event_date BETWEEN valid_from AND valid_to
              fct JOIN dim_loyalty_tier_scd2      ON event_date BETWEEN valid_from AND valid_to
              → N full-table range scans

With PIT:     fct JOIN pit_customer ON (customer_id + DATE(event_timestamp))  ← equality
              pit JOIN dim_customer_segment_scd2 ON customer_segment_sk        ← equality
              pit JOIN dim_loyalty_tier_scd2      ON loyalty_tier_sk           ← equality
              → point lookups after the PIT join
```

**Table structure:**

| Column | Type | Description |
| --- | --- | --- |
| `snapshot_date` | DATE | The date this PIT row was computed for; partition key |
| `{entity_pk}` | STRING | Business key of the entity |
| `{dim1}_sk` | STRING | Surrogate key from `dim_{dim1}_scd2` valid at `snapshot_date` |
| `{dim2}_sk` | STRING | Surrogate key from `dim_{dim2}_scd2` valid at `snapshot_date` |

**Retail examples:**

- `pit_customer` — for each customer × date, records which `customer_segment_sk`,
  `loyalty_tier_sk`, and `region_sk` were current.
- `pit_product` — for each product × date, records which `product_category_sk`,
  `brand_sk`, and `supplier_sk` were current.

**Build query pattern (for one `snapshot_date`):**

```sql
INSERT OVERWRITE `{project}.gold.pit_{entity}`
PARTITION (snapshot_date = :snapshot_date)

SELECT
    :snapshot_date                      AS snapshot_date,
    e.{entity_pk},
    d1.{dim1}_sk,
    d2.{dim2}_sk
FROM (
    SELECT DISTINCT {entity_pk}
    FROM `{project}.gold.fct_{entity}`
    WHERE DATE(event_timestamp) = :snapshot_date
) AS e
LEFT JOIN `{project}.gold.dim_{dim1}_scd2` AS d1
    ON  e.{entity_pk} = d1.{entity_pk}
    AND :snapshot_date BETWEEN DATE(d1.valid_from) AND DATE(COALESCE(d1.valid_to, '9999-12-31'))
LEFT JOIN `{project}.gold.dim_{dim2}_scd2` AS d2
    ON  e.{entity_pk} = d2.{entity_pk}
    AND :snapshot_date BETWEEN DATE(d2.valid_from) AND DATE(COALESCE(d2.valid_to, '9999-12-31'));
```

**Usage pattern (fact query with PIT):**

```sql
SELECT
    f.order_id,
    f.amount,
    seg.segment_name,
    tier.loyalty_tier_name
FROM `{project}.gold.fct_order` AS f
JOIN `{project}.gold.pit_customer` AS pit
    ON  f.customer_id            = pit.customer_pk
    AND DATE(f.event_timestamp)  = pit.snapshot_date
JOIN `{project}.gold.dim_customer_segment_scd2` AS seg
    ON pit.customer_segment_sk   = seg.customer_segment_sk
JOIN `{project}.gold.dim_loyalty_tier_scd2` AS tier
    ON pit.loyalty_tier_sk       = tier.loyalty_tier_sk;
```

**Rules:**

- Rebuild only the `snapshot_date` partitions that have new or changed fact rows — do not
  rebuild all historical dates on every run.
- If a dimension has no valid row for an entity at a given date, leave the `_sk` column
  `NULL` — do not fabricate a surrogate key. Downstream queries must handle nullable SKs.
- PIT tables are **optional optimisations**. Create one only when SCD2 range-join costs are
  measurably impacting Gold query SLA. Profile query costs before building.
- Partition by `snapshot_date` with `require_partition_filter = true` — PIT tables can be
  very large; always filter by date in queries.
- The PIT table is a **derived table**, not a source of truth. It can be dropped and rebuilt
  from the SCD2 dimensions at any time.

---

### Silver / Gold → Access

**Owner:** Data Engineering + Analytics Engineering  
**Engine:** BigQuery (authorized views)

The Access zone exposes Silver, Gold, and Shared objects to end users through authorized views
with Row-Level Security (RLS) and Column-Level Security (CLS) applied.

- **Direct access view** — wraps a single table with no added logic: `avw_{dataset}_{table}`
- **Logic view** — adds filtering, masking, or combining logic: `avw_{purpose}`

No data is stored in the Access zone. Views are the only objects.
End users never access Silver, Gold, or Shared tables directly.

---

## Retail Platform Scalability Patterns

These patterns address the specific scaling challenges of a retail data platform: **intraday
volume spikes** (promotions, store opening/closing), **end-of-month reporting pressure**, and
**peak seasons** (year-end sale, major campaigns like 11.11 or 12.12).

---

### Partition Pruning Strategy

Correct partitioning is the most impactful query optimisation available at retail scale. Every
table must be designed so the most common queries touch the minimum number of partitions.

| Zone | Table Type | Partition Key | Rationale |
| --- | --- | --- | --- |
| Bronze | All | `dp_partition` (hours or days) | Limit raw scan to the recent ETL window |
| Silver | Transaction | `hours(created_at)` | Align with hourly pipeline cadence |
| Silver | Master / State | None or bucket on entity key | Current-state tables are small; avoid over-partitioning |
| Silver | SCD2 | `days(valid_from)` | Time-range queries by validity period |
| Gold | Fact | `days(event_date)` or `months(event_date)` | BI queries almost always filter by date |
| Gold | Mart / Report | `days(report_date)` | Each pipeline run overwrites one report date |

**Rules:**

- Set `require_partition_filter = true` on all BigLake (Bronze/Silver) external tables to block
  full-table scans from runaway queries.
- Always include the partition column in `WHERE` clauses of Silver and Gold queries. If a Gold
  query cannot filter on `event_date`, investigate the query — not the table design.
- For BigQuery native Gold tables, set clustering columns to the most selective `WHERE` / `JOIN`
  columns after the partition column (typically `store_code`, `product_code`, or `customer_id`
  depending on domain).

---

### Spark Cluster Sizing

Spark jobs share a GKE node pool. Cluster sizing must account for both the standard schedule
and peak retail load.

**Standard sizing tiers:**

| Tier | Driver | Executors | Use Case |
| --- | --- | --- | --- |
| `xs` | 2 vCPU / 4 GB | 2 × (2 vCPU / 8 GB) | Low-volume reference tables, lookups |
| `sm` | 4 vCPU / 8 GB | 4 × (4 vCPU / 16 GB) | Standard incremental Bronze → Silver |
| `md` | 8 vCPU / 16 GB | 8 × (8 vCPU / 32 GB) | Daily batch aggregations, SCD2 diffs |
| `lg` | 16 vCPU / 32 GB | 16 × (8 vCPU / 32 GB) | Initial loads, backfills, compaction |

Set the tier in pipeline config: `spark_cluster_tier: sm`. The Airflow operator translates
this to the appropriate GKE resource request.

**Peak season rules:**

- Promote all critical pipelines (`sale`, `inventory`, `promotion` domains) by one tier during
  peak campaign windows.
- File a capacity pre-warming request with the DPE team at least **5 business days** before a
  major campaign to allow GKE node pool pre-scaling.
- Do not run backfills or initial loads during peak windows — they compete for cluster capacity
  with live pipelines.

---

### BigQuery Slot Reservation

Gold-layer BigQuery jobs run on a shared slot pool. Under heavy concurrent Gold pipeline load
or BI queries, slot contention causes job queuing and SLA misses.

**Reservation tiers:**

| Reservation | Slots | Assigned To | Priority |
| --- | --- | --- | --- |
| `de-pipelines` | 500 | Silver → Gold pipeline jobs | HIGH |
| `bi-consumption` | 300 | Looker / Data Studio dashboards | MEDIUM |
| `ae-development` | 200 | AE ad-hoc queries and dbt runs | MEDIUM |
| `on-demand` | — | Overflow for all projects | LOW |

**Rules:**

- Pipeline Airflow jobs must submit to the `de-pipelines` reservation via the pipeline service
  account. Never submit pipeline jobs without a reservation label — they land in on-demand and
  may queue indefinitely during peak.
- If `de-pipelines` utilisation is consistently > 80% over a 7-day window, file a capacity
  increase request with the DPE team.
- AE dbt runs that process > 10 TB should be scheduled off-peak (after 22:00 local time) to
  avoid starving live pipelines of slots.

---

### Intraday Volume Spike Handling

Retail transactions spike sharply at store open (09:00), lunch (12:00), and closing (21:00).
These spikes are predictable — pipelines must absorb them without cascading failure.

**Design rules:**

1. **Decouple ingestion from transformation** — the Raw zone absorbs the spike as raw files.
   Bronze and Silver pipelines process at their own cadence and are not time-coupled to the
   ingest spike.
2. **Use Kafka consumer lag as a run trigger** — if consumer lag on a Bronze pipeline exceeds
   `max_lag_threshold` (set per pipeline), the Airflow DAG triggers a high-priority run instead
   of waiting for the next scheduled slot.
3. **Cap micro-batch size for Kappa pipelines** — configure `maxOffsetsPerTrigger` to prevent
   one spike from monopolising cluster executors:
   ```python
   .option("maxOffsetsPerTrigger", 100_000)
   ```
4. **Monitor queue depth** — emit `kafka_consumer_lag` and `pubsub_oldest_unacked_message_age`
   as custom Cloud Monitoring metrics. Alert when lag exceeds 2× the pipeline's scheduled interval.

---

### Holiday / Campaign Pre-Warm Checklist

Run this checklist **5 business days before** any major retail campaign:

- [ ] Identify all pipelines with `domain in (sale, inventory, promotion)` — confirm `spark_cluster_tier` is promoted by one tier
- [ ] Submit GKE node pool pre-scale request to DPE (target: +30% executor capacity)
- [ ] Confirm BigQuery `de-pipelines` reservation has sufficient slots; request increase if current utilisation > 70%
- [ ] Disable non-critical backfills and initial loads for the campaign window
- [ ] Set Kafka `retention.ms` to cover at least 3× the expected campaign spike duration
- [ ] Verify Pub/Sub / Dataflow sink quotas are not near project limits
- [ ] Run a load test against the Gold Access views with simulated campaign query volume
- [ ] Confirm on-call DE rotation is staffed for the full campaign duration
