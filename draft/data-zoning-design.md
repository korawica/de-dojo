# Data Zoning Design

**Data Zone**(s) define how data is *organized*, *stored*, and *accessed* in alignment with
Data Governance policy. Each zone represents a distinct stage of data maturity
— from raw ingestion through to business-ready consumption — with clear rules
on what transformations are applied, who owns it, and who can access it.

> **Data Zone**(s) applies to each [**Data Domain**](../data-domains/not-implement-yet.md) Google Project ID by IaC.

---

## Architecture Model

This **Data Zone** design reference from the **Medallion/Lakehouse** architecture
but still supports both **Lambda** and **Kappa** architectures:

- **Lambda Architecture** — separate batch and streaming pipelines that converge at the serving layer
- **Kappa Architecture** — a single streaming pipeline that handles all transform logic and can write directly to any zone; pipeline state is checkpointed at the `./checkpoint` prefix

Both architectures coexist. The zone model accommodates either without structural changes.

---

## Zone Flow

```text
                    ┌──────────────────────────────────────────────────────────┐
  External          │                 Standard Data Zones                      │
  Sources  ──────►  │  [Raw] ────► [Bronze] ───► [Silver] ──► [Gold]           │
                    │                    │          │            │             │
  Trusted           │              [Quarantine]  [Staging] ◄─────┘             │
  Sources  ──────►  │  [Shared]                                                │
                    └──────────────────────────────────────────────────────────┘
                                         │  Silver + Gold + Shared
                                         ▼
                                     [Access]  ◄──── End Users (RLS/CLS)
```

**Unstructured data (image / video / audio) follows a parallel path:**

```text
  Media
  Source ────► [Raw GCS /unstructured/] ──► [Shared GCS — Media Store]
                            │                             │
                   metadata extracted              served to end users
                            │                       via signed URLs
                            ▼
                Bronze → Silver → Gold → Access
                    (metadata as structured data)
```

---

## Overview

| Zone           | Purpose                         | Owner            | Retention                           | SLA    |
|----------------|---------------------------------|------------------|-------------------------------------|--------|
| **Raw**        | Exact copy of source data       | DE               | 60 days (then archive)              | Hourly |
| **Bronze**     | Validated & standardized schema | DE               | Indefinite                          | Hourly |
| **Silver**     | Business-modeled data           | AE               | 3 years                             | Daily  |
| **Gold**       | Analytics-ready data marts      | AE               | Varies by mart                      | Daily  |
| **Staging**    | Temporary working tables        | DE / AE          | Short-lived (7-15 days)             | —      |
| **Quarantine** | Failed quality-check records    | DE               | Depends on investigation and fixing | —      |
| **Shared**     | Cloned/shared trusted data      | Shared (DE + AE) | Inherited from source               | Daily  |
| **Access**     | Row/column-level secured views  | Shared (DE + AE) | N/A (views only)                    | Daily  |

---

## Ownership & Responsibilities

| Team                           | Zones                   | Responsibilities                                                                                      |
|--------------------------------|-------------------------|-------------------------------------------------------------------------------------------------------|
| **Data Engineering (DE)**      | Raw, Bronze, Quarantine | Source ingestion, schema standardization, sensitive data encryption/tokenization, pipeline operations |
| **Analytics Engineering (AE)** | Silver, Gold            | Business modeling, data quality with business rules, mart design, Z-order optimization                |
| **Shared (DE + AE)**           | Staging, Shared, Access | Cross-team intermediate tables, external data sharing, access view management                         |

> DE and AE access the underlying objects in each zone directly. End users must
> go through the **Access zone** only.

---

## Compliance & Data Protection

The platform is subject to three regulatory frameworks. All apply from the point of ingestion.

| Framework   | Scope                                                   | Key Obligation                                                                     |
|-------------|---------------------------------------------------------|------------------------------------------------------------------------------------|
| **PCI DSS** | Payment card data (card numbers, CVV, track data)       | Tokenize before data leaves the Raw zone                                           |
| **PDPA**    | Thai customer PII (name, address, national ID, contact) | Mask/pseudonymize in Bronze; honor right-to-erasure within 3-year retention window |
| **GDPR**    | EU customer personal data                               | Data residency, erasure requests, and cross-border transfer controls               |

**Sensitive Data Flow:**

```text
  Raw Zone
  ├── Plain PII and payment data stored (temporary, request-only access)
  └── Encrypted at rest via Google Cloud encryption services
         │
         ▼  Spark pipeline (DE-managed, per-column selection)
  Bronze Zone
  ├── PCI data → tokenized (card numbers replaced with tokens)
  └── PII data → masked or pseudonymized
         │
         ▼
  Silver / Gold / Access
  └── No plain PII or card data; Access zone adds CLS for further restriction
```

> **Right-to-erasure implementation is TBD.** The mechanism (Iceberg row-level
> delete with scheduled compaction, column nullification, or BigQuery DML DELETE
> for Gold native tables) has not yet been finalized. Raw zone data follows the 60-day
> archive policy and will age out without special action. Bronze onward requires
> a defined erasure workflow before the platform can be considered fully PDPA/GDPR
> compliant.
>
> For unstructured data, erasure applies to both the metadata rows (same mechanism as above)
> and the binary files in the Media Store GCS bucket (active deletion required — see
> [Unstructured Data — Compliance](#compliance--unstructured-data)).

---

## Environment Separation

The platform operates two environments: **Production (prod)** and **Development (dev)**.

| Aspect             | Production                                 | Development                                 |
|--------------------|--------------------------------------------|---------------------------------------------|
| **Data**           | Live, full-volume source data              | Sampled or synthetic data                   |
| **Access**         | Strictly controlled via Access zone        | Relaxed; DE/AE use personal sandboxes       |
| **Zone isolation** | Full zone separation per environment       | Dev zones isolated from prod datasets       |
| **Compliance**     | Full PCI DSS / PDPA / GDPR controls active | Sensitive data anonymized before use in dev |

---

## Standard Data Zones

### Raw

**Purpose:** Preserve source data exactly as received, with no modifications.

- Stores data in its original form: JSON strings, semi-structured formats, or files with schema auto-detection
- No transformation, cleaning, or enrichment applied
- Serves as the immutable source of truth for reprocessing
- Sensitive data (PII, payment card data) is permitted to reside here — access is granted on a temporary, request-only basis
- After 60 days, data transitions to **archive mode** (cold storage)

**Sensitive Data Handling:**

Before data moves beyond this zone, Data Engineers apply **column-level encryption and tokenization via Apache Spark** to all sensitive fields. This covers data originating from BigQuery, Looker, LookML, and application sources. Only DE-designated columns are processed; the selection is managed per pipeline.

> All sensitive data is encrypted or tokenized before leaving the Raw zone.
> Downstream zones (Bronze onward) never store plain PII or card data.

**Storage:**

- **Cloud Storage**
  - File pattern: `{source}/year={yyyy}/month={mm}/day={dd}/hour={hh}`
  - Supported formats (structured/semi-structured): `NEWLINE_JSON`, `JSON`, `CSV`, `Excel`, `AVRO`, `Parquet`
  - Supported formats (unstructured media): `JPEG`, `PNG`, `GIF`, `WebP`, `MP4`, `MOV`, `AVI`, `MP3`, `WAV`, `FLAC`
  - Media file path pattern: `unstructured/{type}/year={yyyy}/month={mm}/day={dd}/`
    - `{type}`: `image`, `video`, or `audio`
  - Staging prefix: `/staging`
- **BigQuery**
  - Raw tables store the full payload in a single `value` column
  - BigLake external tables expose raw files via BigLake connection (hot-data layer in Lambda Architecture)
  - Lambda hot tables use time-window suffixes on table names, `{table}_curr_{frequency}`,
    to indicate the data they contain:

    | Suffix               | Window        | Point Prefix                                         |
    |----------------------|---------------|------------------------------------------------------|
    | `{table}_curr_hour`  | Current hour  | `{source}/year={yyyy}/month={mm}/day={dd}/hour={hh}` |
    | `{table}_curr_day`   | Current day   | `{source}/year={yyyy}/month={mm}/day={dd}`           |
    | `{table}_curr_month` | Current month | `{source}/year={yyyy}/month={mm}`                    |

> - **Retention:** 60 days active → archive
> - **SLA:** Hourly

---

### Bronze

**Purpose:** Apply schema standardization, data quality checks, and sensitive data masking — the Data Contract layer.

This zone enforces structure on raw data without applying business logic. It is the foundation for reliable downstream processing. Records that fail quality checks are routed to the **Quarantine zone**.

- Schema evolution and reconciliation applied automatically
- All column names renamed to `snake_case`; non-ASCII source names (e.g., Thai language) retained without special characters
- Nested structs flattened; array-type columns are not exploded at this stage
- Data quality checks run without business rules; failures sent to Quarantine
- PCI data tokenized, PII masked or pseudonymized via Spark before writing

**Late-Arriving Data:**

Records always write to the **current ETL partition** (`dp_partition`), regardless of when the original event occurred. The original event time is preserved in the `{event_timestamp}` column. Downstream Silver and Gold pipelines use `{event_timestamp}` for business-time logic — never `dp_partition`.

| Column              | Value                      | Purpose                                                                                       |
|---------------------|----------------------------|-----------------------------------------------------------------------------------------------|
| `dp_partition`      | ETL ingestion datetime     | Physical partition key for pipeline processing                                                |
| `kafka_timestamp`   | Original Kafka sync time   | Kafka sync time used for deduplicate if data was repeated from source                         |
| `{event_timestamp}` | Original source event time | Business time used for all downstream aggregations like `updated_at`, or `last_modified_date` |

**Deduplication:**

Bronze removes **exact duplicates** introduced by streaming retries (Kappa) before writing to Iceberg.

- Dedup key: `source_id + record_hash` (hash over all payload fields)
- Window: within the same micro-batch; late duplicates caught by Silver's business-key dedup

**Storage:**

- **Cloud Storage**
  - Iceberg open table format
  - Partitioned by ETL datetime using `dp_partition` column
  - Hidden partition pattern: `table/dp_partition={yyyymmdd-hh}`
  - Staging prefix: `/staging`
- **BigQuery**
  - BigLake external tables over Iceberg files via BigLake connection

> - **Retention:** Indefinite
> - **SLA:** Hourly

---

### Silver

**Purpose:** Business-modeled data — the Data Model layer.

This zone holds clean, structured data organized by business entity type. It is the primary source for analytics and Gold zone processing. Records that fail business-rule quality checks are routed to the **Quarantine zone**.

- Data quality checks applied with business rules enforced; failures sent to Quarantine
- All column names in `snake_case`, English only, no special characters
- Selective columns allowed (not all source fields are required)
- Array columns can be exploded (unnested) where primary keys are defined

**Deduplication:**

Silver enforces **business-key uniqueness** as a second dedup stage after Bronze's exact-duplicate removal.

- Dedup key: business primary key per entity type (e.g., `transaction_id` for `txn_`, `customer_id` for `mst_`)
- Strategy: keep the record with the latest `{event_timestamp}`; route superseded duplicates to Quarantine with reason `DUPLICATE_BUSINESS_KEY`

**Entity Types:**

| Type              | Prefix            | Description                                                              |
|-------------------|-------------------|--------------------------------------------------------------------------|
| **Master**        | `mst_`            | Reference entities (customers, products, locations)                      |
| **Master (SCD2)** | `mst_{name}_scd2` | Slowly Changing Dimension Type 2 — full history of master record changes |
| **Transaction**   | `txn_`            | Event and transactional records                                          |
| **State**         | `stt_`            | Current or point-in-time state snapshots                                 |
| **CDC**           | `cdc_`            | Change Data Capture — all incremental data change records                |
| **Snapshot**      | `snp_`            | Full table snapshots at a point in time                                  |

**SCD2 — Change Detection & Column Standard:**

`mst_{name}_scd2` tables are populated by two complementary triggers:

| Trigger                             | When Used                         | Mechanism                                                           |
|-------------------------------------|-----------------------------------|---------------------------------------------------------------------|
| **CDC from Bronze** (`cdc_` tables) | Sources that emit change events   | New CDC record with changed fields → insert new SCD2 row            |
| **Snapshot diff**                   | Sources that emit full loads only | Compare current `snp_` with previous snapshot → detect changed rows |

Every SCD2 table carries these standard columns:

| Column            | Type      | Description                                         |
|-------------------|-----------|-----------------------------------------------------|
| `scd_valid_from`  | TIMESTAMP | When this version became active                     |
| `scd_valid_to`    | TIMESTAMP | When this version was superseded (`NULL` = current) |
| `scd_is_active`   | BOOLEAN   | `true` for the active record                        |

**Storage:**

- **Cloud Storage**
  - Iceberg open table format
  - Business-defined partitioning (defined by Analytics Engineers per use case)
  - Z-order clustering supported for query optimization, tuned to Gold zone access patterns
  - Staging prefix: `/staging`
- **BigQuery**
  - BigLake external tables over Iceberg files via BigLake connection

> - **Retention:** 3 years
> - **SLA:** Daily

---

### Gold

**Purpose:** Analytics-ready data — the Data Mart layer.

This zone delivers curated datasets directly consumable by business users and reporting tools. No raw or intermediate data is exposed here.

**Object Types:**

| Type              | Prefix  | Description                                   |
|-------------------|---------|-----------------------------------------------|
| **Fact**          | `fct_`  | Measurable business events                    |
| **Dimension**     | `dim_`  | Descriptive attributes for facts              |
| **Reference**     | `ref_`  | Lookup and code tables                        |
| **Mart**          | `mrt_`  | Subject-area aggregations                     |
| **One Big Table** | `obt_`  | Denormalized wide tables for direct reporting |
| **Report**        | `rpt_`  | Pre-aggregated summary specific subject-area  |
| **Aggregate**     | `agg_`  | Pre-aggregated summary datasets               |

**Time-Window Naming Suffixes:**

Gold tables that represent a specific time window append a suffix to the base name (e.g., `fct_sales_daily`, `rpt_revenue_prev_month`):

| Suffix                    | Window Description                           |
|---------------------------|----------------------------------------------|
| `_curr_hour`              | Current hour only                            |
| `_curr_day`               | Current day only                             |
| `_curr_month`             | Current month only                           |
| `_prev_day_inc_curr_hour` | Previous full day including the current hour |
| `_prev_month`             | Previous full month                          |
| `_3m_inc_curr_month`      | Last 3 months including the current month    |
| `_7d_exc_curr_day`        | Last 7 days excluding today                  |
| `_daily`                  | Daily rollup (full day aggregation)          |
| `_monthly`                | Monthly rollup (full month aggregation)      |

**Storage:**

- **BigQuery**
  - **Native tables** (`fct_`, `dim_`, `ref_`, `mrt_`, `obt_`, `rpt_`) — fully managed, optimized for read performance
  - **Materialized views** (`mvw_`) — pre-computed aggregations, refreshed on schedule
  - **Logic views** (`vw_`) — on-demand views for ad hoc or low-frequency access where read performance is not critical

> - **Retention:** Varies by mart
> - **SLA:** Hourly

---

### Staging

**Purpose:** Temporary working area for intermediate transformations before data lands in Silver or Gold.

- Used during ETL/ELT pipeline processing steps
- Tables are short-lived and cleaned up after the pipeline run completes
- Table naming convention: `stg_{target-zone}_{purpose}` (e.g., `stg_gold_sales_summary`)

**Storage:**

- **BigQuery**
  - Staging dataset: `staging`

---

## Special Data Zones

> Storage summary for special zones:
> - **Quarantine** — Cloud Storage (Iceberg) + BigQuery (BigLake), same pattern as Bronze/Silver
> - **Shared** — BigQuery (cloned tables, authorized views) + Cloud Storage (Media Store for unstructured data — see [Unstructured Data](#unstructured-data))
> - **Access** — BigQuery only (authorized views); media binary access uses GCS signed URLs outside this zone

### Quarantine

**Purpose:** Isolate records that fail data quality checks in Bronze or Silver for investigation and reprocessing.

Records are routed here automatically when pipeline quality gates reject them — they are never silently dropped.

- Bronze failures: records that fail schema or structural checks (missing required fields, type mismatches)
- Silver failures: records that fail business-rule quality checks

**Storage:**

- **Cloud Storage**
  - Iceberg open table format (written directly by Spark pipelines)
  - Staging prefix: `/staging`
- **BigQuery**
  - BigLake external tables over Iceberg files via BigLake connection

---

### Shared

**Purpose:** Make trusted data from external or cross-domain sources available within the platform without duplicating pipelines.

**Storage:**

- **BigQuery**
  - Cloned tables from trusted data sources
  - Authorized views from trusted data sources
- **Cloud Storage** *(Media Store — for unstructured data serving only)*
  - Bucket pattern: `gs://{domain}-shared-media-{env}/{type}/{source_system}/{id}/`
  - Holds normalized/processed copies of media files (thumbnails, compressed video, transcriptions)
  - Does **not** hold raw originals — those remain in the Raw zone
  - End users access via GCS signed URLs; no standing GCS permissions are granted to end users
  - Retention defined per media type and domain (independent of Raw zone retention)

---

### Access

**Purpose:** Enforce fine-grained access control — the Data Access layer.

This zone is the secure interface between the data platform and end users. All **structured** objects in Silver, Gold, and Shared are exposed exclusively through this layer with appropriate permissions applied.

- **Row-Level Security (RLS)** — restricts data rows by user or role
- **Column-Level Security (CLS)** — masks or hides sensitive fields per user
- All exposed objects are authorized views — no direct table access is granted to end users

> **Unstructured data (media binaries)** is not served through BQ authorized views. 
> End users access media files via GCS signed URLs generated by the platform API
> or application, using `media_store_path` from the Access zone view as the reference.
> See [Unstructured Data — Access Control Summary](#access-control-summary).

**View Naming Conventions:**

| View Type       | Prefix / Pattern        | Example                          |
|-----------------|-------------------------|----------------------------------|
| **Direct View** | `avw_{dataset}_{table}` | `avw_gold_dim_customer`          |
| **Logic View**  | `avw_{purpose}`         | `avw_ref_doc_no_sales_curr_hour` |

**Storage:**

- **BigQuery**
  - Authorized views only (no native tables)

---

## Unstructured Data

The zone model handles structured and semi-structured data by default. Unstructured data
(images, video, audio) follows the same zone principles but requires a clear split between
the **binary file** and its **metadata**.

### Two Things, Two Paths

| Artefact                                                                     | Nature                 | Path                                               |
|------------------------------------------------------------------------------|------------------------|----------------------------------------------------|
| **Binary file** (image / video / audio)                                      | Opaque blob, no schema | Raw zone (GCS) → Media Store (Shared GCS)          |
| **Metadata** (EXIF, dimensions, labels, embeddings, transcriptions, GCS URI) | Structured, queryable  | Standard pipeline: Bronze → Silver → Gold → Access |

The metadata pipeline requires no structural changes to the zone model.
Only the binary file path is specific to unstructured data.

### Binary File Path

```text
Unstructured Source (image / video / audio)
         │
         ▼
┌────────────────────────────────────────────────────────────┐
│  Raw Zone (GCS)                                            │
│  Original file, unmodified. DE-only. 60-day → cold archive │
│  Immutable source of truth; used for reprocessing only.    │
└──────────────────┬─────────────────────────────────────────┘
                   │
        ┌──────────┴───────────┐
        ▼                      ▼
[Metadata extracted]     [File normalized]
 EXIF, labels, URIs,      Resized thumbnails,
 ML-generated tags,       transcriptions,
 embeddings, etc.         audit-safe copies, etc.
 │                              │
 ▼                              ▼
Bronze → Silver → Gold    Shared Zone (GCS — Media Store)
→ Access (BigQuery)       End-user access via signed URLs
```

### Raw Zone — Original Files

Original binary files land in the Raw zone under an `unstructured/` prefix — the same
bucket and access controls as all other Raw zone data.

- **Path pattern**: `gs://{domain}-raw-{env}/unstructured/{type}/year={yyyy}/month={mm}/day={dd}/`
  - `{type}`: `image`, `video`, or `audio`
- **Access**: DE-only, temporary request-only (same as all Raw zone data)
- **Retention**: 60 days active → cold archive (same as structured raw data)

The Raw copy is the immutable source for audit and reprocessing. End users never access it directly.

### Shared Zone (GCS) — Media Store

The Media Store is the serving layer for unstructured data. It holds normalized or processed
copies of media files and is the only layer end users interact with for binary content.

- **Path pattern**: `gs://{domain}-shared-media-{env}/{type}/{source_system}/{id}/`
  - Example: `gs://product-shared-media-prod/image/erp/P123456/thumbnail.jpg`
- **Contents**: Processed copies — resized images, compressed video, extracted audio transcripts.
  Raw originals are never written here.
- **End-user access**: GCS signed URLs generated by the platform API or application.
  End users hold no standing GCS permissions.
- **DE / AE access**: Direct GCS read access on the shared-media bucket.
- **Retention**: Defined per media type and domain; independent of Raw zone retention.

### Metadata in Silver (example)

```sql
silver.mst_product_image (
  product_id          STRING,
  media_store_path    STRING,   -- gs:// path to Media Store object
  file_format         STRING,   -- JPEG, PNG, MP4, WAV, etc.
  width_px            INT64,
  height_px           INT64,
  size_bytes          INT64,
  ml_labels           ARRAY<STRING>,
  captured_at         TIMESTAMP,
  ingested_at         TIMESTAMP,
  is_current          BOOLEAN
)
```

The Access zone exposes `media_store_path` via authorized views. Applications read the path
and fetch the binary from the Media Store bucket using signed URLs.

### Access Control Summary

| Role                         | Raw (binary)       | Metadata (BQ)                   | Media Store (GCS)                           |
|------------------------------|--------------------|---------------------------------|---------------------------------------------|
| **End Users**                | ❌ No access        | ✅ Access zone views (RLS/CLS)   | ✅ Signed URLs only — no standing GCS access |
| **Analytics Engineers (AE)** | ❌ No direct access | ✅ Direct access to all BQ zones | ✅ Direct GCS read                           |
| **Data Engineers (DE)**      | ✅ Direct access    | ✅ Direct access to all BQ zones | ✅ Direct GCS read/write                     |

### Compliance — Unstructured Data

| Framework   | Obligation            | For Media Files                                                                                                                      |
|-------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| **PDPA**    | Mask/pseudonymize PII | Face images and voice recordings containing Thai customer PII must be blurred or anonymized before writing to Media Store            |
| **GDPR**    | Erasure requests      | Right-to-erasure applies to GCS objects. Raw ages out at 60 days. Media Store requires active deletion. **Erasure workflow is TBD.** |
| **PCI DSS** | Tokenize card data    | If scanned receipts or card images are ingested, tokenize any card numbers before writing to Media Store                             |

---

## Access Control Policy

| Role                         | Access Path                                                  |
|------------------------------|--------------------------------------------------------------|
| **End Users**                | Access zone only (via authorized views with RLS/CLS applied) |
| **Data Engineers (DE)**      | Direct access to underlying objects in all zones             |
| **Analytics Engineers (AE)** | Direct access to underlying objects in all zones             |

End users who require data access must submit an access request by table or path location.
Access is provisioned through the Access zone exclusively — no direct zone access
is granted to end users.

---

## Zone × Storage Matrix

Quick reference for which storage system(s) each zone uses:

| Zone           | Cloud Storage (GCS)                   | BigQuery                                      |
|----------------|---------------------------------------|-----------------------------------------------|
| **Raw**        | ✅ Primary (structured + media files) | ✅ BigLake external tables (Lambda hot layer) |
| **Bronze**     | ✅ Iceberg open table format          | ✅ BigLake external tables over Iceberg       |
| **Silver**     | ✅ Iceberg open table format          | ✅ BigLake external tables over Iceberg       |
| **Gold**       | —                                     | ✅ Native tables, materialized views, views   |
| **Staging**    | —                                     | ✅ Native tables (short-lived)                |
| **Quarantine** | ✅ Iceberg open table format          | ✅ BigLake external tables over Iceberg       |
| **Shared**     | ✅ Media Store (unstructured only)    | ✅ Cloned tables, authorized views            |
| **Access**     | —                                     | ✅ Authorized views only                      |

