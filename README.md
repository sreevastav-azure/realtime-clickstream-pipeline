# Real-Time E-Commerce Clickstream Analytics Pipeline

A real-time streaming data pipeline that ingests simulated e-commerce clickstream events (page views, cart additions, checkouts, purchases) through Azure Event Hubs, processes them with Databricks Structured Streaming, and lands them in a medallion architecture (Bronze → Silver → Gold) on Delta Lake with Unity Catalog governance.

Unlike the batch/scheduled pipelines in my other projects, this one processes data continuously as it arrives — with proper watermarking, tumbling window aggregations, and late-data handling.

## Architecture

```
Python Event Generator (simulated clickstream)
        │
        ▼
Azure Event Hub (Kafka-compatible protocol)
        │
        ▼
Databricks Structured Streaming — Bronze
  (raw JSON + Kafka metadata → Delta)
        │
        ▼
Databricks Structured Streaming — Silver
  (schema validation, type casting, null-filtering)
        │
        ▼
Databricks Structured Streaming — Gold
  (1-min tumbling windows, 2-min watermark,
   foreachBatch + MERGE upsert into Delta)
        │
        ▼
Unity Catalog: clickstream_catalog.{bronze|silver|gold}
```

## Tech Stack

- **Ingestion:** Azure Event Hubs (Standard tier, 4 partitions, Kafka protocol)
- **Processing:** Databricks Structured Streaming (Spark 4.0, Runtime 17.3 LTS)
- **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake
- **Governance:** Unity Catalog (three-level namespace, managed locations per medallion layer)
- **Language:** Python (PySpark, `azure-eventhub` SDK for the producer)

## Data Model

Each simulated event contains:

| Field | Description |
|---|---|
| `event_id` | Unique event identifier |
| `user_id` | Simulated user (1 of 500) |
| `session_id` | Simulated browsing session |
| `event_type` | `page_view`, `add_to_cart`, `checkout_start`, `purchase` |
| `product_id` | Simulated product (1 of 200) |
| `category` | electronics, fashion, home, sports, books, beauty, toys |
| `price` | Populated for cart/checkout/purchase events; null for page views |
| `timestamp` | Event-time timestamp (UTC) |

## Key Design Decisions

**Kafka protocol instead of the native Event Hubs Spark connector.**
The native `azure-eventhubs-spark` library only supports Scala 2.12 and older Spark versions. Since Event Hubs natively supports the Kafka protocol, I used Spark's built-in Kafka connector instead — the more modern, actively maintained approach, and one directly transferable to real Kafka-based pipelines.

**Explicit checkpoint locations.**
This Unity Catalog-enabled workspace disallows implicit temporary checkpoints for streaming, so every stream (Bronze write, Silver write, Gold write) uses an explicit, dedicated checkpoint path in ADLS.

**Watermarking (2 minutes) + tumbling windows (1 minute).**
The watermark bounds how long Spark waits for late-arriving events before finalizing a window's aggregate and releasing the associated state — this keeps memory usage bounded in a long-running streaming job, at the cost of dropping events that arrive more than 2 minutes late.

**`foreachBatch` + `MERGE INTO` for Gold, instead of native streaming writes.**
Delta Lake's native streaming sink only supports `append` and `complete` output modes. Windowed aggregations with watermarks require `update` mode (a window's totals can change as more matching events arrive within the watermark period), so `outputMode("update")` combined with `.toTable()` isn't supported. Each micro-batch of aggregated results is instead merged into the Gold Delta table via `DeltaTable.merge()` — updating existing window/category/event_type rows and inserting new ones. This is the standard production pattern for streaming aggregations into Delta.

**Schema-based null-filtering as the Silver-layer data quality gate.**
Records that fail to parse against the expected JSON schema come through with null fields; the Silver layer filters these out (`event_id IS NOT NULL`) rather than propagating malformed data downstream.

## Results

- **Bronze:** 4,710 raw events ingested
- **Silver:** 4,710 events parsed and validated — **zero data loss** from Bronze to Silver
- **Gold:** 1,906 aggregated rows across ~78 one-minute windows, 7 categories, 4 event types

## Sample Gold Output

| window_start | category | event_type | event_count | total_revenue | avg_price |
|---|---|---|---|---|---|
| 07:58:00 | books | checkout_start | 3 | 656.14 | 218.71 |
| 07:58:00 | toys | add_to_cart | 3 | 626.22 | 208.74 |
| 07:57:00 | books | add_to_cart | 4 | 1250.46 | 312.62 |

## Interview Talk Track

"I built an Event Hub-to-Databricks Structured Streaming pipeline that processes simulated e-commerce clickstream events through a medallion architecture. It uses Kafka-protocol ingestion, schema validation with null-filtering as a Silver-layer data quality gate, and 1-minute tumbling window aggregations with a 2-minute watermark in Gold — using foreachBatch and MERGE INTO to upsert results into Delta, since native Delta streaming sinks don't support update output mode."

## Infrastructure

- Event Hub Namespace: `ehns-clickstream-vastav` (Standard tier, Central India)
- Event Hub: `clickstream-events` (4 partitions)
- Storage: `itsmystorage` (bronze/silver/gold containers, reused from prior projects)
- Databricks: `itsmydatabricks` workspace, Unity Catalog `clickstream_catalog`
