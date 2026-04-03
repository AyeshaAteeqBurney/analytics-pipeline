# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

Table DDL:

```sql
CREATE TABLE lakehouse.taxi.bronze (
    VendorID DOUBLE,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag STRING,
    PULocationID DOUBLE,
    DOLocationID DOUBLE,
    payment_type DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    kafka_key STRING,
    kafka_timestamp TIMESTAMP,
    kafka_partition INT,
    kafka_offset LONG,
    ingested_at TIMESTAMP
) USING iceberg
```

What is stored and why it is kept as-is: Bronze holds the taxi payload fields as they arrive from Kafka JSON, with types relaxed to `DOUBLE` / `STRING` so minor schema variation still parses. Datetimes stay strings to preserve the raw source form. Kafka columns (`kafka_key`, `kafka_timestamp`, `kafka_partition`, `kafka_offset`) and `ingested_at` provide lineage and support offset-based reasoning. `airport_fee` is nullable so messages with and without that key (custom scenario) load without failure.

### Silver

Table DDL:

```sql
CREATE TABLE lakehouse.taxi.silver (
    vendor_id            INT,
    pickup_datetime      TIMESTAMP,
    dropoff_datetime     TIMESTAMP,
    passenger_count      INT,
    trip_distance        DOUBLE,
    ratecode_id          INT,
    store_and_fwd_flag   STRING,
    pu_location_id       INT,
    do_location_id       INT,
    payment_type         INT,
    fare_amount          DOUBLE,
    extra                DOUBLE,
    mta_tax              DOUBLE,
    tip_amount           DOUBLE,
    tolls_amount         DOUBLE,
    total_amount         DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee          DOUBLE,
    cbd_congestion_fee   DOUBLE,
    pickup_zone          STRING,
    dropoff_zone         STRING
) USING iceberg
```

What changed compared to bronze and why: IDs and `passenger_count` are `INT` because they are categorical, not continuous measurements. Pickup/dropoff are `TIMESTAMP` for filtering, windows, and gold time bucketing. Kafka metadata is dropped—downstream analytics do not need it. `pickup_zone` and `dropoff_zone` are added after the zone lookup join. Naming is normalized (`vendor_id`, `pu_location_id`, etc.) for readability.

### Gold

Table DDL:

```sql
CREATE TABLE lakehouse.taxi.gold (
    pickup_hour    TIMESTAMP,
    pickup_zone    STRING,
    trip_count     LONG,
    avg_fare       DOUBLE,
    avg_distance   DOUBLE,
    total_revenue  DOUBLE
) USING iceberg
PARTITIONED BY (days(pickup_hour))
```

Aggregation logic: From silver, `pickup_hour = date_trunc('hour', pickup_datetime)`. Group by `pickup_hour` and `pickup_zone`. Metrics: `count(*)` as `trip_count`, `avg(fare_amount)`, `avg(trip_distance)`, `sum(total_amount)` as `total_revenue`. That supports queries filtered by hour and pickup zone.

---

## 2. Cleaning rules and enrichment

| Rule | Condition | Justification |
| --- | --- | --- |
| R1 | `pickup_datetime IS NOT NULL` | Trips without a start time are unusable for duration or time analytics. |
| R2 | `dropoff_datetime IS NOT NULL` | Same for end time. |
| R3 | `fare_amount >= 0` | Negative fares indicate bad or test data. |
| R4 | `trip_distance > 0` | Zero-distance rows are usually cancellations or noise. |
| R5 | `passenger_count` between 1 and 8 (and not null) | Matches realistic NYC taxi capacity; excludes null and zero. |
| R6 (dedup) | `dropDuplicates(vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id)` | Treats duplicate Kafka deliveries of the same trip as one silver row. |

Enrichment: Load `taxi_zone_lookup.parquet` (265 zones). Broadcast left joins: `pu_location_id` → `pickup_zone`, `do_location_id` → `dropoff_zone`, so unknown IDs remain as trips with `NULL` zone names instead of being dropped.

---

## 3. Streaming configuration

- Checkpoint paths: `proj2/.checkpoints/bronze` and `proj2/.checkpoints/silver` on the host (inside the container: `/home/jovyan/project/.checkpoints/...`). The project folder is Docker-mounted so you can delete these folders from Windows Explorer to reset streaming state—not the machine’s unrelated `/tmp`, which Spark inside the container does not use for these paths. Each checkpoint stores Structured Streaming metadata: for bronze, committed Kafka offsets per partition; for silver, progress for the Iceberg streaming scan of bronze (snapshots processed). Restarting with the same path resumes after the last committed batch.

- Trigger: `processingTime = "5 seconds"` for both streams. Balances latency and Iceberg commit frequency so we do not create an excessive number of tiny files.

- Output mode: Append only. Each micro-batch adds new rows; we do not use update or complete mode because the pipeline models immutable events and curated append-only silver rows.

- Watermark: Not used. Events are assumed sufficiently in-order from the producer/Kafka for this project; we do not need to drop late data in a windowed sense.

- Silver (batch catch-up, then stream): The notebook calls `process_silver_batch` once on a batch read of bronze so existing rows become silver before the stream starts. The silver `readStream` uses its checkpoint to continue from new bronze commits only, so those rows are not written twice. The silver restart test (producer off) shows no extra rows after stop/restart.

---

## 4. Gold table partitioning strategy

Gold is partitioned with `days(pickup_hour)`. Queries that slice by calendar day or week align with partition boundaries, so Iceberg can skip files outside the range. Hourly partitions would multiply small files; monthly partitions would be too coarse for daily reporting.

Iceberg snapshot history (`SELECT * FROM lakehouse.taxi.gold.history` — example run):

```
+-----------------------+-------------------+---------+-------------------+
|made_current_at        |snapshot_id        |parent_id|is_current_ancestor|
+-----------------------+-------------------+---------+-------------------+
|2026-04-03 09:53:56.056|5436408693480973246|NULL     |true               |
+-----------------------+-------------------+---------+-------------------+
```

Bronze and silver accumulate more snapshots over time; the notebook prints full `.history` for those tables as well.

---

## 5. Restart proof

With `produce.py` stopped (Ctrl+C) so no new messages arrive, we stop the streaming query, restart it with the same `checkpointLocation`, wait ~15 seconds, and compare `SELECT COUNT(*)` on the table.

| Layer | Rows before restart | Rows after restart | Rows added |
| --- | ---:| ---:| ---:|
| Bronze | 4,136 | 4,136 | 0 |
| Silver | 3,978 | 3,978 | 0 |

Conclusion: No duplicate rows from re-processing already committed inputs. Idempotency comes from Structured Streaming checkpoints and the saved Kafka offsets (bronze) and Iceberg source progress (silver), not from Iceberg merge/upsert on business keys. Bronze writes remain append to Iceberg.

Numbers from our demo run (partial ingest).

---

## 6. Custom scenario

GitHub issue requirement: For each parquet file replayed by the producer, rows 1–500 are sent without an `airport_fee` field; from row 501 onward the JSON includes `airport_fee: 1.75`. Bronze must accept both shapes without errors or row loss.

Implementation: In `produce.py`, each row is built from the parquet dict; TLC’s `Airport_fee` key is removed, `airport_fee` is omitted for `row_idx <= 500`, and `airport_fee = 1.75` is set for `row_idx > 500` so JSON keys match Spark’s lowercase `airport_fee`. In Spark, `StructField("airport_fee", DoubleType(), True)` and the bronze Iceberg column are nullable `DOUBLE`.

Verification: Query bronze with `airport_fee IS NULL` vs `IS NOT NULL`—the first 500 trips per file show NULL fee; later rows show 1.75. Total ingested count matches expectations for the producer run; both shapes land in one table.

---

## 7. How to run

Dependencies: Docker; Python 3 with `kafka-python-ng`, `pandas`, `pyarrow` (producer installs missing packages on first run). Parquet files in `data/` as listed below.

```bash
# Step 1: Start infrastructure
docker compose up -d
# Wait ~20s for Kafka, MinIO, Iceberg REST, Jupyter

# Step 2: Create Kafka topic (once)
docker exec kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9094 \
  --create --topic taxi-trips --partitions 3 --replication-factor 1"

# Step 3: Start the producer (from project root on the host)
python produce.py --rate 10
```

Step 4 — Run the pipeline: Open http://localhost:8888, open `streaming_pipeline.ipynb` (files under `~/project/` in the container), run cells in order. For restart proof cells, stop the producer first (Ctrl+C) so counts stay stable.

Data files (place under `data/`):

- `yellow_tripdata_2025-01.parquet`, `yellow_tripdata_2025-02.parquet`, `taxi_zone_lookup.parquet`

`.env` values for the grader (copy from `.env.example` and align with these if using defaults):

```env
JUPYTER_TOKEN=bdm
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```

Note: Producer in host mode uses `localhost:9094`; the notebook inside Docker uses `kafka:9092` for the Kafka bootstrap server.
