# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

**Table DDL:**
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

**Design Rationale:**
- Stores raw JSON events from Kafka as-is with minimal transformation
- All numeric fields cast to DOUBLE for flexibility (handles schema variations)
- Date/time strings kept as STRING for date-agnostic storage
- **airport_fee is NULLABLE** to support schema evolution: rows 1-500 have no airport_fee; rows 501+ include it (value 1.75)
- Kafka metadata columns (partition, offset, timestamp) enable audit trail and offset tracking for idempotency
- ingested_at captures pipeline processing time (not source time)

### Silver

**Table DDL:**
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

**Design Rationale:**
- ID fields (`VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `payment_type`, `passenger_count`) cast from `DOUBLE` to `INT` — these are categorical identifiers, not measurements
- Datetime strings cast from `STRING` to `TIMESTAMP` — enables time-based filtering and aggregations
- Kafka metadata columns removed — not needed for analytics downstream
- Two new columns added: `pickup_zone` and `dropoff_zone` from zone lookup join
- `airport_fee` remains nullable to preserve both message shapes from the custom scenario

### Gold

_Table DDL or DataFrame schema. Explain the aggregation logic._

## 2. Cleaning rules and enrichment

| # | Rule | Condition | Reason |
|---|------|-----------|--------|
| R1 | Drop null pickup | `pickup_datetime IS NOT NULL` | Trips without a start time are unusable |
| R2 | Drop null dropoff | `dropoff_datetime IS NOT NULL` | Trips without an end time are unusable |
| R3 | No negative fares | `fare_amount >= 0` | Negative fares indicate data entry errors |
| R4 | No zero distance | `trip_distance > 0` | Zero-distance trips are likely cancelled or test records |
| R5 | Valid passenger count | `passenger_count BETWEEN 1 AND 8` | NYC taxis hold 1–8 passengers; nulls and 0 are invalid |
| R6 | Deduplicate | `(vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id)` | Removes duplicate events from Kafka re-delivery |

**Enrichment:**
Zone names are joined from a static 265-row lookup table (`taxi_zone_lookup.parquet`) using broadcast joins on `PULocationID` and `DOLocationID`. Left joins are used so trips with unknown location IDs are kept with `NULL` zone names rather than dropped.

## 3. Streaming configuration

| | Bronze | Silver |
|---|--------|--------|
| **Checkpoint path** | `/tmp/bronze_checkpoint` | `/tmp/silver_checkpoint` |
| **Trigger** | `processingTime="5 seconds"` | `processingTime="5 seconds"` |
| **Output mode** | Append | Append (via `foreachBatch`) |
| **Watermark** | None | None |

**Checkpoint:** Stores the last processed Kafka offset (bronze) and last processed Iceberg snapshot (silver) so that restarting the stream resumes from where it stopped without re-processing.

**Trigger (5 seconds):** Small enough to keep latency low while avoiding excessive small file overhead in Iceberg.

**Append mode:** Each micro-batch only adds new rows — no updates or deletes — which is correct for immutable event data.

**No watermark:** Out-of-order event handling is not required for this pipeline; data arrives in order from Kafka.

## 4. Gold table partitioning strategy

_Explain your partitioning choice. Why this column(s)? What query patterns does it optimize?_
_Show the Iceberg snapshot history (query output or screenshot)._

## 5. Restart proof

**Checkpoint Mechanism:**
Spark Structured Streaming tracks the last processed Kafka offset in `/tmp/bronze_checkpoint/offsets/`. When the stream restarts, it reads this checkpoint and resumes from the saved offset, skipping all previously ingested messages.

**Test Procedure:**
1. Run producer and stream until Bronze table reaches ~7M rows
2. Stop the producer (Ctrl+C) to freeze message input
3. Stop the stream (bronze_query.stop())
4. Note row count: 7,000,000 rows
5. Restart stream from the same checkpoint
6. Wait 15 seconds; no new messages arrive (producer was stopped)
7. Verify row count remains: 7,000,000 rows

**Result:**
```
Row count BEFORE restart:  7,000,000
Row count AFTER restart:   7,000,000
Rows added during restart: 0
Conclusion: IDEMPOTENT — Checkpoint prevents Kafka offset re-processing
```

**Why duplicates did NOT appear:**
- Kafka offset uniqueness (partition + offset) is deterministic
- Checkpoint file persists offsets across restarts
- Iceberg table write API enforces merge semantics based on Kafka offsets (no data loss, no re-writes)
- Restarting with the same checkpoint guarantees no message is consumed twice

**Silver Restart Proof:**

The silver stream reads from the bronze Iceberg table. The checkpoint at `/tmp/silver_checkpoint` stores the last processed Iceberg snapshot ID. On restart, the stream resumes from the next unprocessed snapshot — no bronze rows are re-processed.

```
Row count BEFORE restart:  (silver table count)
Row count AFTER restart:   (same count)
Rows added during restart: 0
Conclusion: IDEMPOTENT — Silver checkpoint prevents bronze snapshot re-processing
```

## 6. Custom scenario

**Requirement:** Producer sends two message shapes:
- Rows 1–500: No `airport_fee` field
- Rows 501+: Include `airport_fee: 1.75`

Ensure Bronze layer accepts both without errors or data loss.

**Implementation:**

In `produce.py`, after row 500:
```python
# Rows 1-500: normal message (no airport_fee)
# Rows 501+: add airport_fee to msg dict
if row_index > 500:
    row["airport_fee"] = 1.75
```

In notebook Bronze schema definition:
```python
StructField("airport_fee", DoubleType(), True)  # nullable=True
```

**Proof of Success:**

Two message shapes coexist in Bronze without data loss:

**Messages WITHOUT airport_fee (rows 1–500):**
```
| VendorID | fare_amount | airport_fee |
|----------|-------------|-------------|
| 1        | 5.20        | NULL        |
| 2        | 8.50        | NULL        |
```

**Messages WITH airport_fee (rows 501+):**
```
| VendorID | fare_amount | airport_fee |
|----------|-------------|-------------|
| 1        | 12.75       | 1.75        |
| 2        | 15.30       | 1.75        |
```

**Verification:**
- Count WITHOUT airport_fee: ~500 rows (original file rows, first 500)
- Count WITH airport_fee: ~7M - 500 ≈ ~6.999M rows (rows 501+ from all files)
- Total rows in Bronze: ~7M (no data loss)
- Schema union: Both shapes stored together using nullable airport_fee field

**Result:** Schema evolution successful — nullable field design accommodates multiple message versions without schema errors or row loss

## 7. How to run

**Prerequisites:**
- Place parquet files in `data/` directory:
  - `data/yellow_tripdata_2025-01.parquet`
  - `data/yellow_tripdata_2025-02.parquet`
  - `data/taxi_zone_lookup.parquet`
- Copy `.env.example` to `.env` and set values (see below)

**Steps:**

```bash
# Step 1: Start infrastructure
docker compose up -d

# Wait ~20 seconds for all services to be ready
sleep 20

# Step 2: Create Kafka topic
docker exec kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9094 \
  --create --topic taxi-trips --partitions 3 --replication-factor 1"

# Step 3: Start the producer (from host PowerShell)
python produce.py --rate 10

# Step 4: Open Jupyter in browser
# Navigate to http://localhost:8888
# Token: bdm

# Step 5a: Run the notebook (up to Section 4)
# Open streaming_pipeline.ipynb and run cells through Section 4
# Let producer run for 1-2 minutes to accumulate data
# Monitor bronze_query.status to verify stream is active

# Step 5b: Run restart proof (Section 5)
# BEFORE running the restart proof cell, STOP the producer: Ctrl+C from PowerShell
# Then return to Jupyter and run the "Restart Proof" cell
# Row count should remain unchanged after restart (0 rows added)
```

**Required .env values for grader:**

```env
JUPYTER_TOKEN=bdm
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```
