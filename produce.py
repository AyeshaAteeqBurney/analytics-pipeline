#!/usr/bin/env python3
"""
Replays NYC taxi trip rows as JSON events into a Kafka topic.

Reads all parquet files from data/inbox/ (or a specific file) row by row and publishes
each row as a JSON message. After row 500 of each file, adds airport_fee: 1.75 field.

Designed to run from inside the Docker Compose network (Jupyter terminal or
docker exec).

Usage
-----
From a Jupyter terminal (File → New Terminal):
    python project/produce.py                    # Finds all .parquet files in data/inbox/

From a host terminal:
    docker exec project2_jupyter python /home/jovyan/project/produce.py

Options:
    --data-dir  Directory containing parquet files (default: data/inbox/)
    --data-file Specific parquet file to replay    (optional; if set, uses this file only)
    --topic     Kafka topic to publish to          (default: taxi-trips)
    --bootstrap Kafka bootstrap server             (default: kafka:9092)
    --rate      Events per second                  (default: 5.0)
    --loop      Replay the file indefinitely       (flag, default: off)

Press Ctrl-C to stop.

Custom Scenario:
    - Rows 1-500 of each file: normal message (no airport_fee)
    - Rows 501+: add airport_fee: 1.75 to each message
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

# Self-install dependencies so the script works from a fresh Jupyter terminal
# without needing to run the notebook setup cell first.
def _ensure(pkg, import_name=None):
    import importlib.util
    import subprocess
    if importlib.util.find_spec(import_name or pkg) is None:
        print(f"Installing {pkg} …")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("kafka-python-ng", "kafka")
_ensure("pandas")
_ensure("pyarrow")

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _json_default(obj):
    """Fallback serialiser for types json.dumps can't handle."""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if hasattr(obj, "item"):          # numpy scalars
        return obj.item()
    raise TypeError(f"Not serialisable: {type(obj)}")


def row_to_json(row: dict) -> bytes:
    return json.dumps(row, default=_json_default).encode()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay taxi trip parquet rows into Kafka.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--data-dir",  default="data/inbox",
                        help="Directory containing parquet files (relative to this script).")
    parser.add_argument("--data-file", default=None,
                        help="Specific parquet file to replay (relative to script). "
                             "If omitted, discovers all .parquet files in --data-dir.")
    parser.add_argument("--topic",     default="taxi-trips",
                        help="Target Kafka topic.")
    parser.add_argument("--bootstrap", default="kafka:9092",
                        help="Kafka bootstrap server.")
    parser.add_argument("--rate",      type=float, default=5.0,
                        help="Events per second.")
    parser.add_argument("--loop",      action="store_true",
                        help="Replay the file indefinitely (Ctrl-C to stop).")
    args = parser.parse_args()

    # Resolve paths relative to this script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Determine which parquet files to process
    if args.data_file:
        # Single file specified by user
        data_path = os.path.join(script_dir, args.data_file)
        if not os.path.exists(data_path):
            sys.exit(f"Data file not found: {data_path}")
        parquet_files = [data_path]
    else:
        # Discover all parquet files in data_dir (except taxi_zone_lookup)
        data_dir = os.path.join(script_dir, args.data_dir)
        if not os.path.exists(data_dir):
            sys.exit(
                f"Data directory not found: {data_dir}\n"
                f"Place the parquet files in: {data_dir}"
            )
        
        all_files = [
            os.path.join(data_dir, f)
            for f in os.listdir(data_dir)
            if f.endswith(".parquet") and "taxi_zone_lookup" not in f
        ]
        
        if not all_files:
            sys.exit(
                f"No parquet files found in {data_dir}\n"
                f"(excluding taxi_zone_lookup.parquet)"
            )
        
        parquet_files = sorted(all_files)
        print(f"Discovered {len(parquet_files)} parquet file(s):")
        for f in parquet_files:
            print(f"  - {os.path.basename(f)}")

    interval = 1.0 / args.rate

    print(f"\nConnecting to Kafka at {args.bootstrap} …")
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            key_serializer=lambda k: str(k).encode(),
            value_serializer=row_to_json,
            acks=1,
            retries=3,
        )
    except NoBrokersAvailable:
        sys.exit(
            "Could not reach Kafka. Make sure the stack is running:\n"
            "  docker compose up -d\n"
            "and that you are inside the Docker network (Jupyter terminal or docker exec)."
        )

    print(f"Topic    : {args.topic}")
    print(f"Rate     : {args.rate:.1f} events/s")
    print(f"Loop     : {args.loop}")
    print(f"Files    : {len(parquet_files)}")
    print("Press Ctrl-C to stop.\n")

    sent       = 0
    pass_num   = 0
    t0_overall = time.monotonic()

    try:
        while True:
            pass_num += 1
            if args.loop:
                print(f"── Pass {pass_num} ──────────────────────────────────")

            for parquet_path in parquet_files:
                file_name = os.path.basename(parquet_path)
                print(f"Processing: {file_name}")
                df = pd.read_parquet(parquet_path)
                print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")

                for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
                    msg = row.to_dict()
                    
                    # Custom scenario: add airport_fee after row 500
                    if row_idx > 500:
                        msg["airport_fee"] = 1.75
                    
                    # Use VendorID as the partition key so trips from the same
                    # vendor land on the same partition (ordering guarantee).
                    key = str(msg.get("VendorID", ""))
                    producer.send(args.topic, key=key, value=msg)
                    sent += 1

                    if sent == 1 or sent % 100 == 0:
                        elapsed = time.monotonic() - t0_overall
                        rate    = sent / elapsed
                        has_fee = "airport_fee" if row_idx > 500 else "  -"
                        print(
                            f"[{sent:>6} sent | {elapsed:>6.1f}s | {rate:>5.1f} ev/s]  "
                            f"{has_fee}  "
                            f"pickup={msg.get('tpep_pickup_datetime', '?')}  "
                            f"PU={msg.get('PULocationID')}  DO={msg.get('DOLocationID')}  "
                            f"fare=${msg.get('fare_amount')}"
                        )

                    time.sleep(interval)

            if not args.loop:
                break

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        elapsed = time.monotonic() - t0_overall
        print(f"\nStopped — {sent:,} events in {elapsed:.0f}s "
              f"({sent / elapsed:.1f} ev/s, {pass_num} pass(es)).")


if __name__ == "__main__":
    main()
