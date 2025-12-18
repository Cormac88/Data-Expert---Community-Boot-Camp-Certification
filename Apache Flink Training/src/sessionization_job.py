"""
sessionization_job.py

PyFlink DataStream job that:
- reads events (example file-source or switch to Kafka),
- sessionizes by (ip, host) with a 5-minute gap,
- emits tuples: (ip, host, session_start_ms, session_end_ms, event_count, session_id)
- writes session rows to PostgreSQL using psycopg2 in a RichSinkFunction.

Timestamps are expected in epoch milliseconds (int).
"""

import json
import hashlib
import time
from datetime import datetime
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext, RichSinkFunction
from pyflink.common import Types, Duration
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import TimeCharacteristic
from pyflink.datastream.state import ValueStateDescriptor

# ---------------------------------------
# Configuration (edit for your environment)
# ---------------------------------------
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "techcreator",
    "user": "flink_user",
    "password": "flink_password"
}

# File source path for quick testing. Each line: JSON event with fields: ip, host, ts, path
SAMPLE_INPUT_FILE = "/data/sample_events.jsonl"

# Whether the job should use file source (True) or Kafka (False). Default: True for testability.
USE_FILE_SOURCE = True

# Watermark lateness allowance (out-of-orderness)
MAX_OUT_OF_ORDER_MS = 10_000  # 10 seconds

# Session gap
SESSION_GAP_MINUTES = 5


# -------------------------
# Example Event schema
# -------------------------
# {
#   "ip": "1.2.3.4",
#   "host": "zachwilson.techcreator.io",
#   "ts": 1697059200000,   # epoch millis
#   "path": "/home"
# }
# -------------------------

class WebEventSource:
    """
    Simple generator source for local testing.
    Uses env.from_collection in main() instead of a SourceFunction to keep example simple.
    """

    @staticmethod
    def read_sample_file(path: str):
        with open(path, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                yield json.loads(line)


class SessionProcessFunction(ProcessWindowFunction):
    """
    Produces tuples:
      (ip, host, session_start_ms, session_end_ms, event_count, session_id)
    """

    def process(self, key, context, elements: Iterable, out):
        ip, host = key
        count = 0
        for _ in elements:
            count += 1

        start_ms = int(context.window().start)
        end_ms = int(context.window().end)

        # deterministic session id
        session_id = hashlib.md5(f"{ip}|{host}|{start_ms}".encode("utf-8")).hexdigest()

        out.collect((ip, host, start_ms, end_ms, count, session_id))


class PostgresSink(RichSinkFunction):
    """
    Simple PostgreSQL sink using psycopg2.
    Each tuple must be: (ip, host, session_start_ms, session_end_ms, event_count, session_id)
    """

    def __init__(self, pg_config):
        self.pg_config = pg_config
        self.conn = None
        self.cur = None
        self.batch = []
        self.batch_size = 100

    def open(self, runtime_context: RuntimeContext):
        # Import here so job does not fail at import-time if psycopg2 missing in client environment
        import psycopg2
        from psycopg2.extras import execute_values

        self.psycopg2 = psycopg2
        self.execute_values = execute_values

        self.conn = psycopg2.connect(
            host=self.pg_config["host"],
            port=self.pg_config["port"],
            dbname=self.pg_config["database"],
            user=self.pg_config["user"],
            password=self.pg_config["password"]
        )
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

    def invoke(self, value, context):
        # value is a tuple (ip, host, start_ms, end_ms, event_count, session_id)
        self.batch.append(value)
        if len(self.batch) >= self.batch_size:
            self._flush_batch()

    def _flush_batch(self):
        if not self.batch:
            return

        rows = []
        for (ip, host, start_ms, end_ms, event_count, session_id) in self.batch:
            start_ts = datetime.utcfromtimestamp(start_ms / 1000.0)
            end_ts = datetime.utcfromtimestamp(end_ms / 1000.0)
            rows.append((ip, host, start_ts, end_ts, int(event_count), session_id))

        insert_sql = """
        INSERT INTO sessionized_events (ip, host, session_start, session_end, event_count, session_id)
        VALUES %s
        ON CONFLICT (host, ip, session_start) DO UPDATE
          SET session_end = EXCLUDED.session_end,
              event_count = EXCLUDED.event_count,
              session_id = EXCLUDED.session_id;
        """

        try:
            self.execute_values(self.cur, insert_sql, rows)
            self.conn.commit()
            self.batch = []
        except Exception:
            self.conn.rollback()
            raise

    def close(self):
        try:
            self._flush_batch()
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()


def build_env_and_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Use event time
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.get_config().set_auto_watermark_interval(1000)

    # ---------------------
    # Source
    # ---------------------
    if USE_FILE_SOURCE:
        # small sample ingestion for tests
        sample_events = list(WebEventSource.read_sample_file(SAMPLE_INPUT_FILE))
        # convert JSON dicts into tuples and create collection
        # expected types: (ip:str, host:str, ts:int, path:str)
        data = []
        for e in sample_events:
            # ensure required fields present
            ip = e["ip"]
            host = e["host"]
            ts = int(e["ts"])  # epoch millis
            path = e.get("path", "")
            data.append((ip, host, ts, path))
        ds = env.from_collection(collection=data,
                                 type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING()]))
    else:
        # Placeholder: show how to connect Kafka (user must configure)
        from pyflink.datastream.connectors import FlinkKafkaConsumer
        from pyflink.common.serialization import SimpleStringSchema
        kafka_props = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "flink-sessionizer"
        }
        kafka_consumer = FlinkKafkaConsumer(
            topics="web_events",
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )
        ds = env.add_source(kafka_consumer) \
                .map(lambda s: json.loads(s), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # --------------
    # Timestamps & Watermarks
    # --------------
    wm_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_milliseconds(MAX_OUT_OF_ORDER_MS)) \
        .with_timestamp_assigner(lambda e, ts: e[2])  # index 2 is ts in our tuples

    timed = ds.assign_timestamps_and_watermarks(wm_strategy)

    # --------------
    # Key by (ip, host) and session window
    # --------------
    keyed = timed.key_by(lambda e: (e[0], e[1]))  # e[0]=ip, e[1]=host

    sessions = keyed.window(EventTimeSessionWindows.with_gap(Time.minutes(SESSION_GAP_MINUTES))) \
        .process(SessionProcessFunction(),
                 output_type=Types.TUPLE([
                     Types.STRING(),  # ip
                     Types.STRING(),  # host
                     Types.LONG(),    # session_start_ms
                     Types.LONG(),    # session_end_ms
                     Types.INT(),     # event_count
                     Types.STRING()   # session_id
                 ]))

    # --------------
    # Sink to Postgres
    # --------------
    pg_sink = PostgresSink(POSTGRES_CONFIG)
    sessions.add_sink(pg_sink).name("postgres-session-sink")

    return env


def main():
    env = build_env_and_pipeline()
    env.execute("PyFlink Sessionization -> Postgres")


if __name__ == "__main__":
    main()
