"""Shared settings for every pipeline stage.

All paths are resolved relative to the project root so the scripts work from any
working directory. MQTT uses the fixed local broker at localhost:1883; only the
topic root can be overridden with MQTT_TOPIC_ROOT.
"""
import os
import signal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Inputs
A1_DB = PROJECT_ROOT / "data" / "a1" / "ev_chargers_a1.duckdb"   # Assignment 1 database (read-only copy)
GENERATOR = PROJECT_ROOT / "generator" / "generate_dataset.py"    # provided generator, unmodified

# Task 1 outputs
STATIONS_CSV = PROJECT_ROOT / "data" / "generated" / "stations_input.csv"
GENERATED_DIR = PROJECT_ROOT / "data" / "generated"

# Task 2 outputs
STREAM_DIR = PROJECT_ROOT / "data" / "stream"
EVENTS_JSONL = STREAM_DIR / "events.jsonl"
PUBLISH_CHECKPOINT = STREAM_DIR / "publisher_checkpoint.json"
PUBLISH_RUN_LOG = STREAM_DIR / "publish_runs.jsonl"          # audit trail: one line per publishing run

# Task 4 outputs
DB_PATH = PROJECT_ROOT / "data" / "db" / "ev_chargers.duckdb"     # A1 database extended with event tables
SCHEMA_A1_SQL = PROJECT_ROOT / "sql" / "01_schema_a1.sql"
SCHEMA_A2_SQL = PROJECT_ROOT / "sql" / "02_schema_a2_events.sql"

# Simulation window: one day of AEMO NSW1-based synthetic activity.
SIM_START = "2026-10-01"
SIM_DAYS = 1

# MQTT always connects to the local Mosquitto listener.
MQTT_HOST = "localhost"
MQTT_PORT = 1883
TOPIC_ROOT = os.environ.get("MQTT_TOPIC_ROOT", "COMP5339/TUT19_G07/ev")
QOS = 1                      # at-least-once delivery; duplicates are removed by event_id on ingest
MIN_PUBLISH_DELAY_S = 0.1    # assignment requirement: at least 0.1 s between published messages



EVENT_TYPES = ("price_update", "session_start", "meter", "session_end")
# Tie-break order for events that share a timestamp: a session must start before its first
# meter reading, and a price change applies before sessions starting in the same instant.
EVENT_ORDER = {"price_update": 0, "session_start": 1, "meter": 2, "session_end": 3}


def topic_for(charger_id, event_type):
    """Topic hierarchy: <root>/<charger_id>/<event_type>, e.g. COMP5339/TUT19_G07/ev/42/meter."""
    return f"{TOPIC_ROOT}/{charger_id}/{event_type}"


def subscription_topic():
    """Wildcard covering every charger and every event type."""
    return f"{TOPIC_ROOT}/+/+"


def stop_on_sigterm():
    """Treat SIGTERM (e.g. `kill <pid>`) like Ctrl-C, so every stage shuts down gracefully:
    the publisher drains un-acknowledged messages and the writer flushes its queue to DuckDB."""
    def handler(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, handler)
