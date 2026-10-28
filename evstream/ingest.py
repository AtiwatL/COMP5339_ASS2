"""Validate MQTT event messages and store them transactionally in DuckDB.

One EventIngestor owns one writable connection.  Each call to ingest() either
commits the audit row, typed event row and current-state update together, or
rolls all of them back.  Re-delivery of an identical event_id is idempotent.
"""

from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from evstream import config


AEST = timezone(timedelta(hours=10))

# ---------------------------------------------------------------------------
# MESSAGE VALIDATION
# Validate the MQTT envelope and event-specific `data` before opening a
# database transaction.  DuckDB constraints remain the final safety boundary,
# but this layer reports clearer errors for malformed JSON messages.
# ---------------------------------------------------------------------------

# Fields required inside the `data` object for each supported event type.
REQUIRED_DATA_FIELDS = {
    "price_update": {"price_per_kwh", "wholesale_per_kwh", "currency"},
    "session_start": {"vehicle_model", "price_per_kwh"},
    "meter": {"voltage_v", "current_a", "power_kw", "energy_kwh", "soc_pct"},
    "session_end": {
        "start_ts", "charge_end_ts", "charge_minutes", "energy_kwh", "avg_kw",
        "peak_kw", "price_per_kwh", "cost_aud",
    },
}


class MessageValidationError(ValueError):
    """The MQTT payload does not conform to message schema version 1."""


class EventConsistencyError(RuntimeError):
    """A valid message conflicts with data already stored in DuckDB."""


@dataclass(frozen=True)
class IngestResult:
    status: str                 # "stored" or "duplicate"
    event_id: str
    event_type: str
    seq: int


def _require_int(value: Any, field: str, *, positive: bool = False) -> int:
    """Return an integer field, rejecting booleans and optionally non-positive values."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise MessageValidationError(f"{field} must be an integer")
    if positive and value <= 0:
        raise MessageValidationError(f"{field} must be greater than zero")
    return value


def _require_number(
    value: Any, field: str, *, minimum: float | None = None, allow_none: bool = False
) -> float | None:
    """Return a finite float after applying nullability and minimum-value rules."""
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MessageValidationError(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise MessageValidationError(f"{field} must be finite")
    if minimum is not None and number < minimum:
        raise MessageValidationError(f"{field} must be at least {minimum}")
    return number


def _require_text(value: Any, field: str, *, allow_none: bool = False) -> str | None:
    """Return a non-blank string, or None only when the field explicitly allows it."""
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MessageValidationError(f"{field} must be a non-empty string")
    return value


def _parse_timestamp(value: Any, field: str) -> datetime:
    """Parse ISO-8601 input and normalize offset-aware values to naive AEST time."""
    text = _require_text(value, field)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise MessageValidationError(f"{field} must be an ISO-8601 timestamp") from exc
    # Event storage and dashboard queries use naive AEMO-market wall time.  If
    # an offset is supplied, normalize it to fixed AEST before removing tzinfo.
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(AEST).replace(tzinfo=None)
    return parsed


def _decode(raw_message: str | bytes | bytearray | dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Decode MQTT bytes/JSON text and return the object plus canonical JSON."""
    if isinstance(raw_message, dict):
        message = raw_message
    else:
        if isinstance(raw_message, (bytes, bytearray)):
            try:
                raw_message = bytes(raw_message).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise MessageValidationError("message payload must be UTF-8") from exc
        if not isinstance(raw_message, str):
            raise MessageValidationError("message payload must be JSON text, bytes, or a dictionary")
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            raise MessageValidationError("message payload is not valid JSON") from exc
    if not isinstance(message, dict):
        raise MessageValidationError("message payload must be a JSON object")
    # Canonical JSON makes conflict checks independent of whitespace and key order.
    return message, json.dumps(message, separators=(",", ":"), sort_keys=True)


def _validate(raw_message: str | bytes | bytearray | dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one version-1 MQTT event for database storage."""
    message, canonical_json = _decode(raw_message)
    # Every event uses the same outer envelope; only `data` varies by type.
    required = {
        "v", "seq", "event_id", "event_type", "event_ts", "charger_id",
        "connector_id", "session_id", "data",
    }
    missing = required - message.keys()
    if missing:
        raise MessageValidationError("missing envelope fields: " + ", ".join(sorted(missing)))
    if message["v"] != 1:
        raise MessageValidationError(f"unsupported message version: {message['v']!r}")

    seq = _require_int(message["seq"], "seq", positive=True)
    event_id = _require_text(message["event_id"], "event_id")
    event_type = _require_text(message["event_type"], "event_type")
    if event_type not in config.EVENT_TYPES:
        raise MessageValidationError(f"unsupported event_type: {event_type!r}")
    event_ts = _parse_timestamp(message["event_ts"], "event_ts")
    charger_id = _require_int(message["charger_id"], "charger_id", positive=True)

    connector_id = message["connector_id"]
    session_id = message["session_id"]
    # A tariff belongs to a charger.  Session events additionally identify the
    # physical connector and charging session to which they belong.
    if event_type == "price_update":
        if connector_id is not None or session_id is not None:
            raise MessageValidationError("price_update must not have connector_id or session_id")
    else:
        connector_id = _require_int(connector_id, "connector_id", positive=True)
        session_id = _require_text(session_id, "session_id")

    data = message["data"]
    if not isinstance(data, dict):
        raise MessageValidationError("data must be a JSON object")
    missing_data = REQUIRED_DATA_FIELDS[event_type] - data.keys()
    if missing_data:
        raise MessageValidationError(
            f"{event_type} missing data fields: " + ", ".join(sorted(missing_data))
        )

    normalized: dict[str, Any] = {
        "seq": seq,
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": event_ts,
        "charger_id": charger_id,
        "connector_id": connector_id,
        "session_id": session_id,
        "payload_json": canonical_json,
    }

    if event_type == "price_update":
        normalized.update(
            price_per_kwh=_require_number(data["price_per_kwh"], "data.price_per_kwh", minimum=0),
            wholesale_per_kwh=_require_number(
                data["wholesale_per_kwh"], "data.wholesale_per_kwh"
            ),
            currency=_require_text(data["currency"], "data.currency"),
        )
    elif event_type == "session_start":
        normalized.update(
            vehicle_model=_require_text(data["vehicle_model"], "data.vehicle_model"),
            price_per_kwh=_require_number(data["price_per_kwh"], "data.price_per_kwh", minimum=0),
        )
    elif event_type == "meter":
        normalized.update(
            voltage_v=_require_number(data["voltage_v"], "data.voltage_v", minimum=0),
            current_a=_require_number(data["current_a"], "data.current_a", minimum=0),
            power_kw=_require_number(data["power_kw"], "data.power_kw", minimum=0),
            energy_kwh=_require_number(data["energy_kwh"], "data.energy_kwh", minimum=0),
            soc_pct=_require_number(data["soc_pct"], "data.soc_pct", minimum=0),
        )
        if normalized["soc_pct"] > 100:
            raise MessageValidationError("data.soc_pct must not exceed 100")
    else:  # session_end
        normalized.update(
            start_ts=_parse_timestamp(data["start_ts"], "data.start_ts"),
            charge_end_ts=_parse_timestamp(data["charge_end_ts"], "data.charge_end_ts"),
            charge_minutes=_require_number(data["charge_minutes"], "data.charge_minutes", minimum=0),
            energy_kwh=_require_number(data["energy_kwh"], "data.energy_kwh", minimum=0),
            avg_kw=_require_number(data["avg_kw"], "data.avg_kw", minimum=0),
            peak_kw=_require_number(data["peak_kw"], "data.peak_kw", minimum=0),
            price_per_kwh=_require_number(data["price_per_kwh"], "data.price_per_kwh", minimum=0),
            cost_aud=_require_number(data["cost_aud"], "data.cost_aud", minimum=0),
        )
        if not normalized["start_ts"] <= normalized["charge_end_ts"] <= event_ts:
            raise MessageValidationError(
                "session_end times must satisfy start_ts <= charge_end_ts <= event_ts"
            )
    return normalized


# ---------------------------------------------------------------------------
# DATABASE INGESTION
# Store one validated event atomically in three layers:
#   1. event_log             - immutable original event/audit envelope
#   2. typed event table     - queryable price/start/meter/end attributes
#   3. charger_live_state    - small mutable cache used by the dashboard
# If any operation fails, ingest() rolls back all three layers.
# ---------------------------------------------------------------------------

class EventIngestor:
    """Validate and atomically persist events through one writable DuckDB connection."""

    def __init__(self, db_path: str | Path = config.DB_PATH):
        """Open the A2 database and prepare the connection for single-writer ingestion."""
        self.db_path = Path(db_path)
        if not self.db_path.is_file():
            raise FileNotFoundError(
                f"Assignment 2 database not found: {self.db_path}; run python -m evstream.db first"
            )
        self._con = duckdb.connect(str(self.db_path))
        self._con.execute("LOAD spatial")
        self._lock = threading.Lock()
        self._closed = False

    def close(self) -> None:
        """Close the owned connection once; repeated calls are safe."""
        with self._lock:
            if not self._closed:
                self._con.close()
                self._closed = True

    def __enter__(self) -> "EventIngestor":
        """Return this ingestor for use in a context-manager block."""
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """Close the database connection when leaving a context-manager block."""
        self.close()

    def ingest(self, raw_message: str | bytes | bytearray | dict[str, Any]) -> IngestResult:
        """Validate one message and commit all history/live-state changes atomically.

        Returns ``stored`` for a new event and ``duplicate`` for an identical
        QoS 1 redelivery. Validation happens before the transaction; any database
        or consistency failure after it begins rolls back every event change.
        """
        # Reject malformed messages before acquiring the writer lock or opening
        # a transaction.  Database-dependent checks occur inside the transaction.
        event = _validate(raw_message)
        with self._lock:
            if self._closed:
                raise RuntimeError("EventIngestor is closed")
            self._con.execute("BEGIN TRANSACTION")
            try:
                result = self._ingest_transaction(event)
                self._con.execute("COMMIT")
                return result
            except Exception:
                self._con.execute("ROLLBACK")
                raise

    def _ingest_transaction(self, e: dict[str, Any]) -> IngestResult:
        """Perform deduplication, consistency checks, typed storage and state updates."""
        # MQTT QoS 1 may deliver the same message more than once.  An identical
        # event_id is a harmless duplicate; reused identity with changed content
        # is a consistency error and must not rewrite immutable history.
        existing = self._con.execute(
            """
            SELECT seq, event_type, event_ts, charger_id, connector_id, session_id,
                   payload_json::VARCHAR
            FROM event_log WHERE event_id = ?
            """,
            [e["event_id"]],
        ).fetchone()
        if existing is not None:
            stored_payload = json.loads(existing[6])
            incoming_payload = json.loads(e["payload_json"])
            if existing[:6] != (
                e["seq"], e["event_type"], e["event_ts"], e["charger_id"],
                e["connector_id"], e["session_id"],
            ) or stored_payload != incoming_payload:
                raise EventConsistencyError(
                    f"event_id {e['event_id']!r} was reused with different content"
                )
            return IngestResult("duplicate", e["event_id"], e["event_type"], e["seq"])

        # Sequence is the unique global publishing position.  Two different
        # events are not allowed to claim the same sequence number.
        seq_owner = self._con.execute(
            "SELECT event_id FROM event_log WHERE seq = ?", [e["seq"]]
        ).fetchone()
        if seq_owner is not None:
            raise EventConsistencyError(
                f"seq {e['seq']} already belongs to event_id {seq_owner[0]!r}"
            )
        # Synthetic events may refer only to chargers imported from Assignment 1.
        if self._con.execute(
            "SELECT 1 FROM charger WHERE charger_id = ?", [e["charger_id"]]
        ).fetchone() is None:
            raise EventConsistencyError(f"unknown Assignment 1 charger_id {e['charger_id']}")

        # First store the common audit envelope and unchanged canonical payload.
        self._con.execute(
            """
            INSERT INTO event_log
                (event_id, seq, event_type, event_ts, charger_id, connector_id,
                 session_id, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                e["event_id"], e["seq"], e["event_type"], e["event_ts"],
                e["charger_id"], e["connector_id"], e["session_id"], e["payload_json"],
            ],
        )

        # Then route the event to exactly one typed storage function.
        dispatch = {
            "price_update": self._store_price,
            "session_start": self._start_session,
            "meter": self._store_meter,
            "session_end": self._end_session,
        }
        dispatch[e["event_type"]](e)
        self._touch_live_state(e)
        return IngestResult("stored", e["event_id"], e["event_type"], e["seq"])

    def _store_price(self, e: dict[str, Any]) -> None:
        """Append a price event and advance the live tariff unless the event is older."""
        self._con.execute(
            """
            INSERT INTO price_event
                (event_id, charger_id, event_ts, price_per_kwh, wholesale_per_kwh, currency)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                e["event_id"], e["charger_id"], e["event_ts"], e["price_per_kwh"],
                e["wholesale_per_kwh"], e["currency"],
            ],
        )
        # An older bootstrap/replay price remains valid history but must not
        # replace a newer price already displayed by the dashboard.
        self._con.execute(
            """
            UPDATE charger_live_state
            SET current_price = ?, wholesale_price = ?, price_ts = ?
            WHERE charger_id = ? AND (price_ts IS NULL OR ? >= price_ts)
            """,
            [
                e["price_per_kwh"], e["wholesale_per_kwh"], e["event_ts"],
                e["charger_id"], e["event_ts"],
            ],
        )

    def _start_session(self, e: dict[str, Any]) -> None:
        """Append a unique session/start event and increment the charger's live count."""
        if self._con.execute(
            "SELECT 1 FROM charging_session_core WHERE session_id = ?", [e["session_id"]]
        ).fetchone() is not None:
            raise EventConsistencyError(
                f"session_id {e['session_id']!r} already exists with another start event"
            )
        self._con.execute(
            """
            INSERT INTO charging_session_core
                (session_id, charger_id, connector_id, vehicle_model, price_per_kwh)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                e["session_id"], e["charger_id"], e["connector_id"],
                e["vehicle_model"], e["price_per_kwh"],
            ],
        )
        self._con.execute(
            """
            INSERT INTO session_start_event (event_id, session_id, start_ts)
            VALUES (?, ?, ?)
            """,
            [e["event_id"], e["session_id"], e["event_ts"]],
        )
        self._con.execute(
            """
            UPDATE charger_live_state
            SET active_sessions = active_sessions + 1
            WHERE charger_id = ?
            """,
            [e["charger_id"]],
        )

    def _matching_session(self, e: dict[str, Any]) -> tuple[Any, ...]:
        """Return an existing parent session after checking charger and connector identity."""
        session = self._con.execute(
            """
            SELECT charger_id, connector_id, start_ts, end_ts, price_per_kwh
            FROM charging_session WHERE session_id = ?
            """,
            [e["session_id"]],
        ).fetchone()
        if session is None:
            raise EventConsistencyError(
                f"event {e['event_id']!r} refers to unknown session {e['session_id']!r}"
            )
        if session[0] != e["charger_id"] or session[1] != e["connector_id"]:
            raise EventConsistencyError(
                f"event {e['event_id']!r} charger/connector does not match its session"
            )
        return session

    def _store_meter(self, e: dict[str, Any]) -> None:
        """Append telemetry after confirming its timestamp falls within the session."""
        session = self._matching_session(e)
        if session[3] is not None and e["event_ts"] > session[3]:
            raise EventConsistencyError(f"meter event {e['event_id']!r} occurs after session end")
        if e["event_ts"] < session[2]:
            raise EventConsistencyError(f"meter event {e['event_id']!r} occurs before session start")
        self._con.execute(
            """
            INSERT INTO meter_reading
                (event_id, session_id, ts, voltage_v, current_a, power_kw, energy_kwh, soc_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                e["event_id"], e["session_id"], e["event_ts"], e["voltage_v"],
                e["current_a"], e["power_kw"], e["energy_kwh"], e["soc_pct"],
            ],
        )

    def _end_session(self, e: dict[str, Any]) -> None:
        """Append consistent final facts and decrement the charger's live session count."""
        session = self._matching_session(e)
        if session[3] is not None:
            raise EventConsistencyError(f"session {e['session_id']!r} is already complete")
        if session[2] != e["start_ts"]:
            raise EventConsistencyError(
                f"session_end {e['event_id']!r} start_ts does not match session_start"
            )
        if not math.isclose(session[4], e["price_per_kwh"], rel_tol=0, abs_tol=1e-9):
            raise EventConsistencyError(
                f"session_end {e['event_id']!r} price does not match session_start"
            )
        # The session core is not updated: end facts are appended to their own
        # table and the charging_session view reconstructs the complete session.
        self._con.execute(
            """
            INSERT INTO session_end_event
                (event_id, session_id, charge_end_ts, end_ts, charge_minutes,
                 energy_kwh, avg_kw, peak_kw, price_per_kwh, cost_aud)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                e["event_id"], e["session_id"], e["charge_end_ts"], e["event_ts"],
                e["charge_minutes"],
                e["energy_kwh"], e["avg_kw"], e["peak_kw"], e["price_per_kwh"],
                e["cost_aud"],
            ],
        )
        self._con.execute(
            """
            UPDATE charger_live_state
            SET active_sessions = greatest(active_sessions - 1, 0)
            WHERE charger_id = ?
            """,
            [e["charger_id"]],
        )

    def _touch_live_state(self, e: dict[str, Any]) -> None:
        """Advance the charger's latest-event marker only for a higher stream sequence."""
        # Do not let an older bootstrap/replayed event replace a more recent
        # last-event marker already held by the database.
        self._con.execute(
            """
            UPDATE charger_live_state
            SET last_event_ts = ?, last_event_id = ?, last_seq = ?
            WHERE charger_id = ? AND (last_seq IS NULL OR ? > last_seq)
            """,
            [e["event_ts"], e["event_id"], e["seq"], e["charger_id"], e["seq"]],
        )

