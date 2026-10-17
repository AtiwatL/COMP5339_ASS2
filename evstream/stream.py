"""Task 2a - turn the generated time series into one ordered stream of MQTT-ready events.

Preparation steps (all done in DuckDB, so 2.5 million rows are processed in seconds):

1. price_update  - from prices_hourly.csv. Only *changes* are kept: a row is emitted when the
                   price differs from the same charger's previous hourly price (the first price of
                   each charger is always emitted so subscribers learn the starting tariff).
2. session_start - one per session at start_ts. It carries only what is known when the car plugs
                   in (connector, vehicle, locked-in tariff). End-of-session facts are NOT leaked.
3. meter         - one per meter reading (voltage, current, power, cumulative energy, SoC).
4. session_end   - one per session at end_ts (unplug), with the session totals and cost.

Events are sorted by event time; ties are broken by type (price -> start -> meter -> end), then
charger and event_id, so a meter reading never precedes its session_start. Each event receives a
global sequence number (seq) used for ordering checks and for resuming the publisher.

Every message is a JSON envelope:
    {"v": 1, "seq": 17, "event_id": "...", "event_type": "meter",
     "event_ts": "2026-10-01T08:02:00+10:00", "charger_id": 2, "connector_id": 1,
     "session_id": "2-1-202610010800", "data": {...type-specific fields...}}
Timestamps are AEMO market time (AEST, UTC+10, no daylight saving), as in the source data.

Outputs: data/stream/events.parquet (full ordered stream incl. the message JSON)
         data/stream/events_sample.jsonl (500 consecutive messages after the 08:00 price updates on day 1, human-readable)

Run:  python -m evstream.stream
"""
import json

import duckdb

from evstream import config

TS_FMT = "%Y-%m-%dT%H:%M:%S+10:00"

STREAM_SQL = f"""
CREATE TEMP TABLE ev AS
WITH prices AS (
    SELECT station_id AS charger_id, CAST(ts AS TIMESTAMP) AS ts, price_per_kwh, wholesale_per_kwh,
           lag(price_per_kwh) OVER (PARTITION BY station_id ORDER BY ts) AS prev_price
    FROM read_csv_auto('{{gen}}/prices_hourly.csv')
),
price_events AS (
    SELECT 'price_update' AS event_type, ts AS event_ts, charger_id,
           NULL::INTEGER AS connector_id, NULL::VARCHAR AS session_id,
           'p-' || charger_id || '-' || strftime(ts, '%Y%m%d%H') AS event_id,
           json_object('price_per_kwh', price_per_kwh, 'previous_price_per_kwh', prev_price,
                       'wholesale_per_kwh', wholesale_per_kwh, 'currency', 'AUD') AS data
    FROM prices
    WHERE prev_price IS NULL OR price_per_kwh <> prev_price
),
sessions AS (SELECT * FROM read_csv_auto('{{gen}}/sessions.csv')),
start_events AS (
    SELECT 'session_start', CAST(start_ts AS TIMESTAMP), station_id, connector_id, session_id,
           'ss-' || session_id,
           json_object('vehicle_model', vehicle_model, 'price_per_kwh', price_per_kwh,
                       'site_type', site_type)
    FROM sessions
),
end_events AS (
    SELECT 'session_end', CAST(end_ts AS TIMESTAMP), station_id, connector_id, session_id,
           'se-' || session_id,
           json_object('start_ts', strftime(CAST(start_ts AS TIMESTAMP), '{TS_FMT}'),
                       'charge_end_ts', strftime(CAST(charge_end_ts AS TIMESTAMP), '{TS_FMT}'),
                       'charge_minutes', charge_minutes, 'energy_kwh', energy_kwh,
                       'avg_kw', avg_kw, 'peak_kw', peak_kw,
                       'price_per_kwh', price_per_kwh, 'cost_aud', cost_aud)
    FROM sessions
),
meter_events AS (
    SELECT 'meter', CAST(ts AS TIMESTAMP), station_id, connector_id, session_id,
           'm-' || session_id || '-' || strftime(CAST(ts AS TIMESTAMP), '%Y%m%d%H%M'),
           json_object('voltage_v', voltage_v, 'current_a', current_a, 'power_kw', power_kw,
                       'energy_kwh', energy_kwh, 'soc_pct', soc_pct)
    FROM read_csv_auto('{{gen}}/session_metering.csv')
),
all_events AS (
    SELECT * FROM price_events
    UNION ALL SELECT * FROM start_events
    UNION ALL SELECT * FROM meter_events
    UNION ALL SELECT * FROM end_events
)
SELECT row_number() OVER (ORDER BY event_ts,
                                   CASE event_type WHEN 'price_update' THEN 0 WHEN 'session_start' THEN 1
                                                   WHEN 'meter' THEN 2 ELSE 3 END,
                                   charger_id, event_id) AS seq,
       *
FROM all_events;
"""

MESSAGE_SQL = f"""
COPY (
    SELECT seq, event_id, event_type, event_ts, charger_id, connector_id, session_id,
           to_json({{
               'v': 1, 'seq': seq, 'event_id': event_id, 'event_type': event_type,
               'event_ts': strftime(event_ts, '{TS_FMT}'), 'charger_id': charger_id,
               'connector_id': connector_id, 'session_id': session_id, 'data': data
           }}) AS message
    FROM ev ORDER BY seq
) TO '{{out}}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000);
"""


def build_stream():
    config.STREAM_DIR.mkdir(parents=True, exist_ok=True)
    out = config.STREAM_DIR / "events.parquet"
    con = duckdb.connect()
    con.execute(STREAM_SQL.replace("{gen}", config.GENERATED_DIR.as_posix()))
    con.execute(MESSAGE_SQL.replace("{out}", out.as_posix()))

    checks = con.execute("""
        SELECT count(*) AS events,
               count(DISTINCT event_id) AS unique_ids,
               min(event_ts), max(event_ts),
               count(DISTINCT charger_id) AS chargers
        FROM ev""").fetchone()
    assert checks[0] == checks[1], "event_id must be unique"
    print(f"{checks[0]:,} events for {checks[4]:,} chargers, {checks[2]} -> {checks[3]}")
    print(con.execute("""
        SELECT event_type, count(*) AS events FROM ev GROUP BY 1 ORDER BY min(seq)""").df().to_string(index=False))
    raw_prices = con.execute(
        f"SELECT count(*) FROM read_csv_auto('{config.GENERATED_DIR.as_posix()}/prices_hourly.csv')").fetchone()[0]
    kept = con.execute("SELECT count(*) FROM ev WHERE event_type = 'price_update'").fetchone()[0]
    print(f"price de-duplication: {raw_prices:,} hourly rows -> {kept:,} price changes "
          f"({raw_prices - kept:,} unchanged prices dropped)")

    # Ordering sanity check: every meter/end follows its session_start.
    bad = con.execute("""
        SELECT count(*) FROM ev m JOIN ev s ON s.session_id = m.session_id AND s.event_type = 'session_start'
        WHERE m.event_type IN ('meter', 'session_end') AND m.seq < s.seq""").fetchone()[0]
    assert bad == 0, f"{bad} events precede their session_start"

    sample = con.execute(f"SELECT message FROM read_parquet('{out.as_posix()}') WHERE event_ts >= TIMESTAMP '{config.SIM_START} 08:00:01' ORDER BY seq LIMIT 500").fetchall()
    with open(config.STREAM_DIR / "events_sample.jsonl", "w") as f:
        for (msg,) in sample:
            f.write(json.dumps(json.loads(msg)) + "\n")
    print(f"-> {out.relative_to(config.PROJECT_ROOT)} ({out.stat().st_size / 1e6:.0f} MB)")


if __name__ == "__main__":
    build_stream()
