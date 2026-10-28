-- COMP5339 Assignment 2: temporal MQTT event schema.
--
-- This DDL extends a copy of the Assignment 1 database.  event_id is the
-- stable identity used for QoS 1 de-duplication; seq preserves the order of
-- the single prepared event stream used for this assignment.
--
-- Ingestion flow:
--   MQTT JSON -> event_log (immutable audit envelope)
--             -> one typed event table (queryable event attributes)
--             -> charger_live_state (small current-state cache)
-- The history tables are append-only.  Only charger_live_state is updated.


-- One row per MQTT message.  The event_id primary key makes redelivery under
-- MQTT QoS 1 idempotent: the same event cannot be stored twice.  payload_json
-- preserves the original message while the typed tables below expose values
-- efficiently to SQL.  event_ts is simulated time; received_at is wall-clock
-- ingestion time.
CREATE TABLE IF NOT EXISTS event_log (
    event_id       VARCHAR PRIMARY KEY,
    seq            BIGINT NOT NULL UNIQUE,
    event_type     VARCHAR NOT NULL CHECK (
        event_type IN ('price_update', 'session_start', 'meter', 'session_end')
    ),
    event_ts       TIMESTAMP NOT NULL,
    charger_id     BIGINT NOT NULL REFERENCES charger(charger_id),
    connector_id   INTEGER CHECK (connector_id IS NULL OR connector_id > 0),
    session_id     VARCHAR,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    payload_json   JSON NOT NULL,
    CHECK (
        (event_type = 'price_update' AND connector_id IS NULL AND session_id IS NULL)
        OR
        (event_type <> 'price_update' AND connector_id IS NOT NULL AND session_id IS NOT NULL)
    )
);


-- Typed projection of price_update messages.  Price history remains immutable;
-- the latest price is also copied into charger_live_state for fast map refresh.
CREATE TABLE IF NOT EXISTS price_event (
    event_id                VARCHAR PRIMARY KEY REFERENCES event_log(event_id),
    charger_id              BIGINT NOT NULL REFERENCES charger(charger_id),
    event_ts                TIMESTAMP NOT NULL,
    price_per_kwh           DOUBLE NOT NULL CHECK (price_per_kwh >= 0),
    wholesale_per_kwh       DOUBLE,
    currency                VARCHAR NOT NULL DEFAULT 'AUD',
    UNIQUE (charger_id, event_ts)
);


-- Stable session identity and attributes known at session_start.  Separating
-- this parent row from start/end events lets meter readings reference a session
-- without updating a referenced row when session_end later arrives.
CREATE TABLE IF NOT EXISTS charging_session_core (
    session_id       VARCHAR PRIMARY KEY,
    charger_id       BIGINT NOT NULL REFERENCES charger(charger_id),
    connector_id     INTEGER NOT NULL CHECK (connector_id > 0),
    vehicle_model    VARCHAR,
    price_per_kwh    DOUBLE NOT NULL CHECK (price_per_kwh >= 0)
);


-- Exactly one start event per session.  event_id links back to the original
-- MQTT envelope; session_id links all start, meter, and end facts together.
CREATE TABLE IF NOT EXISTS session_start_event (
    event_id         VARCHAR PRIMARY KEY REFERENCES event_log(event_id),
    session_id       VARCHAR NOT NULL UNIQUE REFERENCES charging_session_core(session_id),
    start_ts         TIMESTAMP NOT NULL,
    UNIQUE (session_id, start_ts)
);


-- Exactly one end event per completed session.  charge_end_ts is when energy
-- delivery stopped; end_ts is when the vehicle unplugged, so the difference is
-- post-charge idle/occupancy time.  No row exists here while a session is open.
CREATE TABLE IF NOT EXISTS session_end_event (
    event_id         VARCHAR PRIMARY KEY REFERENCES event_log(event_id),
    session_id       VARCHAR NOT NULL UNIQUE REFERENCES charging_session_core(session_id),
    charge_end_ts    TIMESTAMP NOT NULL,
    end_ts           TIMESTAMP NOT NULL,
    charge_minutes   DOUBLE NOT NULL CHECK (charge_minutes >= 0),
    energy_kwh       DOUBLE NOT NULL CHECK (energy_kwh >= 0),
    avg_kw           DOUBLE NOT NULL CHECK (avg_kw >= 0),
    peak_kw          DOUBLE NOT NULL CHECK (peak_kw >= 0),
    price_per_kwh    DOUBLE NOT NULL CHECK (price_per_kwh >= 0),
    cost_aud         DOUBLE NOT NULL CHECK (cost_aud >= 0),
    CHECK (end_ts >= charge_end_ts)
);


-- Append-only telemetry.  energy_kwh is cumulative within a session, whereas
-- power_kw is the instantaneous value at ts.  (session_id, ts) prevents two
-- readings for the same session at the same simulated instant.
CREATE TABLE IF NOT EXISTS meter_reading (
    event_id       VARCHAR PRIMARY KEY REFERENCES event_log(event_id),
    session_id     VARCHAR NOT NULL REFERENCES charging_session_core(session_id),
    ts             TIMESTAMP NOT NULL,
    voltage_v      REAL NOT NULL CHECK (voltage_v >= 0),
    current_a      REAL NOT NULL CHECK (current_a >= 0),
    power_kw       REAL NOT NULL CHECK (power_kw >= 0),
    energy_kwh     REAL NOT NULL CHECK (energy_kwh >= 0),
    soc_pct        REAL NOT NULL CHECK (soc_pct BETWEEN 0 AND 100),
    UNIQUE (session_id, ts)
);


-- Materialized current state used by the polling dashboard.  The subscriber
-- updates this row in the same transaction as each immutable event, avoiding
-- repeated aggregation of the complete event history every 2.5 seconds.
CREATE TABLE IF NOT EXISTS charger_live_state (
    charger_id       BIGINT PRIMARY KEY REFERENCES charger(charger_id),
    current_price    DOUBLE CHECK (current_price IS NULL OR current_price >= 0),
    wholesale_price  DOUBLE,
    price_ts         TIMESTAMP,
    active_sessions  INTEGER NOT NULL DEFAULT 0 CHECK (active_sessions >= 0),
    last_event_ts    TIMESTAMP,
    last_event_id    VARCHAR REFERENCES event_log(event_id),
    last_seq         BIGINT,
    CHECK (
        (current_price IS NULL AND price_ts IS NULL)
        OR
        (current_price IS NOT NULL AND price_ts IS NOT NULL)
    )
);

-- One current-state row per Assignment 1 charger.  Reapplying the schema does
-- not overwrite state already maintained by the subscriber.
INSERT INTO charger_live_state (charger_id)
SELECT charger_id
FROM charger
ON CONFLICT (charger_id) DO NOTHING;


-- ART indexes support the dashboard's selective charger/time lookups.  The
-- primary and unique constraints above already index their own key columns.
CREATE INDEX IF NOT EXISTS idx_event_log_charger_time
    ON event_log (charger_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_price_event_charger_time
    ON price_event (charger_id, event_ts);

CREATE INDEX IF NOT EXISTS idx_session_core_charger
    ON charging_session_core (charger_id);

CREATE INDEX IF NOT EXISTS idx_session_start_time
    ON session_start_event (start_ts);

CREATE INDEX IF NOT EXISTS idx_session_end
    ON session_end_event (end_ts);

CREATE INDEX IF NOT EXISTS idx_meter_session_time
    ON meter_reading (session_id, ts);


-- Reconstruct a dashboard-friendly session from the append-only components.
-- The LEFT JOIN is important: a started session remains visible before its end
-- event arrives, with end fields NULL.  Therefore end_ts IS NULL means active.
CREATE OR REPLACE VIEW charging_session AS
SELECT
    c.session_id,
    c.charger_id,
    c.connector_id,
    c.vehicle_model,
    s.event_id AS start_event_id,
    e.event_id AS end_event_id,
    s.start_ts,
    e.charge_end_ts,
    e.end_ts,
    e.charge_minutes,
    e.energy_kwh,
    e.avg_kw,
    e.peak_kw,
    c.price_per_kwh,
    e.cost_aud
FROM charging_session_core c
JOIN session_start_event s USING (session_id)
LEFT JOIN session_end_event e USING (session_id);


-- One map-ready row for every A1 charger, including chargers that have not yet
-- received an MQTT event.  LEFT JOINs preserve chargers with missing operator,
-- rating, plug, or live-state data.  Power and plug aggregates are calculated
-- separately to avoid multiplying rows where a charger has several of each.
CREATE OR REPLACE VIEW v_charger_live AS
WITH rating AS (
    SELECT charger_id, max(power_kw) AS max_kw
    FROM charger_power_rating
    GROUP BY charger_id
),
plugs AS (
    SELECT cc.charger_id,
           string_agg(ct.connector_name, ', ' ORDER BY ct.connector_name) AS plug_types
    FROM charger_connector cc
    JOIN connector_type ct USING (connector_type_id)
    GROUP BY cc.charger_id
)
SELECT
    c.charger_id,
    o.operator_name,
    s.station_address,
    s.postcode,
    s.lga_name,
    r4.sa4_name,
    s.latitude,
    s.longitude,
    c.charger_type,
    c.charger_status,
    greatest(coalesce(c.number_of_plugs, 1), 1) AS connectors,
    r.max_kw,
    p.plug_types,
    l.current_price,
    l.wholesale_price,
    l.price_ts,
    coalesce(l.active_sessions, 0) AS active_sessions,
    l.last_event_ts,
    l.last_event_ts IS NOT NULL AS has_events,
    coalesce(l.active_sessions, 0)::DOUBLE
        / greatest(coalesce(c.number_of_plugs, 1), 1) AS occupancy
FROM charger c
JOIN charging_site s USING (site_id)
JOIN sa4_region r4 USING (sa4_code)
LEFT JOIN operator o USING (operator_id)
LEFT JOIN rating r USING (charger_id)
LEFT JOIN plugs p USING (charger_id)
LEFT JOIN charger_live_state l USING (charger_id);


-- Open sessions with their newest telemetry for current dashboard KPIs.
-- row_number restarts for each session and ranks newest ts first; rn = 1 is
-- therefore the latest reading.  LEFT JOIN keeps a newly started session visible
-- even when its first meter message has not arrived yet.
CREATE OR REPLACE VIEW v_active_sessions AS
WITH latest_meter AS (
    SELECT session_id,
           ts AS last_meter_ts,
           power_kw,
           energy_kwh,
           soc_pct,
           row_number() OVER (PARTITION BY session_id ORDER BY ts DESC) AS rn
    FROM meter_reading
)
SELECT
    s.session_id,
    s.charger_id,
    s.connector_id,
    s.vehicle_model,
    s.start_ts,
    s.end_ts,
    s.price_per_kwh,
    m.last_meter_ts,
    m.power_kw,
    m.energy_kwh,
    m.soc_pct
FROM charging_session s
LEFT JOIN latest_meter m ON m.session_id = s.session_id AND m.rn = 1
WHERE s.end_ts IS NULL;


-- Historical charger metrics used by the stored-stream pop-up summary. Only ended
-- sessions have final energy/cost, so active sessions are deliberately excluded.
-- A cross-midnight session is assigned wholly to its end day.  Occupied minutes
-- run to unplug time (end_ts), including idle time after charging finishes.
CREATE OR REPLACE VIEW v_charger_usage_daily AS
SELECT
    charger_id,
    CAST(end_ts AS DATE) AS day,
    count(*) AS sessions,
    coalesce(sum(energy_kwh), 0) AS energy_kwh,
    coalesce(sum(cost_aud), 0) AS revenue_aud,
    coalesce(sum(date_diff('minute', start_ts, end_ts)), 0) AS occupied_minutes
FROM charging_session
WHERE end_ts IS NOT NULL
GROUP BY charger_id, CAST(end_ts AS DATE);


-- Network activity per simulated hour. Session counts and average tariff use
-- session start time; energy uses meter time so active sessions are included.
CREATE OR REPLACE VIEW v_network_hourly AS
-- Count starts and the tariff locked in by those new sessions.
WITH starts AS (
    SELECT
        date_trunc('hour', start_ts) AS hour,
        count(*) AS sessions_started,
        avg(price_per_kwh) AS avg_session_price
    FROM charging_session
    GROUP BY 1
),
-- Meter energy is cumulative, so subtract the previous reading within the same
-- session.  The first reading subtracts zero. greatest(..., 0) prevents a meter
-- reset or rounding anomaly from reporting negative delivered energy.
meter_delta AS (
    SELECT
        ts,
        greatest(
            energy_kwh - lag(energy_kwh, 1, 0) OVER (
                PARTITION BY session_id
                ORDER BY ts
            ),
            0
        ) AS delivered_kwh
    FROM meter_reading
),
-- Attribute each positive energy increment to the hour its reading arrived.
-- This includes energy from sessions that are still active.
energy AS (
    SELECT
        date_trunc('hour', ts) AS hour,
        coalesce(sum(delivered_kwh), 0) AS energy_kwh
    FROM meter_delta
    GROUP BY 1
),
-- Preserve hours containing either starts or energy.  UNION removes duplicates;
-- without this set, an hour with ongoing charging but no new starts could vanish.
hours AS (
    SELECT hour FROM starts
    UNION
    SELECT hour FROM energy
)
-- Missing counts/energy mean no activity of that kind, so return zero.  A missing
-- average price remains NULL because no sessions started and zero would imply a
-- real $0 tariff.
SELECT
    h.hour,
    coalesce(s.sessions_started, 0) AS sessions_started,
    coalesce(e.energy_kwh, 0) AS energy_kwh,
    s.avg_session_price
FROM hours h
LEFT JOIN starts s USING (hour)
LEFT JOIN energy e USING (hour);


