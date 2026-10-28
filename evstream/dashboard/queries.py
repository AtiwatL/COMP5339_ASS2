"""SQL used by the dashboard. Every function takes a DuckDB cursor and returns a DataFrame or a dict.

The map refresh reads the 1,926-row charger_live_state (via v_charger_live) instead of aggregating
the event history. "Now" is the simulated time (latest stored event), not the wall clock.
"""

LIVE_COLUMNS = """charger_id, operator_name, station_address, postcode, lga_name, sa4_name, latitude, longitude,
                  charger_type, charger_status, connectors, max_kw, plug_types, current_price, wholesale_price,
                  price_ts, active_sessions, last_event_ts, has_events, occupancy"""


def sim_now(cur):
    """Simulated time = latest event stored (None while the database is empty)."""
    return cur.execute("SELECT max(last_event_ts) FROM charger_live_state").fetchone()[0]


def filter_options(cur):
    operators = [r[0] for r in cur.execute("""
        SELECT DISTINCT operator_name FROM v_charger_live WHERE operator_name IS NOT NULL ORDER BY 1""").fetchall()]
    types = [r[0] for r in cur.execute("SELECT DISTINCT charger_type FROM v_charger_live ORDER BY 1").fetchall()]
    return operators, types


def map_points(cur, operators=None, types=None, include_idle=False):
    """One row per charger for the map; chargers without events only when include_idle."""
    where, params = [], []
    if not include_idle:
        where.append("has_events")
    if operators:
        where.append("operator_name IN (SELECT unnest(?))")
        params.append(list(operators))
    if types:
        where.append("charger_type IN (SELECT unnest(?))")
        params.append(list(types))
    sql = f"SELECT {LIVE_COLUMNS} FROM v_charger_live"
    if where:
        sql += " WHERE " + " AND ".join(where)
    return cur.execute(sql, params).df()


def live_signature(cur):
    """Cheap change detector: the map is only re-sent when the live state changed."""
    return cur.execute("""
        SELECT count(*), max(last_event_ts)::VARCHAR, sum(active_sessions), sum(current_price)
        FROM charger_live_state""").fetchone()


# ---------------------------------------------------------------- pop-up (one charger)

def charger_detail(cur, charger_id):
    df = cur.execute(f"SELECT {LIVE_COLUMNS} FROM v_charger_live WHERE charger_id = ?", [charger_id]).df()
    return df.iloc[0].to_dict() if len(df) else None


def charger_current(cur, charger_id, now):
    """Current state: sessions plugged in and the power delivered by the latest meter readings.
    A reading older than 12 minutes (AC meters report every 10 min) means charging has stopped."""
    return cur.execute("""
        SELECT count(*) AS in_use,
               coalesce(sum(power_kw) FILTER (WHERE last_meter_ts >= ? - INTERVAL 12 MINUTE), 0) AS power_kw
        FROM v_active_sessions WHERE charger_id = ?""", [now, charger_id]).df().iloc[0].to_dict()


def charger_history(cur, charger_id):
    """Completed metrics over all stored stream history for one charger."""
    return cur.execute("""
        WITH span AS (
            SELECT greatest(date_diff('minute', min(event_ts), max(event_ts)), 1) AS minutes
            FROM event_log
        )
        SELECT coalesce(sum(sessions), 0) AS sessions,
               coalesce(sum(energy_kwh), 0) AS energy_kwh,
               coalesce(sum(revenue_aud), 0) AS revenue_aud,
               coalesce(sum(occupied_minutes), 0)
                   / (any_value(span.minutes) * any_value(c.connectors)) AS utilisation
        FROM v_charger_usage_daily u
        JOIN (SELECT charger_id, connectors FROM v_charger_live) c USING (charger_id)
        CROSS JOIN span
        WHERE u.charger_id = ?""", [charger_id]).df().iloc[0].to_dict()


def session_curves(cur, charger_id):
    """Meter readings of the active session(s); if none is active, of the most recent session."""
    sessions = cur.execute("""
        SELECT session_id, connector_id, vehicle_model, start_ts, end_ts FROM charging_session
        WHERE charger_id = ? AND end_ts IS NULL ORDER BY connector_id""", [charger_id]).df()
    live = len(sessions) > 0
    if not live:
        sessions = cur.execute("""
            SELECT session_id, connector_id, vehicle_model, start_ts, end_ts FROM charging_session
            WHERE charger_id = ? ORDER BY start_ts DESC LIMIT 1""", [charger_id]).df()
    readings = cur.execute("""
        SELECT m.session_id, s.connector_id, s.vehicle_model, m.ts, m.power_kw, m.energy_kwh, m.soc_pct
        FROM meter_reading m JOIN charging_session s USING (session_id)
        WHERE m.session_id IN (SELECT unnest(?)) ORDER BY m.ts""", [sessions["session_id"].tolist()]).df()
    return readings, live


def price_history(cur, charger_id):
    """All tariff changes for one charger that were received through MQTT."""
    return cur.execute("""
        SELECT event_ts AS ts, price_per_kwh, wholesale_per_kwh
        FROM price_event
        WHERE charger_id = ?
        ORDER BY event_ts""", [charger_id]).df()


# ---------------------------------------------------------------- KPIs and side panel (network)

def kpis(cur, now):
    """Current state plus completed metrics over all stored streamed history."""
    current = cur.execute("""
        SELECT sum(active_sessions) AS active_sessions,
               count(*) FILTER (WHERE active_sessions > 0) AS chargers_in_use,
               count(*) AS chargers_reporting,
               avg(current_price) AS avg_price,
               (SELECT coalesce(sum(power_kw), 0) / 1000 FROM v_active_sessions
                WHERE last_meter_ts >= ? - INTERVAL 12 MINUTE) AS mw_now
        FROM charger_live_state""", [now]).df().iloc[0].to_dict()
    hist = cur.execute("""
        SELECT count(*) AS sessions, coalesce(sum(energy_kwh), 0) / 1000 AS mwh,
               coalesce(sum(cost_aud), 0) AS revenue_aud
        FROM charging_session WHERE end_ts IS NOT NULL""").df().iloc[0].to_dict()
    return current, hist


def network_hourly(cur, now, hours=48):
    return cur.execute("""
        SELECT hour, sessions_started, energy_kwh, avg_session_price
        FROM v_network_hourly WHERE hour > ?::TIMESTAMP - to_hours(?::INTEGER) AND hour <= ?
        ORDER BY hour""", [now, hours, now]).df()


def top_chargers(cur, n=8):
    """Highest-energy chargers across all completed sessions stored from MQTT."""
    return cur.execute("""
        SELECT s.charger_id, any_value(l.operator_name) AS operator_name, sum(s.energy_kwh) AS energy_kwh,
               count(*) AS sessions
        FROM charging_session s JOIN v_charger_live l USING (charger_id)
        WHERE s.end_ts IS NOT NULL
        GROUP BY s.charger_id ORDER BY energy_kwh DESC LIMIT ?""", [n]).df()


def event_feed(cur, n=12):
    """Latest stored events; the seq range lets DuckDB's zonemaps skip all older row groups."""
    return cur.execute("""
        SELECT e.seq, e.event_ts, e.event_type, e.charger_id, e.connector_id, o.operator_name
        FROM event_log e JOIN charger c USING (charger_id) LEFT JOIN operator o USING (operator_id)
        WHERE e.seq > (SELECT max(seq) FROM event_log) - ?
        ORDER BY e.seq DESC""", [n]).df()
