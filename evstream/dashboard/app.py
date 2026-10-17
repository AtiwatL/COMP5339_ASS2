"""Task 3/4 - live map dashboard (Dash + dash-leaflet), refreshed every 2.5 s from DuckDB.

Run:  python -m evstream.dashboard.app [--no-ingest] [--db PATH] [--port 8050]
"""
import argparse
import threading
from urllib.parse import parse_qs

import dash_leaflet as dl
import duckdb
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update

from evstream import config
from evstream.dashboard import figures as fig
from evstream.dashboard import queries as q

REFRESH_MS = 2500
NSW_CENTER = [-33.2, 149.0]
GRAPH_CONFIG = {"displayModeBar": False}


class Database:
    """One DuckDB connection per process; each callback works on its own cursor (thread-safe)."""

    def __init__(self, path, read_only=True):
        self._con = duckdb.connect(str(path), read_only=read_only)
        # The A1 tables carry RTREE indexes: a writing connection (the embedded ingest) cannot
        # checkpoint without spatial. INSTALL is a no-op once installed (internet needed only once);
        # cursors share the extensions loaded here.
        self._con.execute("INSTALL spatial; LOAD spatial")
        self._lock = threading.Lock()

    def cursor(self):
        with self._lock:
            return self._con.cursor()


def occupancy_class(active, connectors, has_events):
    if not has_events:
        return "none"
    if active <= 0:
        return "free"
    return "full" if active >= connectors else "partial"


def to_geojson(df):
    features = []
    for r in df.itertuples(index=False):
        price = None if r.current_price != r.current_price or r.current_price is None else round(r.current_price, 2)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r.longitude, r.latitude]},
            "properties": {
                "charger_id": int(r.charger_id),
                "price": price,
                "active": int(r.active_sessions),
                "connectors": int(r.connectors),
                "occ": occupancy_class(r.active_sessions, r.connectors, r.has_events),
                "tooltip": f"<b>{r.operator_name or 'Unknown operator'}</b><br>{r.station_address or ''}<br>"
                           f"{r.charger_type} · {r.active_sessions}/{r.connectors} in use"
                           + (f" · ${price:.2f}/kWh" if price is not None else ""),
            },
        })
    return {"type": "FeatureCollection", "features": features}


def build_layout(db):
    with db.cursor() as cur:
        operators, types = q.filter_options(cur)
    filters = html.Div(className="ev-filters", children=[
        dcc.Dropdown(id="f-operator", options=operators, multi=True, placeholder="All operators",
                     className="ev-filter-op"),
        dcc.Checklist(id="f-type", options=types, value=[], inline=True, inputStyle={"marginRight": "4px"},
                      labelStyle={"marginRight": "10px"}),
        dcc.Checklist(id="f-idle", options=[{"label": "show chargers without events", "value": "idle"}],
                      value=[], inputStyle={"marginRight": "4px"}),
        html.Div(className="ev-legend", children=[
            html.Span("free", className="l-free"), html.Span("partly busy", className="l-partial"),
            html.Span("full", className="l-full"), html.Span("no events yet", className="l-none"),
        ]),
    ])
    chargers = dl.GeoJSON(
        id="chargers", data={"type": "FeatureCollection", "features": []},
        cluster=True, zoomToBoundsOnClick=True, spiderfyOnMaxZoom=True,
        superClusterOptions={"radius": 60, "maxZoom": 15},
        pointToLayer={"variable": "dashExtensions.default.pointToLayer"},
        clusterToLayer={"variable": "dashExtensions.default.clusterToLayer"},
    )
    leaflet_map = dl.Map(id="map", center=NSW_CENTER, zoom=6, style={"height": "100%", "width": "100%"}, children=[
        dl.TileLayer(url="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
                     attribution="&copy; OpenStreetMap contributors"),
        chargers,
    ])
    return html.Div([
        html.Div(className="ev-header", children=[
            html.H1("NSW EV charging - live network"),
            html.Span(id="sim-time", className="ev-simtime"),
        ]),
        html.Div(id="kpis", className="ev-kpis"),
        filters,
        html.Div(className="ev-main", children=[
            html.Div(className="ev-map-wrap", children=[
                leaflet_map,
                # Fixed skeleton: refreshes replace the details and the figures only, so the graphs stay
                # mounted and the user's zoom and scroll position survive the 2.5 s refresh.
                html.Div(id="popup", className="ev-popup", hidden=True, children=[
                    html.Button("×", id="popup-close", className="ev-popup-close", title="close"),
                    html.Div(id="pp-details"),
                    html.Div(id="pp-curve-title", className="ev-section"),
                    dcc.Graph(id="pp-curve", config=GRAPH_CONFIG),
                    html.Div("price history - last 7 days", className="ev-section"),
                    dcc.Graph(id="pp-price", config=GRAPH_CONFIG),
                ]),
            ]),
            html.Aside(className="ev-side", children=[
                html.Div("network - last 48 hours", className="ev-section"),
                dcc.Graph(id="g-hourly", config=GRAPH_CONFIG),
                html.Div("top chargers by energy - last 7 days", className="ev-section"),
                dcc.Graph(id="g-top", config=GRAPH_CONFIG),
                html.Div("live event feed", className="ev-section"),
                html.Ul(id="feed", className="ev-feed"),
            ]),
        ]),
        dcc.Interval(id="tick", interval=REFRESH_MS),
        dcc.Store(id="map-sig"),
        dcc.Store(id="selected"),
        dcc.Location(id="url"),
    ])


def _num(v, fmt, none="–"):
    return none if v is None or v != v else format(v, fmt)


def _metric(label, value, sub=None):
    return html.Div(className="ev-metric", children=[
        html.Div(label, className="ev-metric-label"), html.Div(value, className="ev-metric-value"),
        html.Div(sub, className="ev-metric-sub") if sub else None,
    ])


EVENT_LABEL = {"price_update": "price", "session_start": "plug-in", "meter": "meter", "session_end": "unplug"}


def kpi_tiles(cur, now):
    cur_kpi, hist = q.kpis(cur, now)
    reporting = int(cur_kpi["chargers_reporting"] or 0)
    return [
        html.Div(className="ev-kpi-group", children=[
            html.Div("now", className="ev-kpi-group-label"),
            _metric("active sessions", f"{int(cur_kpi['active_sessions'] or 0):,}"),
            _metric("chargers in use", f"{int(cur_kpi['chargers_in_use'] or 0):,}", f"of {reporting:,} reporting"),
            _metric("delivered now", f"{_num(cur_kpi['mw_now'], '.2f')} MW"),
            _metric("avg live price", f"${_num(cur_kpi['avg_price'], '.2f')}/kWh"),
        ]),
        html.Div(className="ev-kpi-group", children=[
            html.Div("last 7 days", className="ev-kpi-group-label"),
            _metric("sessions completed", f"{int(hist['sessions']):,}"),
            _metric("energy delivered", f"{hist['mwh']:,.1f} MWh"),
            _metric("revenue", f"${hist['revenue_aud']:,.0f}"),
        ]),
    ]


def feed_items(events):
    return [html.Li([
        html.Span(f"{r.event_ts:%H:%M:%S}", className="ev-feed-ts"),
        html.Span(EVENT_LABEL.get(r.event_type, r.event_type), className=f"ev-feed-type ev-feed-{r.event_type}"),
        html.Span(f"#{r.charger_id}" + (f"/{int(r.connector_id)}" if r.connector_id == r.connector_id and r.connector_id is not None else "")
                  + f" {r.operator_name or ''}", className="ev-feed-who"),
    ]) for r in events.itertuples()]


def popup_parts(cur, charger_id, now):
    """Pop-up content as (details, curve title, curve figure, price figure)."""
    d = q.charger_detail(cur, charger_id)
    if d is None:
        return html.Div(f"charger {charger_id} not found"), None, fig.empty("–"), fig.empty("–")
    facts = [
        html.Div(d["operator_name"] or "Unknown operator", className="ev-popup-title"),
        html.Div(f"{d['station_address'] or ''}", className="ev-popup-sub"),
        html.Table(className="ev-facts", children=[html.Tbody([
            html.Tr([html.Th("charger"), html.Td(f"#{charger_id} · {d['charger_type']} · {_num(d['max_kw'], '.0f')} kW · {d['connectors']} connectors")]),
            html.Tr([html.Th("plug types"), html.Td(d["plug_types"] or "not recorded")]),
            html.Tr([html.Th("area"), html.Td(", ".join(x for x in (d["lga_name"], d["sa4_name"]) if x))]),
            html.Tr([html.Th("status"), html.Td(d["charger_status"] or "–")]),
        ])]),
    ]
    if now is None:  # nothing stored yet, e.g. the dashboard was started before the publisher
        return (html.Div(facts + [html.Div("waiting for events…", className="ev-popup-sub")]), "charging curve",
                fig.empty("no meter readings yet"), fig.empty("no price events yet"))
    cur_state = q.charger_current(cur, charger_id, now)
    in_use = int(cur_state["in_use"])
    hist = q.charger_hist_7d(cur, charger_id, now)
    readings, live = q.session_curves(cur, charger_id)
    prices = q.price_history(cur, charger_id, now)
    occ = occupancy_class(in_use, d["connectors"], d["has_events"])
    price_since = f"since {d['price_ts']:%d %b %H:%M}" if d["price_ts"] is not None and d["price_ts"] == d["price_ts"] else None
    details = html.Div(facts + [
        html.Div(className="ev-metric-row", children=[
            _metric("latest price", f"${_num(d['current_price'], '.2f')}/kWh", price_since),
            _metric("in use now", html.Span([html.Span(className=f"ev-dot ev-dot-{occ}"),
                                             f" {in_use}/{d['connectors']}"])),
            _metric("power now", f"{cur_state['power_kw']:.0f} kW"),
        ]),
        html.Div("last 7 simulated days", className="ev-section"),
        html.Div(className="ev-metric-row", children=[
            _metric("sessions", f"{int(hist['sessions']):,}"),
            _metric("energy", f"{hist['energy_kwh']:,.0f} kWh"),
            _metric("utilisation", f"{_num(hist['utilisation'], '.0%')}"),
            _metric("revenue", f"${hist['revenue_aud']:,.0f}"),
        ]),
    ])
    curve_title = ("charging curve - live session" + ("s" if readings["session_id"].nunique() > 1 else "")
                   if live else "charging curve - last completed session (none active)")
    # uirevision = charger: zoom is kept across refreshes and reset when another charger is opened
    return (details, curve_title, fig.charging_curve(readings, live, uirevision=charger_id),
            fig.price_history(prices, now, uirevision=charger_id))


def register_callbacks(app, db):
    @app.callback(
        Output("chargers", "data"), Output("map-sig", "data"), Output("sim-time", "children"),
        Input("tick", "n_intervals"), Input("f-operator", "value"), Input("f-type", "value"),
        Input("f-idle", "value"), State("map-sig", "data"),
    )
    def refresh_map(_, operators, types, idle, last_sig):
        with db.cursor() as cur:
            now = q.sim_now(cur)
            sig = [list(q.live_signature(cur)), operators, types, idle]
            sim_time = f"simulated time {now:%a %d %b %Y %H:%M:%S}" if now else "waiting for events…"
            if sig == last_sig:  # nothing changed: keep the markers, avoid re-sending ~2k features
                return no_update, no_update, sim_time
            df = q.map_points(cur, operators, types, include_idle=bool(idle))
        return to_geojson(df), sig, sim_time


    @app.callback(
        Output("selected", "data"),
        Input("chargers", "clickData"), Input("popup-close", "n_clicks"), Input("url", "search"),
    )
    def select_charger(feature, _, search):
        if ctx.triggered_id == "popup-close":
            return None
        if ctx.triggered_id == "url" or ctx.triggered_id is None:  # deep link: /?charger=1001
            ids = parse_qs((search or "").lstrip("?")).get("charger")
            return int(ids[0]) if ids and ids[0].isdigit() else no_update
        props = (feature or {}).get("properties") or {}
        if props.get("cluster"):  # cluster click zooms in; it is not a charger
            return no_update
        return props.get("charger_id", no_update)

    @app.callback(
        Output("popup", "hidden"), Output("pp-details", "children"), Output("pp-curve-title", "children"),
        Output("pp-curve", "figure"), Output("pp-price", "figure"),
        Input("selected", "data"), Input("tick", "n_intervals"),
    )
    def refresh_popup(charger_id, _):
        if charger_id is None:
            return True, no_update, no_update, no_update, no_update
        with db.cursor() as cur:
            return (False, *popup_parts(cur, charger_id, q.sim_now(cur)))


    @app.callback(
        Output("kpis", "children"), Output("g-hourly", "figure"), Output("g-top", "figure"),
        Output("feed", "children"),
        Input("tick", "n_intervals"),
    )
    def refresh_network(_):
        with db.cursor() as cur:
            now = q.sim_now(cur)
            if now is None:
                return [], fig.empty("waiting for events…"), fig.empty("waiting for events…"), []
            return (kpi_tiles(cur, now), fig.network_hourly(q.network_hourly(cur, now)),
                    fig.top_chargers(q.top_chargers(cur, now)), feed_items(q.event_feed(cur)))


def create_app(db):
    app = Dash(__name__, title="EV charging - live map", update_title=None)
    app.layout = build_layout(db)
    register_callbacks(app, db)
    return app


def main():
    ap = argparse.ArgumentParser(description="Live EV charging dashboard")
    ap.add_argument("--db", default=str(config.DB_PATH))
    ap.add_argument("--no-ingest", action="store_true",
                    help="open the stored database read-only, without subscribing to MQTT")
    ap.add_argument("--port", type=int, default=8050)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    # The embedded MQTT subscriber (evstream.ingest, Tasks 3/4 back end) is not integrated yet,
    # so the database is always opened read-only for now.
    db = Database(args.db, read_only=True)
    create_app(db).run(host="127.0.0.1", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
