"""Plotly charts for the dashboard.

Conventions: one y-scale per panel (measures with different units get their own panel, sharing
the time axis), thin 2 px lines, categorical colours assigned in a fixed order, recessive grid.
"""
import plotly.graph_objects as go
from plotly.subplots import make_subplots

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
TEXT_SECONDARY = "#52514e"
GRID = "#ecebe7"
SURFACE = "#fcfcfb"


def _style(fig, height):
    fig.update_layout(
        height=height, margin=dict(l=44, r=10, t=26, b=24), paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(size=11, color=TEXT_SECONDARY), hovermode="x unified", showlegend=False,
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=GRID, zeroline=False, tickformat="%H:%M\n%d %b")
    fig.update_yaxes(gridcolor=GRID, linecolor=GRID, zeroline=False, rangemode="tozero")
    return fig


def empty(message, height=120):
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, font=dict(color=TEXT_SECONDARY))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return _style(fig, height)


def charging_curve(readings, live):
    """Energy, power and SoC of each session as three small multiples on a shared time axis.
    One colour per connector, so several simultaneous sessions stay distinguishable."""
    if readings.empty:
        return empty("no meter readings yet")
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.07,
                        subplot_titles=("energy (kWh)", "power (kW)", "state of charge (%)"))
    sessions = readings.groupby(["connector_id", "session_id"], sort=True)
    for i, ((connector, _), s) in enumerate(sessions):
        color = SERIES[i % len(SERIES)]
        name = f"connector {connector} · {s['vehicle_model'].iloc[0]}"
        for row, col in enumerate(("energy_kwh", "power_kw", "soc_pct"), start=1):
            fig.add_trace(go.Scatter(
                x=s["ts"], y=s[col], mode="lines+markers" if len(s) < 4 else "lines", name=name,
                legendgroup=name, showlegend=row == 1, line=dict(color=color, width=2), marker=dict(size=8),
                hovertemplate="%{y:.1f}<extra>" + f"conn. {connector}" + "</extra>"), row=row, col=1)
    fig.update_yaxes(range=[0, 100], row=3, col=1)
    fig.update_annotations(font=dict(size=11, color=TEXT_SECONDARY), x=0, xanchor="left")
    _style(fig, 330)
    multi = sessions.ngroups > 1
    fig.update_layout(showlegend=multi, legend=dict(y=0.99, yanchor="top", yref="container"),
                      margin=dict(t=56 if multi else 26), height=360 if multi else 330)
    return fig


def price_history(prices, now):
    """Charger tariff (step line) with the AEMO wholesale price on the same $/kWh scale."""
    if prices.empty:
        return empty("no price events yet")
    x = list(prices["ts"]) + [now]  # extend the price in force up to "now"
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=list(prices["price_per_kwh"]) + [prices["price_per_kwh"].iloc[-1]],
                             name="charger tariff", mode="lines", line=dict(color=SERIES[0], width=2, shape="hv"),
                             hovertemplate="$%{y:.2f}/kWh<extra>tariff</extra>"))
    fig.add_trace(go.Scatter(x=x, y=list(prices["wholesale_per_kwh"]) + [prices["wholesale_per_kwh"].iloc[-1]],
                             name="wholesale (AEMO NSW1)", mode="lines",
                             line=dict(color=SERIES[1], width=2, shape="hv"),
                             hovertemplate="$%{y:.2f}/kWh<extra>wholesale</extra>"))
    _style(fig, 200)
    fig.update_layout(showlegend=True, margin=dict(t=30))
    # Wholesale spikes (several $/kWh) would flatten the tariff line, so the scale follows the
    # tariff and clipped spikes are flagged; the exact value stays available on hover.
    top = max(prices["price_per_kwh"].max() * 1.3, 0.1)
    low = min(prices["wholesale_per_kwh"].min(), 0) * 1.1
    fig.update_yaxes(tickprefix="$", rangemode="normal", range=[max(low, -top), top])
    if prices["wholesale_per_kwh"].max() > top:
        fig.add_annotation(text="wholesale spikes clipped - hover for values", xref="paper", yref="paper",
                           x=1, y=-0.22, xanchor="right", yanchor="top", showarrow=False, font=dict(size=10))
        fig.update_layout(margin=dict(b=44), height=215)
    return fig


def network_hourly(hourly):
    """Sessions started per hour and the average tariff they locked in, as two panels."""
    if hourly.empty:
        return empty("no sessions yet")
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.12, row_heights=[0.6, 0.4],
                        subplot_titles=("sessions started per hour", "average session tariff ($/kWh)"))
    fig.add_trace(go.Bar(x=hourly["hour"], y=hourly["sessions_started"], marker=dict(color=SERIES[0]),
                         marker_line_width=0, hovertemplate="%{y} sessions<extra></extra>"), row=1, col=1)
    fig.add_trace(go.Scatter(x=hourly["hour"], y=hourly["avg_session_price"], mode="lines",
                             line=dict(color=SERIES[0], width=2, shape="hv"),
                             hovertemplate="$%{y:.2f}/kWh<extra></extra>"), row=2, col=1)
    fig.update_annotations(font=dict(size=11, color=TEXT_SECONDARY), x=0, xanchor="left")
    _style(fig, 250)
    fig.update_layout(bargap=0.15, margin=dict(t=30))
    fig.update_yaxes(tickprefix="$", rangemode="normal", row=2, col=1)
    return fig


def top_chargers(top):
    if top.empty:
        return empty("no completed sessions yet")
    top = top.iloc[::-1]  # largest at the top
    labels = [f"#{r.charger_id} {(r.operator_name or '')[:18]}" for r in top.itertuples()]
    fig = go.Figure(go.Bar(
        x=top["energy_kwh"] / 1000, y=labels, orientation="h", marker=dict(color=SERIES[0]), marker_line_width=0,
        text=[f"{v / 1000:.1f} MWh" for v in top["energy_kwh"]], textposition="outside", cliponaxis=False,
        customdata=top["sessions"], hovertemplate="%{x:.2f} MWh · %{customdata} sessions<extra></extra>"))
    _style(fig, 230)
    fig.update_layout(hovermode="closest", margin=dict(l=150, r=50, t=6, b=20))
    fig.update_xaxes(tickformat=None, showticklabels=False, showgrid=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)", tickfont=dict(size=10))
    return fig
