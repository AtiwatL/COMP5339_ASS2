"""Plotly figures used by the live dashboard.

Different units use separate panels, short streamed windows retain visible
markers, and chart ranges adapt to the values actually stored in DuckDB.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
TEXT_SECONDARY = "#52514e"
GRID = "#ecebe7"
SURFACE = "#fcfcfb"


def _style(fig, height):
    """Apply shared low-noise dashboard styling."""
    fig.update_layout(
        height=height,
        margin=dict(l=46, r=12, t=28, b=28),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font=dict(size=11, color=TEXT_SECONDARY),
        hovermode="x unified",
        showlegend=False,
        uirevision="keep",
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
        hoverlabel=dict(bgcolor="#ffffff", bordercolor="#d8d7d2", font=dict(size=11)),
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=GRID, zeroline=False, tickformat="%H:%M\n%d %b")
    fig.update_yaxes(gridcolor=GRID, linecolor=GRID, zeroline=False, rangemode="tozero")
    return fig


def empty(message, height=120):
    """Return a styled placeholder instead of an unexplained empty axis."""
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, font=dict(color=TEXT_SECONDARY))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return _style(fig, height)


def charging_curve(readings, live, uirevision="keep"):
    """Energy, power and SoC for all active sessions or the latest completed one."""
    if readings.empty:
        return empty("no meter readings stored")

    # Separate unlike units while sharing time, so power does not flatten energy or SoC.
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("cumulative energy (kWh)", "instantaneous power (kW)", "state of charge (%)"),
    )
    sessions = readings.groupby(["connector_id", "session_id"], sort=True)
    measures = (("energy_kwh", "kWh"), ("power_kw", "kW"), ("soc_pct", "%"))

    # A connector/session keeps the same colour in all three panels and one legend entry.
    for i, ((connector, _), session) in enumerate(sessions):
        color = SERIES[i % len(SERIES)]
        vehicle = session["vehicle_model"].iloc[0]
        vehicle = "vehicle not recorded" if pd.isna(vehicle) else str(vehicle)
        name = f"connector {connector} · {vehicle}"
        for row, (column, unit) in enumerate(measures, start=1):
            fig.add_trace(
                go.Scatter(
                    x=session["ts"],
                    y=session[column],
                    mode="lines+markers",
                    name=name,
                    legendgroup=name,
                    showlegend=row == 1,
                    line=dict(color=color, width=2.5),
                    marker=dict(size=8, color=color, symbol="circle",
                                line=dict(color="#ffffff", width=1.5)),
                    hovertemplate=f"%{{y:.1f}} {unit}<extra>{name}</extra>",
                ),
                row=row,
                col=1,
            )

    # Zoom to the observed SoC band while retaining context and never exceeding physical bounds.
    soc = readings["soc_pct"].dropna()
    if len(soc):
        low, high = float(soc.min()), float(soc.max())
        if high - low < 10:
            middle = (low + high) / 2
            low, high = middle - 10, middle + 10
        else:
            padding = (high - low) * 0.15
            low, high = low - padding, high + padding
        fig.update_yaxes(range=[max(0, low), min(100, high)], row=3, col=1)

    fig.update_annotations(font=dict(size=11, color=TEXT_SECONDARY), x=0, xanchor="left")
    _style(fig, 350)
    fig.update_layout(
        showlegend=True,
        legend=dict(y=1.03, yanchor="bottom", yref="container"),
        margin=dict(t=60),
        height=385,
        uirevision=uirevision,
    )
    return fig


def price_history(prices, now, uirevision="keep"):
    """Stored charger tariff changes and AEMO wholesale prices in AUD."""
    if prices.empty:
        return empty("no price events stored")

    # Extend the final step to simulated "now" without inventing another price event.
    x = list(prices["ts"]) + [now]
    tariff = list(prices["price_per_kwh"]) + [prices["price_per_kwh"].iloc[-1]]
    wholesale = list(prices["wholesale_per_kwh"]) + [prices["wholesale_per_kwh"].iloc[-1]]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=tariff, name="charger tariff", mode="lines",
        line=dict(color=SERIES[0], width=2.5, shape="hv"),
        hovertemplate="A$%{y:.2f}/kWh<extra>tariff</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=x, y=wholesale, name="wholesale (AEMO NSW1)", mode="lines",
        line=dict(color=SERIES[1], width=2, shape="hv"),
        hovertemplate="A$%{y:.3f}/kWh<extra>wholesale</extra>",
    ))
    # Large points mark real stored price_update events. The final line point at
    # `now` only extends the price in force and is deliberately not marked.
    fig.add_trace(go.Scatter(
        x=prices["ts"], y=prices["price_per_kwh"], mode="markers",
        name="tariff update", showlegend=False, hoverinfo="skip",
        marker=dict(size=9, color=SERIES[0], line=dict(color="#ffffff", width=1.5)),
    ))
    fig.add_trace(go.Scatter(
        x=prices["ts"], y=prices["wholesale_per_kwh"], mode="markers",
        name="wholesale update", showlegend=False, hoverinfo="skip",
        marker=dict(size=7, color=SERIES[1], line=dict(color="#ffffff", width=1.2)),
    ))
    _style(fig, 220)
    fig.update_layout(showlegend=True, margin=dict(t=34), uirevision=uirevision)

    # Keep rare wholesale spikes from flattening the customer tariff.
    top = max(float(prices["price_per_kwh"].max()) * 1.25, 0.1)
    low = min(float(prices["wholesale_per_kwh"].min()), 0) * 1.1
    fig.update_yaxes(title_text="AUD/kWh", tickprefix="A$", tickformat=".2f",
                     rangemode="normal", range=[max(low, -top), top])
    if prices["wholesale_per_kwh"].max() > top:
        fig.add_annotation(
            text="wholesale spike clipped · hover for value", xref="paper", yref="paper",
            x=1, y=-0.24, xanchor="right", yanchor="top", showarrow=False, font=dict(size=10),
        )
        fig.update_layout(margin=dict(b=48), height=235)
    return fig


def network_hourly(hourly):
    """Stored session starts and their average locked tariff by simulated hour."""
    if hourly.empty:
        return empty("no session starts stored")
    # Counts and tariffs use separate panels because their scales and units differ.
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.14,
        row_heights=[0.6, 0.4],
        subplot_titles=("session starts per hour", "average starting tariff (AUD/kWh)"),
    )
    fig.add_trace(go.Bar(
        x=hourly["hour"], y=hourly["sessions_started"],
        marker=dict(color=SERIES[0]), marker_line_width=0, marker_cornerradius=4,
        hovertemplate="%{y} starts<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=hourly["hour"], y=hourly["avg_session_price"], mode="lines+markers",
        line=dict(color=SERIES[1], width=2.5, shape="hv"),
        marker=dict(size=8, color=SERIES[1], symbol="circle",
                    line=dict(color="#ffffff", width=1.5)),
        hovertemplate="A$%{y:.2f}/kWh<extra></extra>",
    ), row=2, col=1)
    fig.update_annotations(font=dict(size=11, color=TEXT_SECONDARY), x=0, xanchor="left")
    _style(fig, 275)
    fig.update_layout(bargap=0.18, margin=dict(t=32))
    fig.update_yaxes(tickprefix="A$", tickformat=".2f", rangemode="normal", row=2, col=1)
    return fig


def top_chargers(top):
    """Horizontal ranking using kWh so short replay windows retain useful precision."""
    if top.empty:
        return empty("no completed sessions stored")
    # Reverse descending query results so the largest horizontal bar appears at the top.
    top = top.iloc[::-1]
    labels = [
        f"#{row.charger_id} {'' if pd.isna(row.operator_name) else str(row.operator_name)[:18]}"
        for row in top.itertuples()
    ]
    fig = go.Figure(go.Bar(
        x=top["energy_kwh"], y=labels, orientation="h",
        marker=dict(color=SERIES[0]), marker_line_width=0, marker_cornerradius=4,
        text=[f"{value:,.0f} kWh" for value in top["energy_kwh"]],
        textposition="outside", cliponaxis=False, customdata=top["sessions"],
        hovertemplate="%{x:,.1f} kWh · %{customdata} sessions<extra></extra>",
        hoverlabel=dict(bgcolor="#ffffff", bordercolor=SERIES[0], font=dict(color="#111111", size=12)),
    ))
    _style(fig, 260)
    fig.update_layout(hovermode="closest", margin=dict(l=150, r=74, t=8, b=22))
    fig.update_xaxes(showticklabels=False, showgrid=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)", tickfont=dict(size=10))
    return fig
