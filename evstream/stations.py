"""Task 1a - export every Assignment 1 charger as a generator station.

The provided generator expects a CSV with the columns
    station_id, name, lat, lon, site_type, connectors, max_kw
This module derives those columns from the normalised Assignment 1 DuckDB database:

* station_id  = charger.charger_id, so every synthetic event joins back to A1 without a lookup table.
* name        = "<operator> - <address>" (A1 has no charger name column).
* lat / lon   = charging_site coordinates.
* site_type   = "urban" for Greater Sydney plus the Newcastle and Illawarra SA4s, else "regional"
                (the generator's ARENA calibration distinguishes metropolitan from regional sites).
* connectors  = charger.number_of_plugs (at least 1).
* max_kw      = highest parsed power component; AC chargers without a rating receive the median
                AC rating observed in A1 (documented as power_source = 'ac_median_default').
* sim_group   = "dc" or "ac". The groups are generated separately because public AC (destination)
                chargers are used far less often than DC fast chargers.

Run:  python -m evstream.stations
"""
import duckdb

from evstream import config

URBAN_SA4_OUTSIDE_GSYD = ("Newcastle and Lake Macquarie", "Illawarra")

EXPORT_SQL = f"""
WITH rating AS (
    SELECT charger_id, max(power_kw) AS max_kw
    FROM charger_power_rating
    GROUP BY charger_id
),
ac_default AS (                       -- data-driven default for unrated AC chargers
    SELECT quantile_disc(r.max_kw, 0.5) AS kw
    FROM rating r JOIN charger c USING (charger_id)
    WHERE c.charger_type = 'AC'
)
SELECT
    c.charger_id                                             AS station_id,
    coalesce(o.operator_name, 'Unknown operator') || ' - ' || s.station_address AS name,
    s.latitude                                               AS lat,
    s.longitude                                              AS lon,
    CASE WHEN r4.gcc_code = '1GSYD' OR r4.sa4_name IN {URBAN_SA4_OUTSIDE_GSYD}
         THEN 'urban' ELSE 'regional' END                    AS site_type,
    greatest(coalesce(c.number_of_plugs, 1), 1)              AS connectors,
    coalesce(r.max_kw, (SELECT kw FROM ac_default))          AS max_kw,
    -- Upcoming records have no AC/DC type in A1 but are rated 350 kW, i.e. ultra-fast DC sites.
    coalesce(c.charger_type, CASE WHEN r.max_kw >= 50 THEN 'DC' ELSE 'AC' END) AS charger_type_sim,
    CASE WHEN r.max_kw IS NULL THEN 'ac_median_default' ELSE 'a1_power_rating' END AS power_source,
    c.charger_status,
    lower(coalesce(c.charger_type, CASE WHEN r.max_kw >= 50 THEN 'DC' ELSE 'AC' END)) AS sim_group
FROM charger c
JOIN charging_site s USING (site_id)
JOIN sa4_region r4 USING (sa4_code)
LEFT JOIN operator o USING (operator_id)
LEFT JOIN rating r USING (charger_id)
ORDER BY c.charger_id
"""


def export_stations():
    con = duckdb.connect(str(config.A1_DB), read_only=True)
    con.execute("INSTALL spatial; LOAD spatial")  # sa4_region stores a GEOMETRY column
    df = con.execute(EXPORT_SQL).df()
    con.close()

    config.GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(config.STATIONS_CSV, index=False)
    for group, part in df.groupby("sim_group"):
        part.to_csv(config.GENERATED_DIR / f"stations_input_{group}.csv", index=False)

    print(f"Exported {len(df)} chargers -> {config.STATIONS_CSV.relative_to(config.PROJECT_ROOT)}")
    print(df.groupby(["sim_group", "site_type"]).agg(chargers=("station_id", "size"),
                                                     connectors=("connectors", "sum"),
                                                     median_kw=("max_kw", "median")))
    print(df["power_source"].value_counts().to_string())
    return df


if __name__ == "__main__":
    export_stations()
