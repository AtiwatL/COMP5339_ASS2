# NSW EV Charging Event Stream

COMP5339 Assignment 2 - **TUT19_G07**.

This project converts all 1,926 chargers from the Assignment 1 DuckDB database into a one-day synthetic EV charging stream. JSON events are published through MQTT, validated and stored in DuckDB, then displayed on a live Dash dashboard.

```text
A1 DuckDB -> station preparation -> data generation -> stream preparation
                                                           |
                                                    events.parquet
                                                    events.jsonl
                                                           |
                                                           v
publisher -> Mosquitto -> subscriber -> ingestion -> A2 DuckDB -> dashboard
```

## Run locally

Run all Python commands from the repository directory, which contains `requirements.txt` and the `evstream` folder:

```powershell
Set-Location "C:\path\to\COMP5339_ASS2"
```

Choose either Conda or Python `venv`. Only one environment is needed.

### Python environment option A: Conda

Create the environment:

```powershell
conda create --name comp5339-a2 python=3.13 -y
conda activate comp5339-a2
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Activate it again in every new terminal that runs a Python component:

```powershell
conda activate comp5339-a2
```

### Python environment option B: `venv` and pip

Create and activate a standard Python virtual environment on Windows PowerShell:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

On macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

In each later terminal, activate the same environment before running a Python command:

```powershell
# Conda
conda activate comp5339-a2

# OR Windows venv
.\.venv\Scripts\Activate.ps1
```

```bash
# OR macOS/Linux venv
source .venv/bin/activate
```

Confirm that the required packages are available:

```powershell
python --version
python -c "import duckdb, paho.mqtt.client, dash; print('dependencies OK')"
```

Install DuckDB's spatial extension once if necessary:

```powershell
python -c "import duckdb; duckdb.connect().execute('INSTALL spatial')"
```

## Summary execution order

Run the preparation commands once when rebuilding the dataset:

```powershell
python -m evstream.stations
python -m evstream.generation
python -m evstream.stream
python -m evstream.db --reset
```

For each live local run, start the components in this order:

```text
1. Mosquitto broker
2. Dashboard with embedded MQTT subscriber
3. MQTT publisher
4. Open http://127.0.0.1:8050
```

If the generated files already exist, do not rerun the four preparation commands. Apply non-destructive schema updates with `python -m evstream.db`, then start the three live components.

### 1. Initialise the A2 database

```powershell
python -m evstream.db
```

This applies the schema without deleting stored events. To discard all A2 events and rebuild from the A1 copy, stop the dashboard first and run:

```powershell
python -m evstream.db --reset
```

### 2. Install and start Mosquitto

Install the local MQTT broker if it is not already available:

```powershell
winget install --id EclipseFoundation.Mosquitto -e
```

Open a dedicated terminal in the repository directory and start it:

```powershell
& "C:\Program Files\mosquitto\mosquitto.exe" -c .\mosquitto\mosquitto.conf
```

If Mosquitto was installed elsewhere, replace the executable path. If `mosquitto` is already on `PATH`, the shorter command is:

```powershell
mosquitto -c .\mosquitto\mosquitto.conf
```

### 3. Start the dashboard and embedded subscriber

Open a second terminal in the repository directory. Activate whichever Python environment you created, then run:

```powershell
python -m evstream.dashboard.app --mqtt-host localhost --mqtt-port 1883
```

Open <http://127.0.0.1:8050>. The dashboard process starts a background MQTT subscriber and writes received events to DuckDB.

### 4. Publish a demonstration window

Open a third terminal in the repository directory. Activate the same environment, then run:

```powershell
python -m evstream.publisher `
    --host localhost `
    --from "2026-10-01 09:00" `
    --until "2026-10-01 13:00"
```

## Current dataset

The configuration in `evstream/config.py` generates one simulated day:

```python
SIM_START = "2026-10-01"
SIM_DAYS = 1
```

| Output | Count |
|---|---:|
| Chargers | 1,926 |
| Hourly prices | 46,224 |
| Charging sessions | 9,576 |
| Meter readings | 120,093 |
| Price-change events | 32,742 |
| Complete event stream | 171,987 |

Sessions and prices are generated for 1 October 2026. A late session can finish after midnight on 2 October; that is a spill-over session, not a second generated day.

The complete stream takes about 4 hours 47 minutes at 0.1 seconds per event, so a demonstration normally publishes a smaller continuous simulated-time window.

## Assignment 1 audit and station preparation

`A1_charger_check.ipynb` is a read-only data-quality audit. It reports problems in the Assignment 1 source, including missing charger types, missing power ratings, type/rating conflicts and missing generator-required attributes. The notebook deliberately does not fill values, transform records or produce generator input.

The production solution is `evstream/stations.py`. It reads the normalized A1 tables and prepares every charger for the supplied generator by:

- retaining `charger_id` as `station_id`;
- deriving a display name from operator and address because A1 has no station-name field;
- joining coordinates and geographic classifications;
- deriving the urban/regional `site_type` used by the generator;
- ensuring a usable connector count;
- selecting the highest recorded charger rating;
- filling missing AC ratings with the observed A1 AC median;
- inferring missing AC/DC simulation types from the available rating;
- recording whether power came from A1 or from the documented default.

The data-generation workflow is therefore:

```text
A1_charger_check.ipynb       documents source-data problems only
          |
          v
evstream/stations.py         applies the production preparation rules
          |
          +--> stations_input_ac.csv
          `--> stations_input_dc.csv
                         |
                         v
evstream/generation.py       runs the supplied generator from those inputs
```

The generator does not query or modify the A1 database directly. Its station population is based on the two input files written by `stations.py`.

Run the station preparation and data generation from the repository directory:

```powershell
python -m evstream.stations
python -m evstream.generation
```

The first command writes the AC/DC station inputs. The second command passes those files to the supplied generator and merges its station, hourly-price, session and metering outputs into `data/generated`.

## Rebuild generated data

The generated outputs are already included. To reproduce them:

```powershell
# A1 charger records -> AC and DC simulator inputs
python -m evstream.stations

# Run the provided generator and merge AC/DC outputs
python -m evstream.generation

# CSV data -> ordered Parquet and JSONL stream
python -m evstream.stream

# Fresh A1 working copy with empty A2 event tables
python -m evstream.db --reset
```

Generation parameters are:

| Group | Urban sessions/connector/day | Regional sessions/connector/day | Meter interval |
|---|---:|---:|---:|
| DC | 4.0 | 3.0 | 2 minutes |
| AC | 1.0 | 0.6 | 10 minutes |

Both groups use the same day and AEMO NSW1 wholesale-price series. Their outputs are merged into `data/generated`.

## Publishing options

```powershell
# Complete stream
python -m evstream.publisher --host localhost

# First 100 messages
python -m evstream.publisher --host localhost --limit 100

# Continue after the saved acknowledged sequence
python -m evstream.publisher --host localhost --resume

# Send all events through the local broker without the artificial delay
python -m evstream.publisher --host localhost --backfill
```

Backfill still sends JSON through Mosquitto and the subscriber still validates and stores every message. It only removes the 0.1-second demonstration delay. It is restricted to a local broker and recorded in `data/stream/publish_runs.jsonl`.

### Window bootstrap

When `--from` is supplied, the publisher first sends:

- each charger's latest earlier price event;
- the start event of each session already active at the window boundary.

These are unchanged original events. They give the dashboard the state needed to interpret the selected window. The publisher then sends all events inside the window in sequence order. Use `--no-bootstrap` only when deliberately testing a raw window.

Progress is stored in `data/stream/publisher_checkpoint.json`.

## MQTT stream

The topic format is:

```text
COMP5339/TUT19_G07/ev/<charger_id>/<event_type>
```

| Event type | Meaning |
|---|---|
| `price_update` | Customer and wholesale tariff update |
| `session_start` | Vehicle connected and session began |
| `meter` | Voltage, current, power, cumulative energy and state of charge |
| `session_end` | Final energy, duration, power and cost |

The publisher reads `events.parquet`, but sends its `message` column as JSON. Parquet itself is not sent through MQTT. `events.jsonl` contains the same complete stream in human-readable form.

Example:

```json
{
  "v": 1,
  "seq": 1927,
  "event_id": "ss-1846-1-202610010000",
  "event_type": "session_start",
  "event_ts": "2026-10-01T00:00:00+10:00",
  "charger_id": 1846,
  "connector_id": 1,
  "session_id": "1846-1-202610010000",
  "data": {
    "vehicle_model": "Tesla Model Y",
    "price_per_kwh": 0.46
  }
}
```

Events are sorted by simulated timestamp. Ties use:

```text
price_update -> session_start -> meter -> session_end
```

`seq` is the global publishing order. `event_id` is the stable identity used for MQTT QoS 1 de-duplication.

## Subscription and ingestion

Normal live mode runs the subscriber and dashboard together:

```text
one dashboard process
|-- Paho MQTT background thread
|   `-- receives COMP5339/TUT19_G07/ev/+/+
|-- EventIngestor
|   `-- validates and writes DuckDB transactions
|-- Dash callbacks
|   `-- poll committed state every 2.5 seconds
`-- one DuckDB database file
```

Each new message is processed atomically:

```text
BEGIN
  validate envelope and event-specific fields
  insert immutable event_log row
  insert typed event row
  update charger_live_state
COMMIT
acknowledge the QoS 1 MQTT message
```

An identical redelivery is returned as a duplicate and not inserted again. A conflicting message that reuses an existing `event_id` is rejected.

For headless ingestion without the dashboard:

```powershell
python -m evstream.subscriber --host localhost --port 1883
```

Do not run the standalone subscriber and embedded dashboard subscriber together. To view an existing database without subscribing:

```powershell
python -m evstream.dashboard.app --no-ingest
```

## Database design

The original `data/a1/ev_chargers_a1.duckdb` is not modified. `evstream.db` copies it to `data/db/ev_chargers.duckdb` and adds only the A2 event tables and views. 


| Object | Purpose |
|---|---|
| `event_log` | Immutable envelope and canonical JSON for each unique event |
| `price_event` | Customer and wholesale price history |
| `charging_session_core` | Stable session, charger and connector identity |
| `session_start_event` | Immutable session-start fact |
| `meter_reading` | Append-only telemetry |
| `session_end_event` | Immutable final session facts |
| `charger_live_state` | Current price, active-session count and latest event time |
| `charging_session` | View combining the session tables |
| `v_charger_live` | A1 details plus current streamed state |
| `v_active_sessions` | Active sessions and their latest meter reading |
| `v_charger_usage_daily` | Completed usage grouped by charger and day |
| `v_network_hourly` | Hourly session starts, delivered energy and average tariff |

Historical event tables are append-only. Only `charger_live_state` is updated, allowing fast current-state dashboard queries.

## Dashboard information

The dashboard includes a charger map, live network metrics, stored historical metrics, charging curves, price history, hourly network activity, top chargers and a recent-event feed.

Each charger panel explicitly displays:

- **Name**, rendered as `<operator> - <address>` without storing another A1 charger attribute;
- **Address** from the A1 charging site;
- **Operator** network from A1;
- **Plug types** from A1;
- **Latest price** received from MQTT and its simulated update time;
- charger ID, type, maximum power, connector count, area and status.

Historical labels refer to **stored streamed history**, not a fixed seven-day dataset. With the current one-day simulation, charts show only events actually received for that day and any late session completions. No unavailable days are fabricated.

## Project structure

```text
COMP5339_ASS2/
|-- README.md
|-- A1_charger_check.ipynb
|-- requirements.txt
|-- mosquitto/mosquitto.conf
|-- generator/generate_dataset.py
|-- sql/02_schema_a2_events.sql
|-- evstream/
|   |-- config.py
|   |-- stations.py
|   |-- generation.py
|   |-- stream.py
|   |-- publisher.py
|   |-- subscriber.py
|   |-- ingest.py
|   |-- db.py
|   `-- dashboard/
`-- data/
    |-- a1/ev_chargers_a1.duckdb
    |-- generated/
    |-- stream/
    `-- db/ev_chargers.duckdb
```

## Important outputs

| Path | Description |
|---|---|
| `data/generated/stations_input_ac.csv` | A1 AC chargers prepared for simulation |
| `data/generated/stations_input_dc.csv` | A1 DC chargers prepared for simulation |
| `data/generated/stations.csv` | Merged generated stations |
| `data/generated/prices_hourly.csv` | Hourly customer and wholesale prices |
| `data/generated/sessions.csv` | Generated sessions |
| `data/generated/session_metering.csv` | Session telemetry |
| `data/stream/events.parquet` | Ordered stream and JSON messages |
| `data/stream/events.jsonl` | Complete JSON Lines stream |
| `data/stream/publisher_checkpoint.json` | Latest acknowledged sequence |
| `data/stream/publish_runs.jsonl` | Publisher run audit log |
| `data/db/ev_chargers.duckdb` | A1 copy extended with A2 events |


## Assumptions

- Generated `station_id` equals the A1 `charger_id`.
- Site type controls generator calibration but is not included in MQTT payloads or stored as an A2 event attribute.
- The 518 AC chargers missing an A1 rating use the median AC rating of 22 kW for simulation.
- One connector can serve at most one vehicle at a time.
- Occupancy continues until unplug time (`end_ts`), including post-charge idle time.
- Meter `energy_kwh` is cumulative; interval energy is derived from consecutive readings.
- Simulation timestamps use fixed AEST (UTC+10), matching the generator's market-time handling.
