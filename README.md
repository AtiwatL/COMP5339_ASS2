# NSW EV Charging Event Stream

COMP5339 Assignment 2 - **TUT19_G07**.

This project converts all 1,926 chargers from the Assignment 1 DuckDB database into a one-day synthetic EV charging stream. JSON events are published through MQTT, validated and stored in DuckDB, then displayed on a live Dash dashboard.

MQTT always uses the local Mosquitto broker at `localhost:1883`; the run commands do not accept broker-address overrides.

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

**Expected output after database initialisation:** `data/db/ev_chargers.duckdb` contains a working copy of the Assignment 1 charger tables plus the Assignment 2 event tables and views listed below. A fresh database has no received events yet; `python -m evstream.db` preserves any events already stored, while `--reset` starts with empty A2 event history.

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
python -m evstream.dashboard.app
```

Open <http://127.0.0.1:8050>. The dashboard process starts a background MQTT subscriber and writes received events to DuckDB.

### 4. Publish a demonstration window

Open a third terminal in the repository directory. Activate the same environment, then run:

```powershell
python -m evstream.publisher `
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

### How generation works

```text
A1 DuckDB charger/site/operator/rating/region tables
                 |
                 v
evstream.stations -> stations_input_ac.csv + stations_input_dc.csv
                 |
                 v
evstream.generation -> supplied generator (AC and DC runs)
                 |
                 v
stations.csv + prices_hourly.csv + sessions.csv + session_metering.csv
```

`stations.py` exports the generator's required `station_id`, name, coordinates, site type, connector count and maximum power from the A1 tables. It keeps `station_id = charger_id`, which lets every later event join directly to the original charger. Greater Sydney, Newcastle and Lake Macquarie, and Illawarra are classified as urban; other SA4 regions are regional. Chargers without a recorded AC rating use the observed AC median of 22 kW for simulation. These preparation rules affect generator input; they do not alter the A1 database.

`generation.py` runs the supplied generator twice: once for DC chargers and once for AC chargers. The groups use different session rates and meter intervals (shown below), but share the same 1 October 2026 simulation date and AEMO NSW1 wholesale-price series. It merges the four generated CSV types, adds a `sim_group` column, and checks whether the AEMO data covers every simulated hour. `stations.csv` contains station details; `prices_hourly.csv` contains hourly tariffs; `sessions.csv` contains session starts, ends and final totals; and `session_metering.csv` contains readings during sessions. Some sessions finish after midnight on 2 October.

**Expected output after generation:** `stations.py` prints the number of exported chargers, the AC/DC and urban/regional breakdown, and power-source counts. `generation.py` prints row counts for each merged CSV and AEMO hourly coverage. The current files in `data/generated/` contain 1,926 stations, 46,224 hourly price rows, 9,576 sessions and 120,093 meter readings. The two `stations_input_*.csv` files are the generator inputs; the four merged CSVs are the inputs for stream preparation. No MQTT messages or A2 database events are created at this stage.

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

### How the event stream is prepared

```text
Four merged generated CSVs
          |
          v
evstream.stream -> event creation -> price-change filtering -> time ordering
          |
          +--> data/stream/events.parquet (publisher input)
          `--> data/stream/events.jsonl (readable message copy)
```

Run `python -m evstream.stream` after generation. It reads the merged price, session and metering CSVs and creates `price_update`, `session_start`, `meter` and `session_end` events. The first hourly price for each charger is published; later hourly rows become events only when that charger's customer price changes. A start event contains the connector, vehicle and price known at connection time. Meter events contain electrical readings, cumulative energy and state of charge. End events contain final duration, energy and cost.

Events are ordered by simulated time, then by event type (`price_update`, `session_start`, `meter`, `session_end`) and stable identifiers. A global `seq` records that order; a stable `event_id` supports duplicate detection. The script verifies unique event IDs and checks that no meter or end event precedes its session start. It writes an ordered Parquet file for the publisher and a JSONL copy of the same messages for inspection and submission. The current stream has 171,987 events, from 1 October 00:00:00 to 2 October 03:56:41 AEST.

**Expected output after stream preparation:** The command prints the total event and charger counts, the first and last event times, a count by event type, and how many unchanged hourly prices were removed. `data/stream/events.parquet` and `data/stream/events.jsonl` should each represent the same 171,987 ordered messages. The current breakdown is 32,742 `price_update`, 9,576 `session_start`, 120,093 `meter` and 9,576 `session_end` events. These are prepared files; they have not yet been sent to Mosquitto.

## Publishing options

```text
events.parquet JSON messages -> evstream.publisher -> local Mosquitto
                               |                       |
                               v                       v
                    checkpoint + run log      charger/event MQTT topics
```

Start the local Mosquitto broker and the dashboard/subscriber before publishing. The publisher reads the JSON `message` field from `events.parquet` in sequence order and sends one message per event to `COMP5339/TUT19_G07/ev/<charger_id>/<event_type>`. It uses MQTT QoS 1, waits for publish acknowledgements, and saves the highest safely acknowledged sequence in `publisher_checkpoint.json`. Normal publishing spaces messages by at least 0.1 seconds; publishing the full stream therefore takes about 4 hours 47 minutes before other overhead. Use a time window for a short live demonstration.

```powershell
# Complete stream
python -m evstream.publisher

# First 100 messages
python -m evstream.publisher --limit 100

# Continue after the saved acknowledged sequence
python -m evstream.publisher --resume

# Send all events through the local broker without the artificial delay
python -m evstream.publisher --backfill
```

Backfill still sends JSON through Mosquitto and the subscriber still validates and stores every message. It only removes the 0.1-second demonstration delay. It is restricted to a local broker and recorded in `data/stream/publish_runs.jsonl`.

`--limit` stops after a chosen number of messages, while `--resume` continues after the saved acknowledged sequence. `--from` and `--until` select a simulated-time window. The publisher records each run's mode, delay and sequence range in `publish_runs.jsonl`. Backfill is intended to load history through the same MQTT ingestion path before a normal-speed demonstration.

**Expected output after publishing:** The terminal reports the local broker connection, publish progress and the final number of messages sent. Mosquitto receives JSON messages on the charger/event topics; the subscriber can then ingest them. `data/stream/publisher_checkpoint.json` records the latest safely acknowledged sequence, and `data/stream/publish_runs.jsonl` gains one audit record for the run. A window or `--limit` run sends only its selected events (plus any bootstrap events for `--from`), so its count can be smaller than 171,987. These files record publishing progress; the subscriber controls what has been committed to DuckDB.

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

```text
Local Mosquitto -> MQTT topic/payload check -> event validation
                                              |
                                              v
                          one DuckDB transaction per new event
                         /             |                 \
                    event_log      typed event table    charger_live_state
                         \             |                 /
                          `------------+----------------'
                                       |
                                       v
                           MQTT acknowledgement
```

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

The Paho client subscribes to every charger and event type using the `+/+` wildcard. It checks that the MQTT topic agrees with the payload's charger ID and event type. `EventIngestor` then validates the JSON envelope and event-specific fields. Database-dependent checks and writes happen inside one transaction for each message: an immutable `event_log` row, the matching typed event row, and any change to `charger_live_state` commit together. The subscriber manually acknowledges a QoS 1 message only after it is committed or confirmed as an identical duplicate. Invalid messages are logged and acknowledged; unexpected database errors leave the message unacknowledged for retry.

Each new message is processed atomically:

```text
BEGIN
  check database-dependent event consistency
  insert immutable event_log row
  insert typed event row
  update charger_live_state
COMMIT
acknowledge the QoS 1 MQTT message
```

The JSON envelope and event-specific fields are validated before `BEGIN`. A repeated `event_id` with identical content is a harmless redelivery; the ingestor does not insert it again. Reusing an `event_id` with different content is rejected. This yields one stored copy per event ID despite MQTT's at-least-once delivery.

### Standalone subscriber: ingest only

After starting Mosquitto and initialising the A2 database, run this command in its own terminal to receive and store events without opening a web dashboard:

```powershell
python -m evstream.subscriber
```

This process only subscribes to MQTT, validates messages and writes them to DuckDB. It does not start Dash, serve a page at port 8050, draw a map or calculate dashboard figures. Start the publisher in another terminal to send events to it. Stop the subscriber with Ctrl+C when ingestion is complete.

The normal dashboard command below starts its own embedded subscriber, so do not run both subscriber modes at once. To view an already populated database without subscribing to MQTT, use:

```powershell
python -m evstream.dashboard.app --no-ingest
```

**Expected output after subscription and ingestion:** The standalone subscriber prints its connection and topic subscription, then reports received, stored, duplicate and rejected counts when it stops. It produces no dashboard or HTTP address. Each accepted new message adds one `event_log` row and one corresponding typed event row in `data/db/ev_chargers.duckdb`; price and session events also update the appropriate current state. Replayed identical messages increase the duplicate count without adding new rows. Counts in DuckDB reflect messages actually received, so a short demonstration window will not populate the entire generated history.

## Database design

```text
A1 DuckDB copy + A2 schema -> data/db/ev_chargers.duckdb
                                    |
                  received events -> event tables + live-state table
                                    |
                                    v
                         dashboard views and queries
```

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

```text
Local Mosquitto -> embedded Subscriber -> DuckDB committed state
                                               |
                                               v
                                      dashboard/queries.py
                                               |
                               +---------------+----------------+
                               |                                |
                         dashboard/app.py                dashboard/figures.py
                               |                                |
                               +--------------+-----------------+
                                              |
                                 Dash page + Leaflet map
                                              |
                           assets/map.js + assets/style.css
```

The dashboard includes a charger map, live network metrics, stored historical metrics, charging curves, price history, hourly network activity, top chargers and a recent-event feed.

For the live dashboard with subscription and ingestion, start Mosquitto first, then run:

```powershell
python -m evstream.dashboard.app
```

Open `http://127.0.0.1:8050` and start the publisher in a separate terminal. This one dashboard process starts an embedded MQTT subscriber in a background thread; there is no need to run `python -m evstream.subscriber` as well. It receives, validates and stores events while Dash polls committed DuckDB state every 2.5 seconds. The map joins A1 locations and charger attributes with `charger_live_state`; markers show whether a charger has reported and its occupancy, and clicking one opens details. Operator, charger-type and idle-charger controls narrow the displayed markers. The map refreshes its features when the data or filters change, while an open charger panel continues to refresh.

### Dashboard folder: files and execution

Run the package entry point from the repository root, not by opening an individual file in `evstream/dashboard/`. `python -m evstream.dashboard.app` imports `queries.py` and `figures.py`, creates the Dash layout and callbacks, starts the embedded subscriber, and serves the page. Dash loads files in the `assets/` folder automatically when the page opens.

| File | Purpose |
|---|---|
| `dashboard/app.py` | Starts the server and embedded subscriber; defines the page layout, map features, filters, popup, metrics and refresh callbacks. |
| `dashboard/queries.py` | Reads committed DuckDB data for map markers, current state, charger history, charging curves, price history, network metrics and event feed. It does not ingest MQTT messages. |
| `dashboard/figures.py` | Converts query results into Plotly charging, price-history, hourly-activity and top-charger charts, including empty-state figures. |
| `dashboard/assets/map.js` | Supplies Leaflet marker and cluster rendering for the GeoJSON map, including price and occupancy labels. |
| `dashboard/assets/style.css` | Styles the page layout, markers, popup, metric cards, filters and responsive display. |

On each refresh, `app.py` asks `queries.py` for the latest stored state and passes chart data to `figures.py`. The browser uses `map.js` and `style.css` to render that state. Only the embedded `Subscriber` and `EventIngestor` write events; dashboard queries read committed rows.

The top metrics separate current state (active sessions, chargers in use, current power and average live price) from stored history (completed sessions, delivered energy and revenue). A selected charger's panel shows its latest price and update time, active connector count, current charging power, completed-session statistics, a meter-based charging curve and price history. If no session is active, the curve shows the latest completed session. The side panel shows hourly network activity, top chargers by delivered energy and a recent-event feed. The dashboard's simulated clock follows the latest stored event timestamp; it is not the computer's wall clock.

Each charger panel explicitly displays:

- **Name**, rendered as `<operator> - <address>` without storing another A1 charger attribute;
- **Address** from the A1 charging site;
- **Operator** network from A1;
- **Plug types** from A1;
- **Latest price** received from MQTT and its simulated update time;
- charger ID, type, maximum power, connector count, area and status.

Historical labels refer to **stored streamed history**, not a fixed seven-day dataset. With the current one-day simulation, charts show only events actually received for that day and any late session completions. No unavailable days are fabricated.

**Expected output after starting the dashboard:** The app is available at `http://127.0.0.1:8050`. Before any events are received, it shows charger locations and a waiting-for-events state. While the publisher runs, the simulated timestamp, marker state, current metrics, selected charger details, charging curve and event feed update from committed DuckDB data. Historical charts and totals grow as session events arrive. The dashboard itself creates no new synthetic events; its displayed history is limited to what the subscriber has stored.

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
|   |-- db.py
|   |-- stream.py
|   |-- publisher.py
|   |-- subscriber.py
|   |-- ingest.py
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
