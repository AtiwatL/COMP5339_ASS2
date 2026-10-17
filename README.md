# NSW EV Charging - Data Streaming and Analysis

COMP5339 Data Engineering, Assignment 2 (group TUT19_G07).

This project simulates a real-time event stream for **all 1,926 EV chargers of our Assignment 1
database**, publishes it over MQTT, stores every received event in DuckDB as an extension of the
Assignment 1 schema, and visualises it on a live map dashboard.

```
 A1 DuckDB ──► stations ──► generator (AEMO NSW1 prices) ──► ordered event stream (Parquet)
                                                                      │
                                                                      ▼  publisher (≥0.1 s, QoS 1)
                                                               MQTT broker
                                                                      │  subscribe COMP5339/TUT19_G07/ev/+/+
                                                                      ▼
                                 dashboard process:  IngestService ──► DuckDB (A1 + event tables)
                                                     Dash + Leaflet map ◄──┘
```

## Project structure

```text
ev_streaming/
|-- README.md                       this file
|-- requirements.txt                pinned Python packages
|-- mosquitto/mosquitto.conf        local MQTT broker configuration
|-- generator/
|   |-- generate_dataset.py         provided dataset generator (unmodified)
|   `-- GENERATOR_README.md         provided documentation of the generator
|-- sql/
|   |-- 01_schema_a1.sql            Assignment 1 DDL (unchanged)
|   `-- 02_schema_a2_events.sql     Assignment 2 DDL: event tables, indexes, views
|-- evstream/                       Python package (run each stage with python -m)
|   |-- config.py                   paths, simulation window, broker and topic settings
|   |-- stations.py                 Task 1a  A1 chargers -> generator station list
|   |-- generation.py               Task 1b  run generator (DC and AC groups) and merge outputs
|   |-- stream.py                   Task 2a  ordered, de-duplicated event stream + JSON messages
|   |-- publisher.py                Task 2b  MQTT publisher (>=0.1 s delay, QoS 1, resume)
|   |-- ingest.py                   Task 3/4 MQTT subscriber + transactional DuckDB writer
|   |-- subscriber.py               standalone (headless) subscriber
|   |-- db.py                       Task 4   create the A2 database from the A1 database
|   `-- dashboard/
|       |-- app.py                  Task 3/4 Dash dashboard (map, pop-up, KPIs) with embedded ingest
|       |-- queries.py              SQL used by the dashboard
|       |-- figures.py              Plotly charts
|       `-- assets/                 map marker JavaScript and CSS
`-- data/
    |-- a1/ev_chargers_a1.duckdb    copy of the Assignment 1 database (input, never modified)
    |-- generated/                  Task 1 outputs (see "Outputs")
    |-- stream/                     Task 2 event stream, sample, publisher checkpoint
    `-- db/ev_chargers.duckdb       Task 4 database: A1 tables + stored MQTT events
```

## Setup

Python 3.11-3.13 is required (developed with 3.13.9). From the `ev_streaming/` folder:

```bash
python -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

DuckDB's `spatial` extension (used by the A1 tables) is installed automatically the first time a
database connection is opened, so internet access is required once. Internet access is also needed
for the AEMO download (Task 1) and for the public MQTT broker.

**MQTT broker.** By default the public broker `broker.hivemq.com:1883` (as in the Week 9 tutorial)
is used under the group-specific topic root `COMP5339/TUT19_G07/ev`, so no installation is needed.
For long runs, backfills and the demo, a local Mosquitto broker with the project configuration is
recommended (it is more reliable and never drops queued messages):

```bash
brew install mosquitto                                  # macOS; Windows: installer from mosquitto.org
mosquitto -c mosquitto/mosquitto.conf                   # own terminal; macOS Homebrew path if not on PATH:
                                                        #   /opt/homebrew/opt/mosquitto/sbin/mosquitto
export MQTT_HOST=localhost                              # all scripts read MQTT_HOST / MQTT_PORT
                                                        # (PowerShell: $env:MQTT_HOST="localhost")
```

`mosquitto/mosquitto.conf` removes Mosquitto's default limit of 1,000 queued messages per
subscriber (messages beyond it are silently dropped) and raises the in-flight window, which the
subscriber needs because it acknowledges messages only after they are committed to DuckDB.

## Execution

The included `data/` folder already contains every output, so steps 1-3 are only needed to
reproduce them. Run all commands from `ev_streaming/` with the virtual environment active.

| Step | Command | Result |
|---|---|---|
| 1 | `python -m evstream.stations` | `data/generated/stations_input*.csv` from the A1 database |
| 2 | `python -m evstream.generation` | generated prices, sessions, meter data (≈30 s) |
| 3 | `python -m evstream.stream` | `data/stream/events.parquet` (2.43 M ordered events) |
| 4 | `python -m evstream.db --reset` | fresh `data/db/ev_chargers.duckdb` = A1 + event schema |
| 5 | `python -m evstream.dashboard.app` | subscribe, store and visualise; open <http://127.0.0.1:8050> |
| 6 | `python -m evstream.publisher --from "2026-10-01 07:45"` | publish (second terminal) |

Notes:

* Start the dashboard (step 5) **before** the publisher (step 6), so no message is missed.
  The dashboard embeds the subscriber; use `python -m evstream.subscriber` instead for headless
  ingestion. Never run both at once (DuckDB allows one writing process, and both use the same
  MQTT client id).
* Publisher options: `--from/--until` replay a window of event time, `--limit N` stops after N
  messages, `--resume` continues after the last acknowledged message (`data/stream/publisher_checkpoint.json`),
  `--delay` sets the gap between messages (minimum 0.1 s, enforced). Without `--from` the full
  stream is published from 1 Oct 00:00.
* `python -m evstream.dashboard.app --no-ingest` opens the stored database read-only (e.g. for
  browsing the submitted database without a broker).
* `python -m evstream.verify` (with the dashboard/subscriber stopped) checks that every acknowledged
  message is stored exactly once and that the tables are consistent.

### How the submitted database was filled, and how to run the demo

Publishing all 2.43 M events at 0.1 s would take about 67 hours, so the history was **backfilled**
through the same MQTT pipeline (same topics, messages, QoS 1, subscriber and tables) without the
delay, using the explicit `--backfill` mode (local broker only). The live part of the stream is
then published normally with the enforced ≥ 0.1 s delay:

```bash
# 1. history: 1 Oct 00:00 -> 14 Oct 07:00, backfilled (about 10 minutes)
mosquitto -c mosquitto/mosquitto.conf                                    # terminal 1
MQTT_HOST=localhost python -m evstream.subscriber                       # terminal 2 (stop with Ctrl-C when done)
python -m evstream.publisher --backfill --host localhost --until "2026-10-14 07:00"   # terminal 3

# 2. live demo: continues from 14 Oct 07:00 with the 0.1 s delay
MQTT_HOST=localhost python -m evstream.dashboard.app                     # terminal 2
MQTT_HOST=localhost python -m evstream.publisher --resume               # terminal 3
```

After ingesting, stop the subscriber and run `python -m evstream.db --compact` (rewrites the file
without the free blocks left by the many small upserts; 226 MB -> 192 MB) and `python -m evstream.verify`.

Every publishing run is recorded in `data/stream/publish_runs.jsonl` (mode, delay, seq range,
message count), and `verify` reports the backfill and live totals. To rehearse the demo more than
once, copy `data/db/ev_chargers.duckdb` and `data/stream/publisher_checkpoint.json` before the
rehearsal and restore both afterwards.

## Task 1 - synthetic data generation

* **Station list** (`stations.py`). Every A1 charger becomes one generator station:
  `station_id = charger_id` (so each event maps back to A1 without a lookup table), coordinates
  from `charging_site`, `connectors = number_of_plugs`, `max_kw =` highest parsed power component,
  `site_type = urban` for Greater Sydney and the Newcastle/Illawarra SA4s, otherwise `regional`.
* **Two simulation groups** (`generation.py`). The generator is calibrated on ARENA DC fast-charging
  data. DC chargers (incl. the 98 *Upcoming* 350 kW sites) use the generator defaults
  (4/3 sessions per connector per day urban/regional, meter every 2 min). AC destination chargers
  are used far less, so they use 1.0/0.6 sessions per connector per day and a meter reading every
  10 min. Both groups use the same 14-day window (1-14 Oct 2026) and the same real AEMO NSW1
  wholesale prices (336/336 hours covered).
* **Result**: 1,926 chargers, 647,136 hourly prices, 135,855 sessions, 1,707,161 meter readings.
  Generated averages: DC urban 29.7 min / 25.4 kWh, DC regional 27.1 min / 25.3 kWh
  (ARENA 2023: 27.3 / 27.0 and 25.9 / 24.5).

## Task 2 - event stream and MQTT publishing

`stream.py` prepares the time series for publishing:

1. `price_update` - only **actual price changes** per charger (647,136 hourly rows -> 448,758 changes;
   the first price of each charger is always sent).
2. `session_start` - at plug-in, with only the facts known then (connector, vehicle, tariff).
3. `meter` - each meter reading (voltage, current, power, cumulative kWh, SoC).
4. `session_end` - at unplug, with totals (energy, average/peak kW, cost).

All events are merged and sorted by event time (ties: price -> start -> meter -> end) and numbered
with a global `seq`. The stream is checked for unique `event_id`s and that no meter/end precedes its
start.

**Topic design**: `COMP5339/TUT19_G07/ev/<charger_id>/<event_type>`, so a client can subscribe to
everything (`.../ev/+/+`), one event type (`.../ev/+/meter`) or one charger (`.../ev/42/#`).
A retained `COMP5339/TUT19_G07/status/publisher` message with a last-will shows whether the
publisher is online.

**Message format** (JSON envelope, `v` = schema version):

```json
{"v": 1, "seq": 23766, "event_id": "m-24-2-202610010719-202610010801", "event_type": "meter",
 "event_ts": "2026-10-01T08:01:00+10:00", "charger_id": 24, "connector_id": 2,
 "session_id": "24-2-202610010719",
 "data": {"voltage_v": 722.9, "current_a": 68.1, "power_kw": 49.22, "energy_kwh": 34.177, "soc_pct": 70.5}}
```

See `data/stream/events_sample.jsonl` for examples of all four types.

**Publisher** (`publisher.py`): QoS 1; at least 0.1 s between messages (monotonic-clock
schedule); waits for the connection and auto-reconnects; at most 200 un-acknowledged messages;
the checkpoint only advances to the highest seq whose PUBACK was received, so `--resume` never
skips a message. When replaying from `--from`, it first re-sends the latest price of every
charger and the start of every session still active at that time (bootstrap), so the dashboard
is consistent immediately. `--backfill` disables the delay for pre-loading history (local broker
only, logged in `publish_runs.jsonl`); without it, a delay below 0.1 s is rejected.

## Tasks 3 and 4 - subscription, storage and integration

`ingest.py` subscribes with QoS 1 and a persistent session (`clean_session=False`), decodes and
validates each message (envelope keys, version, event type, topic matches payload, `charger_id`
exists in A1), and queues it. A writer thread stores batches (every 0.5 s or 500 events) in one
DuckDB transaction. Invalid messages go to `data/stream/rejected.jsonl`. Re-delivered messages are
dropped by `event_id`.

Schema extension (`sql/02_schema_a2_events.sql`), all linked to A1 `charger(charger_id)`:

| Table | Content | Key design choices |
|---|---|---|
| `event_log` | one row per distinct MQTT event | PK `seq` (idempotent ingest; 1:1 with `event_id`), arrival time |
| `price_event` | tariff changes | PK (`charger_id`, `event_ts`), FK to A1 charger, wholesale price kept |
| `charging_session` | one row per session | FK to A1 charger; inserted at start, completed at end; `end_ts IS NULL` = active |
| `meter_reading` | session telemetry | FK to session (charger via session), REAL columns |
| `charger_live_state` | current price, active sessions, last event per charger | FK to A1 charger; maintained in the same transaction |

Views: `v_charger_live` (A1 attributes + live state), `v_active_sessions` (current state),
`v_charger_usage_daily` (sessions, kWh, revenue, utilisation per charger and day),
`v_network_hourly`.

Physical design (measured on the 2.27 M-event database):

* Events are inserted in event-time order, so DuckDB's row-group min/max zonemaps prune
  time-range scans without an index.
* DuckDB creates an ART index for every primary and foreign key. The foreign-key indexes already
  serve the pop-up's point look-ups (meter readings by session, sessions and prices by charger:
  0.2 ms instead of 1.5 ms), so extra `CREATE INDEX` statements were removed (each added ~45 MB).
* `event_log` is keyed by the BIGINT `seq` instead of the VARCHAR `event_id`: index 19 MB instead
  of 128 MB. `meter_reading` has no composite key (+80 MB); its uniqueness is guaranteed by the
  `event_log` check in the same transaction.
* `charger_live_state` lets the map refresh read 1,926 rows instead of aggregating the history;
  `v_active_sessions` filters meter readings to active sessions before aggregating.
* Result: 699 MB for the first design -> 192 MB after redesign and compaction, same data and checks.
If a meter arrives before its `session_start` (e.g. subscriber joined late), a placeholder session
is created (`start_inferred = true`) and completed by later events.

## Task 3 - dashboard

`python -m evstream.dashboard.app` (Dash + dash-leaflet) refreshes every 2.5 s:

* **Map**: a marker appears for a charger when its first event arrives. Each marker shows the
  **live price** and is coloured by **occupancy** (green free, amber partly busy, red full);
  `⚡n/m` = active sessions / connectors. Clusters show the number of chargers and of busy chargers.
  Filters: operator, charger type, show chargers without events.
* **Pop-up on click**: operator network, address, charger type and power, plug types, area,
  status, latest price; *current state*: connectors in use and power delivered now;
  *historical*: sessions, energy and utilisation over the last 7 simulated days; a **live-updated
  charging curve** (energy, power, SoC) of the active session(s) from the stored meter events, and
  the charger's price history.
* **KPIs**: simulated time, active sessions, chargers in use, MW delivered now, average live price
  (current state); sessions completed, MWh delivered, revenue (historical).
* Side panel: hourly sessions and tariff (stored history), top chargers by energy, live event feed.

## Outputs and included data

| Path | Description |
|---|---|
| `data/generated/stations_input.csv` (+ `_dc`, `_ac`) | generator input built from A1 (with power source and group) |
| `data/generated/stations.csv` | stations with the generator's base price |
| `data/generated/prices_hourly.csv` | hourly dynamic price per charger with AEMO wholesale price |
| `data/generated/sessions.csv` | one row per charging session |
| `data/generated/session_metering.csv` | meter readings of all sessions |
| `data/generated/aemo_cache/` | downloaded AEMO NSW1 5-minute price file |
| `data/stream/events.parquet` | the complete ordered event stream incl. each JSON message |
| `data/stream/events_sample.jsonl` | 500 consecutive messages, readable |
| `data/stream/publisher_checkpoint.json` | last acknowledged seq of the last publishing run |
| `data/stream/publish_runs.jsonl` | one line per publishing run (backfill or live, delay, seq range) |
| `mosquitto/mosquitto.conf` | local broker configuration (no message drops) |
| `data/db/ev_chargers.duckdb` | A1 database extended with the stored MQTT events |

## Assumptions

* Prices follow the generator's hypothetical dynamic tariff (NRMA itself uses flat prices).
* Timestamps are AEMO market time (AEST, UTC+10, no daylight saving), as in the AEMO data.
* The 98 *Upcoming* chargers (no AC/DC type, 350 kW) are simulated as commissioned DC sites so that
  every A1 charger has data.
* 518 AC chargers without a power rating use the median A1 AC rating (22 kW).
* AC utilisation (1.0/0.6 sessions per connector per day) is our assumption; the generator's
  vehicle model is designed for DC charging, so AC sessions are limited only by the charger rating.
* Session occupancy (for "in use") lasts until unplug (`end_ts`), not only until charging stops.
* Publishing the complete stream (2.43 M messages) at 0.1 s takes about 67 hours. The history
  (1-14 Oct 07:00) was therefore backfilled through MQTT without the delay (see "How the submitted
  database was filled"); live streaming always uses the ≥ 0.1 s delay. The publisher can also
  publish the full stream at 0.1 s, resuming across several runs with `--resume`.

## Known limitations

* DuckDB allows one writing process, so the subscriber is embedded in the dashboard process.
* The public broker is shared and unauthenticated; use a local Mosquitto broker for long runs.
* Chargers sharing a site have identical coordinates; their markers are spread out ("spiderfied")
  when the cluster is clicked at maximum zoom.
