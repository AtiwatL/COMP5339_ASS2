"""Task 2b - publish the ordered event stream to the MQTT broker.

* Reads data/stream/events.parquet in event-time (seq) order, in chunks, so memory stays flat.
* One message per event on topic <root>/<charger_id>/<event_type> with QoS 1 (at-least-once).
* At least --delay seconds (minimum 0.1 s, enforced) between consecutive publishes, using a
  monotonic-clock schedule so the delay never drifts below the minimum.
* Reliability: waits for the broker connection before publishing, auto-reconnects, bounds the
  number of un-acknowledged messages, and only advances the checkpoint up to the highest seq whose
  PUBACK has been received. --resume continues after the checkpoint without gaps.
* A retained status message (<root-parent>/status/publisher) with a last-will tells subscribers
  whether the publisher is online.

Replaying a window (--from/--until): the full stream has 171,987 events,
so a demo usually replays a window. Before the window, the publisher bootstraps state by sending
the latest price_update of every charger and the session_start of every session already active at
the window start (these are the original events, unchanged), so the dashboard is consistent at once.

Backfill (--backfill): loads *history* quickly through the same MQTT pipeline (same topics,
messages, QoS 1 and subscriber) but without the 0.1 s delay. It is an explicit, logged mode for
pre-loading the database before live streaming, restricted to a local broker. All live streaming
(including the demo) uses the normal mode with the enforced >= 0.1 s delay. Every run, with its
mode, delay and seq range, is appended to data/stream/publish_runs.jsonl.

Run:  python -m evstream.publisher                              # full stream from the beginning
      python -m evstream.publisher --from "2026-10-01 07:30"    # replay from 07:30 on day 1
      python -m evstream.publisher --resume                     # continue after the checkpoint
      python -m evstream.publisher --backfill --until "2026-10-01 07:00"
"""
import argparse
import json
import threading
import time

import duckdb
import paho.mqtt.client as mqtt

from evstream import config

STATUS_TOPIC = config.TOPIC_ROOT.rsplit("/", 1)[0] + "/status/publisher"
MAX_INFLIGHT = 200


class Publisher:
    def __init__(self, host, port, delay):
        self.delay = delay                # validated in main(): >= 0.1 s, or 0 only for --backfill
        self.pending = {}                 # mid -> seq of messages awaiting PUBACK
        self.early_acks = set()           # mids acknowledged before publish() registered them
        self.lock = threading.Lock()
        self.connected = threading.Event()
        self.published = 0
        self.closing = False
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"comp5339-g07-pub-{int(time.time())}")
        self.client.max_inflight_messages_set(MAX_INFLIGHT)
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)
        self.client.will_set(STATUS_TOPIC, json.dumps({"status": "offline"}), qos=1, retain=True)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.connect_async(host, port, keepalive=60)
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if not reason_code.is_failure:
            self.connected.set()
            client.publish(STATUS_TOPIC, json.dumps({"status": "online"}), qos=1, retain=True)
        print(f"[publisher] connected: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected.clear()
        if not self.closing:
            print(f"[publisher] disconnected: {reason_code} - reconnecting")

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        # Called by paho while it holds its own lock, so publish() must never hold self.lock
        # while calling into paho (that would deadlock). A PUBACK can therefore arrive before the
        # message is registered in self.pending; it is remembered in early_acks.
        with self.lock:
            if self.pending.pop(mid, None) is None:
                self.early_acks.add(mid)

    def publish(self, seq, charger_id, event_type, message):
        self.connected.wait()
        while len(self.pending) >= MAX_INFLIGHT:          # back-pressure if the broker is slow
            time.sleep(0.01)
        info = self.client.publish(config.topic_for(charger_id, event_type), message, qos=config.QOS)
        with self.lock:
            if info.mid in self.early_acks:
                self.early_acks.discard(info.mid)
            else:
                self.pending[info.mid] = seq
        self.published += 1

    def safe_checkpoint(self, last_sent_seq):
        """Highest seq such that every message up to it has been acknowledged."""
        with self.lock:
            return min(self.pending.values()) - 1 if self.pending else last_sent_seq

    def drain(self, timeout=60):
        end = time.monotonic() + timeout
        while self.pending and time.monotonic() < end:
            time.sleep(0.1)
        return not self.pending

    def close(self):
        self.closing = True
        self.client.publish(STATUS_TOPIC, json.dumps({"status": "offline"}), qos=1, retain=True).wait_for_publish(5)
        self.client.loop_stop()
        self.client.disconnect()


def load_checkpoint():
    if config.PUBLISH_CHECKPOINT.exists():
        return json.loads(config.PUBLISH_CHECKPOINT.read_text())
    return None


def save_checkpoint(seq, args):
    config.PUBLISH_CHECKPOINT.write_text(json.dumps(
        {"last_acked_seq": seq, "from": args.from_ts, "until": args.until,
         "mode": "backfill" if args.backfill else "live", "saved_at": time.ctime()}, indent=2))


def log_run(args, pub, first_seq, last_acked, started, all_acked):
    """Append one line per publishing run, so it is always documented how the database was filled."""
    entry = {"mode": "backfill" if args.backfill else "live", "host": config.MQTT_HOST,
             "delay_s": pub.delay,
             "from": args.from_ts, "until": args.until, "resumed": args.resume,
             "first_seq": first_seq, "last_acked_seq": last_acked, "messages": pub.published,
             "all_acknowledged": all_acked, "started": started, "finished": time.strftime("%Y-%m-%d %H:%M:%S")}
    with open(config.PUBLISH_RUN_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def build_query(args, start_after_seq):
    """SQL returning (seq, charger_id, event_type, message) in publishing order."""
    src = f"read_parquet('{(config.STREAM_DIR / 'events.parquet').as_posix()}')"
    window = [f"seq > {int(start_after_seq)}"]
    if args.from_ts:
        window.append(f"event_ts >= TIMESTAMP '{args.from_ts}'")
    if args.until:
        window.append(f"event_ts < TIMESTAMP '{args.until}'")
    main = f"SELECT seq, charger_id, event_type, message FROM {src} WHERE {' AND '.join(window)}"
    if not args.from_ts or args.no_bootstrap:
        return main + " ORDER BY seq"
    # Bootstrap: latest price per charger and sessions active at the window start.
    bootstrap = f"""
        --Bootstrap prices
        SELECT seq, charger_id, event_type, message FROM (
            SELECT *, row_number() OVER (PARTITION BY charger_id ORDER BY seq DESC) AS rn
            FROM {src} WHERE event_type = 'price_update' AND event_ts < TIMESTAMP '{args.from_ts}'
        ) WHERE rn = 1
        UNION ALL
        --Bootstrap active sessions
        SELECT s.seq, s.charger_id, s.event_type, s.message
        FROM {src} s JOIN {src} e ON e.session_id = s.session_id AND e.event_type = 'session_end'
        WHERE s.event_type = 'session_start' AND s.event_ts < TIMESTAMP '{args.from_ts}'
          AND e.event_ts >= TIMESTAMP '{args.from_ts}'"""
    # On --resume, bootstrap events already acknowledged (seq <= checkpoint) are skipped as well.
    return (f"SELECT * FROM ({bootstrap}) WHERE seq > {int(start_after_seq)} "
            f"UNION ALL SELECT * FROM ({main}) ORDER BY seq")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--delay", type=float, default=config.MIN_PUBLISH_DELAY_S,
                    help="seconds between messages (minimum 0.1)")
    ap.add_argument("--from", dest="from_ts", help="first event time to replay, e.g. '2026-10-01 07:30'")
    ap.add_argument("--until", help="stop before this event time")
    ap.add_argument("--limit", type=int, help="stop after this many messages")
    ap.add_argument("--resume", action="store_true", help="continue after the saved checkpoint")
    ap.add_argument("--no-bootstrap", action="store_true", help="do not send state before --from")
    ap.add_argument("--backfill", action="store_true",
                    help="pre-load history without the delay (local broker only; logged as a backfill run)")
    args = ap.parse_args()
    config.stop_on_sigterm()
    if args.backfill:
        args.delay = 0.0
        print("*** BACKFILL MODE: loading history without the 0.1 s delay (recorded in publish_runs.jsonl) ***")
    elif args.delay < config.MIN_PUBLISH_DELAY_S:
        ap.error(f"--delay must be at least {config.MIN_PUBLISH_DELAY_S} s")

    start_after = 0
    if args.resume:
        cp = load_checkpoint()
        if cp:
            start_after = cp["last_acked_seq"]
            # --until is deliberately not inherited: resuming after a backfill that stopped at
            # --until continues with the live stream from that point.
            args.from_ts = args.from_ts or cp.get("from")
            print(f"resuming after seq {start_after:,}")

    cur = duckdb.connect().execute(build_query(args, start_after))
    pub = Publisher(config.MQTT_HOST, config.MQTT_PORT, args.delay)
    print(f"[publisher] {config.MQTT_HOST}:{config.MQTT_PORT}  "
          f"topic {config.TOPIC_ROOT}/<charger_id>/<event_type>  "
          f"delay {pub.delay}s")
    last_seq, t_next, t0 = start_after, time.monotonic(), time.monotonic()
    first_seq, started = None, time.strftime("%Y-%m-%d %H:%M:%S")
    checkpoint_every, report_every = (5000, 50000) if args.backfill else (100, 500)
    try:
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            for seq, charger_id, event_type, message in rows:
                now = time.monotonic()
                if now < t_next:
                    time.sleep(t_next - now)
                pub.publish(seq, charger_id, event_type, message)
                first_seq = first_seq or seq
                t_next = max(t_next + pub.delay, time.monotonic())   # never closer than delay
                last_seq = seq
                if pub.published % checkpoint_every == 0:
                    save_checkpoint(pub.safe_checkpoint(last_seq), args)
                if pub.published % report_every == 0:
                    rate = pub.published / (time.monotonic() - t0)
                    event_ts = json.loads(message)["event_ts"]
                    print(f"  {pub.published:,} sent  seq {seq:,}  event time {event_ts}  {rate:.1f} msg/s")
                if args.limit and pub.published >= args.limit:
                    raise StopIteration
    except (KeyboardInterrupt, StopIteration):
        print("stopping ...")
    finally:
        ok = pub.drain()
        save_checkpoint(pub.safe_checkpoint(last_seq), args)
        log_run(args, pub, first_seq, pub.safe_checkpoint(last_seq), started, ok)
        pub.close()
        print(f"[publisher] {pub.published:,} messages published; all acknowledged: {ok}; "
              f"checkpoint {pub.safe_checkpoint(last_seq):,}")


if __name__ == "__main__":
    main()
