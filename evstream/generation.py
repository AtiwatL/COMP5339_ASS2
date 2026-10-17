"""Task 1b - run the provided generator for every Assignment 1 charger and merge the outputs.

The provided generator (generator/generate_dataset.py, unmodified) is calibrated on ARENA data
for DC fast chargers: about 4 sessions per connector per day at urban sites and 3 at regional sites.
Public AC chargers are destination chargers with much lower turnover, so the chargers are
generated in two groups with group-specific parameters:

    group  sessions/connector/day (urban, regional)  meter interval
    dc     4.0, 3.0  (generator default, ARENA)       2 min
    ac     1.0, 0.6  (assumption, documented)         10 min

Both groups use the same 14-day window and the same AEMO NSW1 wholesale prices, so the dynamic
tariff is consistent across the network. The outputs are concatenated into data/generated/:
stations.csv, prices_hourly.csv, sessions.csv and session_metering.csv, each with a sim_group column.

Run:  python -m evstream.generation
"""
import shutil
import subprocess
import sys

import pandas as pd

from evstream import config

GROUPS = {
    "dc": {"seed": 5339, "per_day": ("4.0", "3.0"), "meter_every": "2"},
    "ac": {"seed": 5340, "per_day": ("1.0", "0.6"), "meter_every": "10"},
}
OUTPUT_FILES = ("stations.csv", "prices_hourly.csv", "sessions.csv", "session_metering.csv")


def run_group(group, params):
    out_dir = config.GENERATED_DIR / f"raw_{group}"
    cache = config.GENERATED_DIR / "aemo_cache"
    # Share one AEMO download between the two runs (the generator caches under <out>/aemo_cache).
    if cache.exists():
        shutil.copytree(cache, out_dir / "aemo_cache", dirs_exist_ok=True)
    cmd = [sys.executable, str(config.GENERATOR),
           "--stations", str(config.GENERATED_DIR / f"stations_input_{group}.csv"),
           "--start", config.SIM_START, "--days", str(config.SIM_DAYS),
           "--out", str(out_dir), "--seed", str(params["seed"]),
           "--sessions-per-connector-day", *params["per_day"],
           "--meter-every", params["meter_every"]]
    print(f"[{group}] {' '.join(cmd[1:])}")
    subprocess.run(cmd, check=True)
    shutil.copytree(out_dir / "aemo_cache", cache, dirs_exist_ok=True)
    return out_dir


def merge_groups(out_dirs):
    for name in OUTPUT_FILES:
        parts = []
        for group, out_dir in out_dirs.items():
            df = pd.read_csv(out_dir / name)
            df["sim_group"] = group
            parts.append(df)
        merged = pd.concat(parts, ignore_index=True)
        merged.to_csv(config.GENERATED_DIR / name, index=False)
        print(f"  {name:22s} {len(merged):>10,d} rows")


def check_wholesale_coverage():
    """Every simulated hour should have a real AEMO price (the generator silently falls back to the mean)."""
    cache_file = next((config.GENERATED_DIR / "aemo_cache").glob("*.csv"))
    aemo = pd.read_csv(cache_file, parse_dates=["SETTLEMENTDATE"])
    hours = (aemo["SETTLEMENTDATE"] - pd.Timedelta(minutes=5)).dt.floor("h").unique()
    wanted = pd.date_range(config.SIM_START, periods=config.SIM_DAYS * 24, freq="h")
    missing = wanted.difference(hours)
    print(f"  AEMO coverage: {len(wanted) - len(missing)}/{len(wanted)} simulated hours have real NSW1 prices")
    if len(missing):
        print(f"  WARNING: {len(missing)} hours fall back to the period mean, first {missing[0]}")


def main():
    out_dirs = {group: run_group(group, params) for group, params in GROUPS.items()}
    print("Merged outputs:")
    merge_groups(out_dirs)
    check_wholesale_coverage()
    for out_dir in out_dirs.values():      # per-group copies are intermediate only
        shutil.rmtree(out_dir)


if __name__ == "__main__":
    main()
