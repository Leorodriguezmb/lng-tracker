"""
LNG Fleet Tracker — AISStream.io collector
==========================================
Connects to the free AISStream.io websocket, listens for position
reports and static data for the vessels listed in fleet.csv, merges
them with previously known positions, and writes:
 
  data/positions.json         (full state, machine-readable)
  data/latest_positions.csv   (imported by Google Sheets via IMPORTDATA)
 
Designed to run on a schedule via GitHub Actions.
 
Environment variables:
  AISSTREAM_API_KEY   (required) free key from aisstream.io
  LISTEN_SECONDS      (optional) how long to listen per run, default 600
"""
 
import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
 
import websockets
 
# ----------------------------- CONFIG ------------------------------
 
FLEET_FILE = Path("fleet.csv")
STATE_FILE = Path("data/positions.json")
CSV_FILE = Path("data/latest_positions.csv")
 
WS_URL = "wss://stream.aisstream.io/v0/stream"
LISTEN_SECONDS = int(os.environ.get("LISTEN_SECONDS", "600"))
MAX_MMSI_PER_SUBSCRIPTION = 50  # AISStream hard limit
 
NAV_STATUS = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted manoeuvrability",
    4: "Constrained by draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in fishing",
    8: "Under way sailing",
    15: "Undefined",
}
 
CSV_COLUMNS = [
    "mmsi", "vessel_name", "latitude", "longitude", "speed_kn",
    "course_deg", "heading_deg", "nav_status", "destination",
    "eta", "last_position_utc", "last_updated_by_run_utc",
]
 
# --------------------------- FLEET INPUT ---------------------------
 
 
def load_fleet():
    """Read fleet.csv -> list of (mmsi, label). MMSI must be 9 digits."""
    if not FLEET_FILE.exists():
        sys.exit("ERROR: fleet.csv not found. Create it with a 'mmsi' column.")
    vessels = []
    with FLEET_FILE.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "mmsi" not in [c.strip().lower() for c in reader.fieldnames]:
            sys.exit("ERROR: fleet.csv must have a header row containing an 'mmsi' column.")
        # normalise header names
        for row in reader:
            row = {k.strip().lower(): (v or "").strip() for k, v in row.items()}
            mmsi = "".join(ch for ch in row.get("mmsi", "") if ch.isdigit())
            if not mmsi:
                continue
            if len(mmsi) != 9:
                print(f"WARNING: skipping '{row.get('mmsi')}' — MMSI must be exactly 9 digits.")
                continue
            vessels.append((mmsi, row.get("name", "")))
    if not vessels:
        sys.exit("ERROR: no valid MMSI numbers found in fleet.csv.")
    return vessels
 
 
# --------------------------- STATE STORE ---------------------------
 
 
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print("WARNING: positions.json corrupted — starting fresh.")
    return {}
 
 
def save_outputs(state, fleet):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
 
    with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for mmsi, label in fleet:
            v = state.get(mmsi, {})
            writer.writerow({
                "mmsi": mmsi,
                "vessel_name": v.get("vessel_name") or label,
                "latitude": v.get("latitude", ""),
                "longitude": v.get("longitude", ""),
                "speed_kn": v.get("speed_kn", ""),
                "course_deg": v.get("course_deg", ""),
                "heading_deg": v.get("heading_deg", ""),
                "nav_status": v.get("nav_status", ""),
                "destination": v.get("destination", ""),
                "eta": v.get("eta", ""),
                "last_position_utc": v.get("last_position_utc", ""),
                "last_updated_by_run_utc": v.get("last_updated_by_run_utc", ""),
            })
 
 
# ------------------------- MESSAGE HANDLING ------------------------
 
 
def handle_message(msg, state, wanted, run_ts):
    """Update state dict from one AISStream JSON message. Returns MMSI if updated."""
    meta = msg.get("MetaData", {})
    mmsi = str(meta.get("MMSI", "")).strip()
    if mmsi not in wanted:
        return None
 
    entry = state.setdefault(mmsi, {})
    mtype = msg.get("MessageType", "")
 
    ship_name = str(meta.get("ShipName", "")).strip()
    if ship_name:
        entry["vessel_name"] = ship_name
 
    if mtype == "PositionReport":
        body = msg.get("Message", {}).get("PositionReport", {})
        entry["latitude"] = body.get("Latitude", meta.get("latitude"))
        entry["longitude"] = body.get("Longitude", meta.get("longitude"))
        if body.get("Sog") is not None:
            entry["speed_kn"] = body["Sog"]
        if body.get("Cog") is not None:
            entry["course_deg"] = body["Cog"]
        heading = body.get("TrueHeading")
        if heading is not None and heading != 511:  # 511 = not available
            entry["heading_deg"] = heading
        ns = body.get("NavigationalStatus")
        if ns is not None:
            entry["nav_status"] = NAV_STATUS.get(ns, f"Code {ns}")
        entry["last_position_utc"] = _clean_time(meta.get("time_utc", ""))
        entry["last_updated_by_run_utc"] = run_ts
 
    elif mtype == "ShipStaticData":
        body = msg.get("Message", {}).get("ShipStaticData", {})
        dest = str(body.get("Destination", "")).strip()
        if dest:
            entry["destination"] = dest
        eta = body.get("Eta")
        if isinstance(eta, dict) and eta.get("Month"):
            entry["eta"] = "{:02d}-{:02d} {:02d}:{:02d} UTC".format(
                int(eta.get("Month", 0)), int(eta.get("Day", 0)),
                int(eta.get("Hour", 0)), int(eta.get("Minute", 0)))
        entry["last_updated_by_run_utc"] = run_ts
 
    return mmsi
 
 
def _clean_time(raw):
    """AISStream time_utc looks like '2024-05-20 09:21:31.781972101 +0000 UTC'."""
    if not raw:
        return ""
    return raw.split(".")[0]
 
 
# ----------------------------- LISTENER ----------------------------
 
 
async def listen_batch(api_key, mmsi_batch, state, run_ts):
    subscription = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],  # whole world
        "FiltersShipMMSI": mmsi_batch,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    wanted = set(mmsi_batch)
    updated = set()
    seconds_per_batch = LISTEN_SECONDS
    deadline = asyncio.get_event_loop().time() + seconds_per_batch
 
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps(subscription))
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if "error" in {k.lower() for k in msg.keys()}:
                sys.exit(f"ERROR from AISStream: {msg}")
            hit = handle_message(msg, state, wanted, run_ts)
            if hit:
                updated.add(hit)
    return updated
 
 
async def main():
    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not api_key:
        sys.exit("ERROR: AISSTREAM_API_KEY environment variable not set.")
 
    fleet = load_fleet()
    state = load_state()
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
 
    mmsis = [m for m, _ in fleet]
    batches = [mmsis[i:i + MAX_MMSI_PER_SUBSCRIPTION]
               for i in range(0, len(mmsis), MAX_MMSI_PER_SUBSCRIPTION)]
 
    all_updated = set()
    for n, batch in enumerate(batches, 1):
        print(f"Listening for batch {n}/{len(batches)} "
              f"({len(batch)} vessels, {LISTEN_SECONDS}s)...")
        try:
            updated = await listen_batch(api_key, batch, state, run_ts)
            all_updated |= updated
        except (websockets.WebSocketException, OSError) as e:
            print(f"WARNING: websocket problem in batch {n}: {e} — "
                  f"keeping previously known positions.")
 
    save_outputs(state, fleet)
    stale = len(fleet) - len(all_updated)
    print(f"Done. {len(all_updated)} vessel(s) updated this run, "
          f"{stale} kept last-known position (out of AIS receiver range "
          f"or not transmitting during the listen window).")
 
 
if __name__ == "__main__":
    asyncio.run(main())
