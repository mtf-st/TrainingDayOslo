# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f6e75393-5b7d-453f-ba29-f4b3dc2df25c",
# META       "default_lakehouse_name": "Streaming",
# META       "default_lakehouse_workspace_id": "31d9702e-629f-44e9-b1cf-b4dc9fa73122",
# META       "known_lakehouses": [
# META         {
# META           "id": "f6e75393-5b7d-453f-ba29-f4b3dc2df25c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime, timezone
import time, uuid, random
from pyspark.sql import Row

# ============================
# Configuration
# ============================
DEST_PATH = "Files/windmill_events"
RUN_FOR_SECONDS = 5 * 60        # 5 minutes
INTERVAL_SECONDS = 1            # one file per second

# Fixed windfarms (stable IDs + locations)
WINDFARMS = [
    ("WF-01", "Nordic Ridge",   59.9139, 10.7522),
    ("WF-02", "Coastal Gale",   60.3920, 5.3242),
    ("WF-03", "Highland Sweep", 63.4305, 10.3951),
    ("WF-04", "Arctic Draft",   69.6492, 18.9553),
    ("WF-05", "Fjord Current",  58.9690, 5.7331),
]

# Power curve parameters (MW)
RATED_POWER = 15.0
CUT_IN, RATED, CUT_OUT = 3.0, 12.0, 25.0

# ============================
# Helper functions
# ============================
def utc_now():
    return datetime.now(timezone.utc)

def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

def power_curve(v):
    if v < CUT_IN or v >= CUT_OUT:
        return 0.0
    if v >= RATED:
        return RATED_POWER
    x = (v - CUT_IN) / (RATED - CUT_IN)
    return RATED_POWER * (x ** 3)

# Smooth wind evolution per farm
wind_state = {wf[0]: random.uniform(5.0, 11.0) for wf in WINDFARMS}

def next_wind(prev):
    drift = (8.5 - prev) * 0.05
    noise = random.gauss(0.0, 0.3)
    gust = random.uniform(-1.2, 2.0) if random.random() < 0.05 else 0.0
    return max(0.0, min(22.0, prev + drift + noise + gust))

# ============================
# Main loop
# ============================
start = time.time()
counter = 0

while time.time() - start < RUN_FOR_SECONDS:
    counter += 1
    ts = utc_now()

    wf_id, name, lat, lon = random.choice(WINDFARMS)

    # Update wind
    wind_state[wf_id] = next_wind(wind_state[wf_id])
    wind_speed = round(wind_state[wf_id], 2)

    # Compute power
    base_power = power_curve(wind_speed)
    power_mw = round(
        max(0.0, min(RATED_POWER, base_power + random.uniform(-0.3, 0.3))),
        2
    )

    record = Row(
        event_id=str(uuid.uuid4()),
        timestamp=iso(ts),
        windfarm_id=wf_id,
        windfarm_name=name,
        latitude=lat,
        longitude=lon,
        wind_speed_mps=wind_speed,
        power_mw=power_mw
    )

    df = spark.createDataFrame([record])

    # Write ONE JSON record to ONE file
    json_line = df.toJSON().collect()[0] + "\n"
    fname = f"windfarm_{ts.strftime('%Y%m%d_%H%M%S')}_{counter:04d}.json"
    full_path = f"{DEST_PATH}/{fname}"

    mssparkutils.fs.put(full_path, json_line, True)

    if counter % 10 == 0:
        print(f"[{counter}] wrote {full_path}")
        df.show(truncate=False)

    time.sleep(INTERVAL_SECONDS)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
