# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14877ca1-3b6b-4f11-b3ff-e1dc9b592370",
# META       "default_lakehouse_name": "GOLD",
# META       "default_lakehouse_workspace_id": "31d9702e-629f-44e9-b1cf-b4dc9fa73122",
# META       "known_lakehouses": [
# META         {
# META           "id": "14877ca1-3b6b-4f11-b3ff-e1dc9b592370"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🚀 Delta Performance Optimizations Demo (Fabric Lakehouse)
# 
# ## Goal
# We will create a large Delta table (~20,000,000 rows) and demonstrate performance improvements using:
# 
# - Partitioning (before/after)
# - V-Order (before/after)
# - Z-Order (before/after)
# - OPTIMIZE (before/after)
# - VACUUM (before/after) by creating a “bad table” with stale files
# - Deletion Vectors (before/after) for DELETE/UPDATE workloads
# 
# ## How we’ll measure improvements
# For each technique we’ll compare:
# - **Query runtime** (same query patterns every time)
# - **Storage stats** (numFiles, sizeInBytes, partition columns) via `DESCRIBE DETAIL`
# 
# 


# CELL ********************

import time
from math import ceil
from pyspark.sql import functions as F

def safe_sql(label: str, sql_text: str):
    """Execute SQL with try/except for demo robustness."""
    try:
        spark.sql(sql_text)
        print(f"✅ {label}")
    except Exception as e:
        print(f"❌ {label} failed")
        print(f"Error: {e}")

def describe_detail(table_name: str):
    """Show key storage stats for a Delta table."""
    try:
        df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        display(df.select("format", "numFiles", "sizeInBytes", "partitionColumns", "createdAt", "lastModified"))
    except Exception as e:
        print(f"❌ DESCRIBE DETAIL failed for {table_name}")
        print(f"Error: {e}")

def time_sql(label: str, sql_text: str, warmup: int = 1, runs: int = 3):
    """Run a SQL query multiple times and print timings (count triggers execution)."""
    try:
        # warmup
        for _ in range(warmup):
            spark.sql(sql_text).count()

        times = []
        for _ in range(runs):
            t0 = time.perf_counter()
            spark.sql(sql_text).count()
            t1 = time.perf_counter()
            times.append(t1 - t0)

        print(f"✅ {label} | runs={runs} | secs={times} | best={min(times):.3f}s | avg={sum(times)/len(times):.3f}s")
    except Exception as e:
        print(f"❌ Timing failed: {label}")
        print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1) Create a big baseline table (~20,000,000 rows)
# 
# We’ll use an existing Delta table (recommended: `dw.FactSales`) as the seed,
# replicate it with a `range()` cross join, and write a **plain baseline** table:
# 
# - Not partitioned
# - No OPTIMIZE yet
# - V-Order not explicitly applied
# 
# This gives us a clean “before” for comparisons.


# CELL ********************

from pyspark.sql import functions as F, Window
from math import ceil

try:
    safe_sql("Create perf schema", "CREATE DATABASE IF NOT EXISTS perf")

    if not spark.catalog.tableExists("dw.FactSales"):
        raise ValueError("dw.FactSales not found. Build your star schema first.")

    target = 20_000_000

    # ----------------------------
    # Demo-tuning knobs (adjustable)
    # ----------------------------
    DATE_SPREAD_DAYS = 730          # spread dates across ~2 years (helps partition pruning)
    COMMON_PRODUCTS = 200           # most rows land in ~200 products (creates clustering opportunities)
    RARE_EVERY_N = 2000             # 1 out of N rows gets a rare product (~0.05%)
    RARE_PRODUCT_BUCKET = 999999    # bucket id for rare product

    # ----------------------------
    # Load and stabilize the seed data
    # ----------------------------
    base = spark.read.table("dw.FactSales").cache()
    base_count = base.count()
    if base_count <= 0:
        raise ValueError("dw.FactSales is empty. Cannot scale up.")

    print(f"Base rows: {base_count:,} | Target rows: {target:,}")

    # Add a stable row number to base (1..base_count)
    w = Window.orderBy(F.monotonically_increasing_id())
    base_rn = base.withColumn("__rn", F.row_number().over(w)).cache()

    # Generate exactly target ids (0..target-1)
    ids = spark.range(target).withColumnRenamed("id", "__id")

    # Map each generated id to a base row using modulo (repeat base rows evenly)
    mapped = ids.withColumn("__rn", (F.pmod(F.col("__id"), F.lit(base_count)) + F.lit(1)).cast("int"))

    # Join to get the base row, repeated to exactly target rows
    big = mapped.join(F.broadcast(base_rn), "__rn", "inner").drop("__rn")

    # ----------------------------
    # Make the data "benchmark-friendly"
    # ----------------------------
    # 1) Spread OrderDateKey across many days
    #    This makes partition pruning very visible on partitioned tables.
    #
    # If your OrderDateKey is YYYYMMDD integer, adding "days" directly is not a real date add.
    # For benchmarking it is fine (it increases distinct values and supports BETWEEN predicates).
    big = big.withColumn(
        "OrderDateKey",
        (F.col("OrderDateKey").cast("int") + F.pmod(F.col("__id"), F.lit(DATE_SPREAD_DAYS)).cast("int"))
    )

    # 2) Create skewed ProductKey distribution:
    #    - Most rows go to COMMON_PRODUCTS buckets (hot values)
    #    - Very rarely, assign a special "rare" bucket
    #    This makes Z-Order skipping pop when you filter on that rare ProductKey.
    product_bucket = F.when(
        (F.pmod(F.col("__id"), F.lit(RARE_EVERY_N)) == 0),
        F.lit(RARE_PRODUCT_BUCKET)
    ).otherwise(
        F.pmod(F.col("__id"), F.lit(COMMON_PRODUCTS))
    )

    # Overwrite ProductKey with a deterministic hash that incorporates the bucket
    big = big.withColumn(
        "ProductKey",
        F.sha2(F.concat_ws("||", F.col("ProductKey").cast("string"), product_bucket.cast("string")), 256)
    )

    # Optional: keep a debug column to help you explain skew during the demo
    big = big.withColumn("ProductBucket", product_bucket.cast("int"))

    # Useful row instance for traceability
    big = big.withColumn("RowInstance", F.col("__id")).drop("__id")

    # ----------------------------
    # Write baseline table
    # ----------------------------
    safe_sql("Drop baseline if exists", "DROP TABLE IF EXISTS perf.FactSales_20M_base")
    big.write.mode("overwrite").format("delta").saveAsTable("perf.FactSales_20M_base")

    print("✅ Created perf.FactSales_20M_base (exactly 20M rows, benchmark-friendly distribution)")
    describe_detail("perf.FactSales_20M_base")

    # Quick sanity checks (optional)
    display(
        spark.sql("""
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT OrderDateKey) AS distinct_dates,
              COUNT(DISTINCT ProductKey) AS distinct_products,
              SUM(CASE WHEN ProductBucket = 999999 THEN 1 ELSE 0 END) AS rare_rows
            FROM perf.FactSales_20M_base
        """)
    )

except Exception as e:
    print("❌ Failed to create baseline table")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2) Define benchmark queries (repeatable)
# 
# We’ll benchmark three common patterns:
# 
# 1) **Selective date filter** → great for partition pruning & clustering  
# 2) **Selective ProductKey filter** → great for Z-Order  
# 3) **Group by** → scan + aggregate pattern
# 
# We’ll pick actual values from the table so the filter returns results reliably.


# CELL ********************

try:
    # Pick a date key that exists
    date_row = spark.sql("""
        SELECT OrderDateKey
        FROM perf.FactSales_20M_base
        WHERE OrderDateKey IS NOT NULL
        LIMIT 1
    """).collect()
    if not date_row:
        raise ValueError("No OrderDateKey found in baseline.")
    date_key = int(date_row[0][0])

    # Pick a ProductKey that exists
    prod_row = spark.sql("""
        SELECT ProductKey
        FROM perf.FactSales_20M_base
        WHERE ProductKey IS NOT NULL
        LIMIT 1
    """).collect()
    if not prod_row:
        raise ValueError("No ProductKey found in baseline.")
    product_key = prod_row[0][0]

    Q1 = f"SELECT COUNT(*) FROM perf.FactSales_20M_base WHERE OrderDateKey = {date_key}"
    Q2 = f"SELECT SUM(NetAmount) FROM perf.FactSales_20M_base WHERE ProductKey = '{product_key}'"
    Q3 = "SELECT OrderStatus, SUM(NetAmount) AS NetSales FROM perf.FactSales_20M_base GROUP BY OrderStatus"

    print(f"Using OrderDateKey={date_key}, ProductKey={product_key}")

    time_sql("BASE Q1 (date filter)", Q1)
    time_sql("BASE Q2 (product filter)", Q2)
    time_sql("BASE Q3 (group by)", Q3)

except Exception as e:
    print("❌ Benchmark setup failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3) Partitioning (before/after)
# 
# ### Why it helps
# Partitioning physically organizes files by a column (often date).  
# Queries that filter on that column can skip entire partitions.
# 
# ### Plan
# - Create `perf.FactSales_20M_part` partitioned by `OrderDateKey`
# - Re-run the same benchmarks and compare


# CELL ********************

try:
    safe_sql("Drop partitioned table if exists", "DROP TABLE IF EXISTS perf.FactSales_20M_part")

    df = spark.read.table("perf.FactSales_20M_base")
    (df.write
       .mode("overwrite")
       .format("delta")
       .partitionBy("OrderDateKey")
       .saveAsTable("perf.FactSales_20M_part"))

    print("✅ Created perf.FactSales_20M_part partitioned by OrderDateKey")
    describe_detail("perf.FactSales_20M_part")

    Q1p = Q1.replace("perf.FactSales_20M_base", "perf.FactSales_20M_part")
    Q2p = Q2.replace("perf.FactSales_20M_base", "perf.FactSales_20M_part")
    Q3p = Q3.replace("perf.FactSales_20M_base", "perf.FactSales_20M_part")

    time_sql("PART Q1 (date filter)", Q1p)
    time_sql("PART Q2 (product filter)", Q2p)
    time_sql("PART Q3 (group by)", Q3p)

except Exception as e:
    print("❌ Partitioning demo failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4) OPTIMIZE (baseline compaction) (before/after)
# 
# ### Why it helps
# Even without Z-Order/V-Order, `OPTIMIZE` compacts many small files into fewer larger ones,
# reducing file-open overhead and improving scan performance.
# 
# ### Plan
# - Copy baseline → `perf.FactSales_20M_opt`
# - Measure before
# - Run `OPTIMIZE`
# - Measure after


# CELL ********************

try:
    safe_sql("Drop OPT table if exists", "DROP TABLE IF EXISTS perf.FactSales_20M_opt")
    safe_sql("Create OPT table", """
        CREATE TABLE perf.FactSales_20M_opt
        USING DELTA
        AS SELECT * FROM perf.FactSales_20M_base
    """)

    print("📌 Before OPTIMIZE")
    describe_detail("perf.FactSales_20M_opt")

    # Before timings
    Q1o = Q1.replace("perf.FactSales_20M_base", "perf.FactSales_20M_opt")
    Q2o = Q2.replace("perf.FactSales_20M_base", "perf.FactSales_20M_opt")
    Q3o = Q3.replace("perf.FactSales_20M_base", "perf.FactSales_20M_opt")
    time_sql("OPT(pre) Q1", Q1o)
    time_sql("OPT(pre) Q2", Q2o)
    time_sql("OPT(pre) Q3", Q3o)

    # Run OPTIMIZE
    safe_sql("OPTIMIZE perf.FactSales_20M_opt", "OPTIMIZE perf.FactSales_20M_opt")

    print("📌 After OPTIMIZE")
    describe_detail("perf.FactSales_20M_opt")

    # After timings
    time_sql("OPT(post) Q1", Q1o)
    time_sql("OPT(post) Q2", Q2o)
    time_sql("OPT(post) Q3", Q3o)

except Exception as e:
    print("❌ OPTIMIZE demo failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5) V-Order (before/after)
# 
# ### Why it helps
# V-Order is a Fabric-specific parquet layout optimization designed to improve read performance
# (especially for downstream engines and scan-heavy workloads).
# 
# ### Plan
# - Copy baseline → `perf.FactSales_20M_vorder`
# - Benchmark before
# - Run `OPTIMIZE ... VORDER`
# - Benchmark after


# CELL ********************

try:
    safe_sql("Drop VORDER table if exists", "DROP TABLE IF EXISTS perf.FactSales_20M_vorder")
    safe_sql("Create VORDER table", """
        CREATE TABLE perf.FactSales_20M_vorder
        USING DELTA
        AS SELECT * FROM perf.FactSales_20M_base
    """)

    print("📌 Before V-Order")
    describe_detail("perf.FactSales_20M_vorder")

    Q1v = Q1.replace("perf.FactSales_20M_base", "perf.FactSales_20M_vorder")
    Q2v = Q2.replace("perf.FactSales_20M_base", "perf.FactSales_20M_vorder")
    Q3v = Q3.replace("perf.FactSales_20M_base", "perf.FactSales_20M_vorder")
    time_sql("VORDER(pre) Q1", Q1v)
    time_sql("VORDER(pre) Q2", Q2v)
    time_sql("VORDER(pre) Q3", Q3v)

    safe_sql("OPTIMIZE ... VORDER", "OPTIMIZE perf.FactSales_20M_vorder VORDER")

    print("📌 After V-Order")
    describe_detail("perf.FactSales_20M_vorder")

    time_sql("VORDER(post) Q1", Q1v)
    time_sql("VORDER(post) Q2", Q2v)
    time_sql("VORDER(post) Q3", Q3v)

except Exception as e:
    print("❌ V-Order demo failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6) Z-Order (before/after)
# 
# ### Why it helps
# Z-Order clusters data for better **data skipping** on frequently-filtered columns.
# 
# ### Plan
# - Copy baseline → `perf.FactSales_20M_zorder`
# - Benchmark before
# - Run `OPTIMIZE ... ZORDER BY (ProductKey, OrderDateKey)`
# - Benchmark after (focus on Q2 and Q1)


# CELL ********************

try:
    safe_sql("Drop ZORDER table if exists", "DROP TABLE IF EXISTS perf.FactSales_20M_zorder")
    safe_sql("Create ZORDER table", """
        CREATE TABLE perf.FactSales_20M_zorder
        USING DELTA
        AS SELECT * FROM perf.FactSales_20M_base
    """)

    print("📌 Before Z-Order")
    describe_detail("perf.FactSales_20M_zorder")

    Q1z = Q1.replace("perf.FactSales_20M_base", "perf.FactSales_20M_zorder")
    Q2z = Q2.replace("perf.FactSales_20M_base", "perf.FactSales_20M_zorder")
    Q3z = Q3.replace("perf.FactSales_20M_base", "perf.FactSales_20M_zorder")
    time_sql("ZORDER(pre) Q1", Q1z)
    time_sql("ZORDER(pre) Q2", Q2z)
    time_sql("ZORDER(pre) Q3", Q3z)

    safe_sql("OPTIMIZE ... ZORDER", """
        OPTIMIZE perf.FactSales_20M_zorder
        ZORDER BY (ProductKey, OrderDateKey)
    """)

    print("📌 After Z-Order")
    describe_detail("perf.FactSales_20M_zorder")

    time_sql("ZORDER(post) Q1", Q1z)
    time_sql("ZORDER(post) Q2", Q2z)
    time_sql("ZORDER(post) Q3", Q3z)

except Exception as e:
    print("❌ Z-Order demo failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7) VACUUM (before/after) — create a “bad table” and clean it up
# 
# ### What we’re showing
# VACUUM removes old, unreferenced data files after the retention window.
# To make the effect visible in a demo, we intentionally create:
# - many small files
# - many stale files via DELETE/UPDATE
# 
# ⚠️ Demo note:
# Retention safety rules may be enforced in your runtime/workspace.
# If lowering retention is blocked, you can still show:
# - file explosion from small-file writes
# - improvement from OPTIMIZE
# …and explain VACUUM conceptually.


# CELL ********************

try:
    safe_sql("Drop bad table if exists", "DROP TABLE IF EXISTS perf.FactSales_20M_bad")

    df = spark.read.table("perf.FactSales_20M_base")

    # Create many small files
    (df.repartition(500)
       .write.mode("overwrite").format("delta")
       .saveAsTable("perf.FactSales_20M_bad"))

    print("📌 After small-file write")
    describe_detail("perf.FactSales_20M_bad")

    # Create stale files via DML (rewrites)
    safe_sql("DELETE slice", "DELETE FROM perf.FactSales_20M_bad WHERE OrderStatus IS NOT NULL AND OrderStatus = 5")
    safe_sql("UPDATE slice", "UPDATE perf.FactSales_20M_bad SET OrderStatus = 9 WHERE OrderStatus IS NOT NULL AND OrderStatus = 1")

    print("📌 After DELETE/UPDATE churn")
    describe_detail("perf.FactSales_20M_bad")

    # For demo, attempt to disable retention check and VACUUM aggressively
    # Some environments may block this; we catch and continue.
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        safe_sql("VACUUM (demo retention)", "VACUUM perf.FactSales_20M_bad RETAIN 0 HOURS")
    except Exception as e:
        print("⚠️ Could not run aggressive VACUUM (likely restricted).")
        print(f"Reason: {e}")

    print("📌 After VACUUM attempt")
    describe_detail("perf.FactSales_20M_bad")

except Exception as e:
    print("❌ VACUUM demo failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8) Delete Vectors (before/after)
# 
# ### What we’re showing
# Deletion vectors can reduce file rewrites for DELETE/UPDATE heavy workloads
# by tracking row-level removals more efficiently (merge-on-read).
# 
# ### Plan
# - Create `perf.FactSales_20M_dv_off` (default behavior)
# - Create `perf.FactSales_20M_dv_on` and attempt to enable deletion vectors
# - Run the same DELETE + UPDATE and compare runtimes and table stats
# 
# ⚠️ Reality check
# Whether deletion vectors are available can depend on runtime/table feature support.
# This section is written to attempt enablement and continue gracefully if it’s unsupported.


# CELL ********************

try:
    safe_sql("Drop dv_off", "DROP TABLE IF EXISTS perf.FactSales_20M_dv_off")
    safe_sql("Drop dv_on",  "DROP TABLE IF EXISTS perf.FactSales_20M_dv_on")

    safe_sql("Create dv_off", """
        CREATE TABLE perf.FactSales_20M_dv_off
        USING DELTA
        AS SELECT * FROM perf.FactSales_20M_base
    """)

    safe_sql("Create dv_on", """
        CREATE TABLE perf.FactSales_20M_dv_on
        USING DELTA
        AS SELECT * FROM perf.FactSales_20M_base
    """)

    dv_enabled = True
    try:
        safe_sql("Enable deletion vectors",
                 "ALTER TABLE perf.FactSales_20M_dv_on SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    except Exception as e:
        dv_enabled = False
        print("⚠️ Deletion vectors could not be enabled here.")
        print(f"Reason: {e}")

    pred_delete = "OrderStatus IS NOT NULL AND OrderStatus = 5"
    pred_update = "OrderStatus IS NOT NULL AND OrderStatus = 1"

    # Time DML on dv_off
    time_sql("DV_OFF DELETE", f"DELETE FROM perf.FactSales_20M_dv_off WHERE {pred_delete}", warmup=0, runs=1)
    time_sql("DV_OFF UPDATE", f"UPDATE perf.FactSales_20M_dv_off SET OrderStatus = 9 WHERE {pred_update}", warmup=0, runs=1)
    describe_detail("perf.FactSales_20M_dv_off")

    # Time DML on dv_on (even if DV enable fails, table exists and the timings still show baseline rewrite costs)
    time_sql("DV_ON  DELETE", f"DELETE FROM perf.FactSales_20M_dv_on WHERE {pred_delete}", warmup=0, runs=1)
    time_sql("DV_ON  UPDATE", f"UPDATE perf.FactSales_20M_dv_on SET OrderStatus = 9 WHERE {pred_update}", warmup=0, runs=1)
    describe_detail("perf.FactSales_20M_dv_on")

except Exception as e:
    print("❌ Deletion vectors demo failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  

# MARKDOWN ********************

# ## ✅ Final Summary — 10-Run Benchmark + Storage “Scoreboard” Charts (no table rebuild)
# 
# This final step **does not recreate any tables**. It:
# 
# 1) Discovers your existing `perf.FactSales_20M_*` tables (or uses a manual list)  
# 2) Picks a real `OrderDateKey` + `ProductKey` from your baseline so filters always match  
# 3) Runs the **same 3 benchmark queries 10 times** against each table  
# 4) Captures:
#    - avg / best / p50 / p90 runtimes (seconds)
#    - `numFiles`, `sizeInBytes`, `partitionColumns` from `DESCRIBE DETAIL`
# 5) Produces charts:
#    - **Runtime chart** (avg seconds by table, per query)
#    - **Storage chart** (size + file count by table)
# 
# > Tip for the demo: run this right after all optimizations so it “tells the story” in one place.


# CELL ********************

# 🧠 Clean Summary Code Cell (PySpark/Python)
# Excludes DV and churned tables. No table recreation.

import time, math
import pandas as pd
import matplotlib.pyplot as plt

RUNS = 10
WARMUP = 1
SCHEMA = "perf"
PREFIX = "factsales_20m_"

# ✅ Only include these suffixes (adjust if your names differ)
ALLOW_SUFFIXES = {"base", "part", "opt", "vorder", "zorder"}

# ❌ Exclude anything containing these substrings (extra safety)
EXCLUDE_CONTAINS = {"dv", "bad", "auto"}  # add more if needed

def exists_table(full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        try:
            spark.sql(f"DESCRIBE TABLE {full_name}")
            return True
        except Exception:
            return False

def discover_allowed_tables(schema=SCHEMA, prefix=PREFIX):
    """
    Discover perf tables, include only allowlist suffixes, exclude DV/bad.
    Returns (ScenarioLabel, fully_qualified_table).
    """
    rows = spark.sql(f"SHOW TABLES IN {schema}").collect()
    found = []
    for r in rows:
        name = r.tableName  # likely lowercase
        low = name.lower()
        if not low.startswith(prefix.lower()):
            continue

        suffix = low[len(prefix):]  # e.g. "base", "part"
        if suffix not in ALLOW_SUFFIXES:
            continue

        if any(x in low for x in EXCLUDE_CONTAINS):
            continue

        # Nice human label
        label_map = {
            "base": "Baseline",
            "part": "Partitioned",
            "opt": "Optimized",
            "vorder": "V-Order",
            "zorder": "Z-Order",
        }
        label = label_map.get(suffix, suffix.upper())
        found.append((label, f"{schema}.{name}"))

    # Order in a logical story sequence
    order = {"Baseline": 0, "Partitioned": 1, "Optimized": 2, "V-Order": 3, "Z-Order": 4}
    found.sort(key=lambda x: order.get(x[0], 999))
    return found

def clear_all_caches():
    try:
        spark.catalog.clearCache()
        spark.sql("CLEAR CACHE")
    except Exception:
        pass

def describe_detail_dict(table_name: str):
    d = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
    size = d.get("sizeInBytes")
    return {
        "numFiles": d.get("numFiles"),
        "sizeInBytes": size,
        "sizeGiB": (size / (1024**3)) if size is not None else None,
        "partitionColumns": ",".join(d.get("partitionColumns") or []),
    }

def run_sql_timed(sql_text: str, warmup: int, runs: int):
    for _ in range(warmup):
        spark.sql(sql_text).count()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        spark.sql(sql_text).count()
        t1 = time.perf_counter()
        times.append(t1 - t0)
    return times

def summarize_times(times):
    t = sorted(times)
    n = len(t)
    def pct(p):
        k = max(0, min(n-1, int(math.ceil(p*n) - 1)))
        return t[k]
    return {"avg_s": sum(t)/n, "best_s": t[0], "p50_s": pct(0.50), "p90_s": pct(0.90)}

try:
    # 1) Discover “clean” tables only
    TABLES = discover_allowed_tables()
    if not TABLES:
        print("⚠️ No allowlisted tables found. Here are tables in perf:")
        display(spark.sql(f"SHOW TABLES IN {SCHEMA}"))
        raise ValueError("No clean perf tables found. Ensure base/part/opt/vorder/zorder exist.")

    print("✅ Tables included:")
    for lbl, t in TABLES:
        print(f" - {lbl}: {t}")

    # 2) Pick filter values from Baseline (preferred)
    baseline = "perf.factsales_20m_base"
    sample_table = baseline if exists_table(baseline) else TABLES[0][1]

    # Choose a real date key + product key that exists
    date_key = spark.sql(f"""
        SELECT OrderDateKey FROM {sample_table}
        WHERE OrderDateKey IS NOT NULL
        LIMIT 1
    """).collect()
    if not date_key:
        raise ValueError(f"No OrderDateKey found in {sample_table}")
    date_key = int(date_key[0][0])

    prod_key = spark.sql(f"""
        SELECT ProductKey FROM {sample_table}
        WHERE ProductKey IS NOT NULL
        LIMIT 1
    """).collect()
    if not prod_key:
        raise ValueError(f"No ProductKey found in {sample_table}")
    product_key = prod_key[0][0]

    # Use a small range to make pruning/skipping visible
    date_low = date_key - 3
    date_high = date_key + 3

    print(f"\n✅ Using filter values from {sample_table}")
    print(f"   OrderDateKey range: [{date_low}, {date_high}]")
    print(f"   ProductKey: {product_key}")

    # 3) Queries tuned for optimizations
    QUERIES = [
        ("Q1 Date range (partition)", lambda t: f"""
            SELECT SUM(NetAmount)
            FROM {t}
            WHERE OrderDateKey BETWEEN {date_low} AND {date_high}
        """),
        ("Q2 Product + date (Z-Order)", lambda t: f"""
            SELECT SUM(NetAmount)
            FROM {t}
            WHERE ProductKey = '{product_key}'
              AND OrderDateKey BETWEEN {date_low} AND {date_high}
        """),
        ("Q3 Narrow scan group by", lambda t: f"""
            SELECT OrderStatus, SUM(NetAmount) AS NetSales
            FROM {t}
            WHERE OrderDateKey BETWEEN {date_low} AND {date_high}
            GROUP BY OrderStatus
        """),
    ]

    runtime_rows = []
    storage_rows = []

    # 4) Measure
    for scen, table in TABLES:
        clear_all_caches()

        d = describe_detail_dict(table)
        storage_rows.append({
            "Scenario": scen,
            "Table": table,
            "sizeGiB": d["sizeGiB"],
            "numFiles": d["numFiles"],
            "partitionColumns": d["partitionColumns"],
        })

        for qname, qfn in QUERIES:
            sql_text = qfn(table)
            times = run_sql_timed(sql_text, warmup=WARMUP, runs=RUNS)
            summ = summarize_times(times)

            runtime_rows.append({
                "Scenario": scen,
                "Query": qname,
                "avg_s": summ["avg_s"],
                "best_s": summ["best_s"],
                "p50_s": summ["p50_s"],
                "p90_s": summ["p90_s"],
            })

            print(f"✅ {scen:<12} | {qname:<26} | avg={summ['avg_s']:.3f}s best={summ['best_s']:.3f}s p50={summ['p50_s']:.3f}s p90={summ['p90_s']:.3f}s")

    runt_df = pd.DataFrame(runtime_rows)
    stor_df = pd.DataFrame(storage_rows)

    print("\n📌 Runtime results:")
    display(runt_df)

    print("\n📌 Storage results:")
    display(stor_df)

    # 5) Charts (one per query)
    for q in runt_df["Query"].unique():
        sub = runt_df[runt_df["Query"] == q].sort_values("avg_s", ascending=True)
        plt.figure(figsize=(12, 5))
        plt.bar(sub["Scenario"], sub["avg_s"])
        plt.xticks(rotation=20, ha="right")
        plt.ylabel("Average runtime (seconds) — lower is better")
        plt.title(f"10-run benchmark: {q}")
        plt.tight_layout()
        plt.show()

    # Storage size chart
    sp = stor_df.sort_values("sizeGiB", ascending=False)
    plt.figure(figsize=(12, 5))
    plt.bar(sp["Scenario"], sp["sizeGiB"])
    plt.xticks(rotation=20, ha="right")
    plt.ylabel("Table size (GiB)")
    plt.title("Delta storage size by scenario")
    plt.tight_layout()
    plt.show()

    # File count chart
    fp = stor_df.sort_values("numFiles", ascending=False)
    plt.figure(figsize=(12, 5))
    plt.bar(fp["Scenario"], fp["numFiles"])
    plt.xticks(rotation=20, ha="right")
    plt.ylabel("Number of files (numFiles)")
    plt.title("Delta file count by scenario")
    plt.tight_layout()
    plt.show()

    print("✅ Clean summary complete")

except Exception as e:
    print("❌ Clean summary step failed")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
