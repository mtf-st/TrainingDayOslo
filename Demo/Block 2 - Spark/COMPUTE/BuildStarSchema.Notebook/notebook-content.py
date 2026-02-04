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
# META           "id": "b5531d4b-cb01-4361-8525-cbbbbd7c269c"
# META         },
# META         {
# META           "id": "14877ca1-3b6b-4f11-b3ff-e1dc9b592370"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🏗️ Building a Star Schema from AdventureWorksLT
# ## Microsoft Fabric Lakehouse Notebook Demo
# 
# This notebook demonstrates how **Microsoft Fabric notebooks** let us combine  
# **Spark SQL, PySpark, and Scala** in a single, coherent data engineering workflow.
# 
# We start with AdventureWorksLT tables already loaded into a Lakehouse and end with
# a **Delta-based star schema** ready for analytics and Power BI.
# 
# ---
# 
# ## 🧱 Step 0 — Prepare the Data Warehouse Schema
# 
# ### What we’re doing
# Before creating any dimensional models, we:
# - Create a dedicated `dw` schema
# - Verify that our source tables exist
# - Perform quick row-count checks
# 
# This is a great example of **Spark SQL** operating directly over Lakehouse tables.
# 
# ---
# 
# ## 📥 Step 0.5 — Load AdventureWorksLT Parquet Files into DataFrames (Lakehouse *Files*)
# 
# ### What we’re doing
# In this notebook, the AdventureWorksLT tables are not registered as Lakehouse tables yet — they live as **Parquet files under the Lakehouse `Files/` area**.  
# So before we can build dimensions and facts, we first load each required dataset into a Spark **DataFrame**.
# 
# This step gives us a clean, explicit starting point for the rest of the notebook:
# - ✅ repeatable reads from `Files/…`
# - ✅ easy to inspect with `display()`
# - ✅ we can optionally create **temp views** so Spark SQL can query them
# 
# ---
# 
# ### What you need to know about paths in Fabric
# In Microsoft Fabric notebooks, the Lakehouse `Files/` area is accessible using paths like:
# 
# - `Files/<folder>/<file>.parquet`
# - or a folder containing parquet parts: `Files/<folder>/<tableName>/`
# 
# > **Tip:** If your parquet is stored as a folder (common when written by Spark), point to the folder.  
# > If it’s a single parquet file, point to the `.parquet` file.
# 
# ---
# 
# ### How it works
# 1. Define a base folder where your parquet files are stored  
# 2. Read each required table into a DataFrame  
# 3. (Optional) Register each DataFrame as a **temporary view** for Spark SQL steps later
# 
# ---
# 
# ### 🧠 Code Cell (PySpark) — Load Parquet into DataFrames


# CELL ********************

from pyspark.sql import DataFrame

# 🔧 Change this to match your Lakehouse Files folder structure
# Example folder layouts you might have:
#   Files/AdventureWorksLT/SalesLT/Customer/
#   Files/AdventureWorksLT/SalesLT/Customer.parquet
BASE = "abfss://31d9702e-629f-44e9-b1cf-b4dc9fa73122@onelake.dfs.fabric.microsoft.com/b5531d4b-cb01-4361-8525-cbbbbd7c269c/Files/AzureSQLDB/SalesLT"

def read_parquet_table(name: str) -> DataFrame:
    """
    Reads a parquet 'table' from the Lakehouse Files area.
    Works whether the table is stored as a folder or a single .parquet file
    (assuming your BASE path matches your layout).
    """
    folder_path = f"{BASE}/{name}"
    file_path   = f"{BASE}/{name}/2026/02/03/21/36/*.parquet"
    try:
        df = spark.read.parquet(folder_path)
        return df
    except Exception:
        # fall back to single-file parquet
        return spark.read.parquet(file_path)

# ✅ Load only what we need for the star schema
customer          = read_parquet_table("Customer")
soh               = read_parquet_table("SalesOrderHeader")
sod               = read_parquet_table("SalesOrderDetail")
product           = read_parquet_table("Product")
productCategory   = read_parquet_table("ProductCategory")
productModel      = read_parquet_table("ProductModel")
address           = read_parquet_table("Address")
customerAddress   = read_parquet_table("CustomerAddress")

# (Optional but very demo-friendly) create temp views for Spark SQL usage
customer.createOrReplaceTempView("src_Customer")
soh.createOrReplaceTempView("src_SalesOrderHeader")
sod.createOrReplaceTempView("src_SalesOrderDetail")
product.createOrReplaceTempView("src_Product")
productCategory.createOrReplaceTempView("src_ProductCategory")
productModel.createOrReplaceTempView("src_ProductModel")
address.createOrReplaceTempView("src_Address")
customerAddress.createOrReplaceTempView("src_CustomerAddress")

# Quick sanity display
display(customer.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📅 Step 1 — Create the Date Dimension (`DimDate`)
# 
# ### What we’re doing
# In this step, we build a **Date dimension**, which is a foundational component of any star schema.
# Rather than relying on dates embedded in fact tables, a dedicated `DimDate` allows us to:
# 
# - Filter and group data efficiently by time
# - Join using compact integer keys instead of timestamps
# - Add rich calendar attributes without repeating logic in every query
# 
# ---
# 
# ### Where the data comes from
# To ensure our date dimension matches the data we’ll actually analyze, we derive dates directly from
# the **SalesOrderHeader** table:
# 
# - `OrderDate`
# - `DueDate`
# - `ShipDate`
# 
# This guarantees that every date used in the fact table exists in `DimDate`.
# 
# ---
# 
# ### How it works
# 1. Extract all relevant dates from the source table  
# 2. Union them into a single list  
# 3. Remove nulls and duplicates  
# 4. Generate a surrogate `DateKey` in **YYYYMMDD** format  
# 5. Enrich each date with common calendar attributes
# 
# ---
# 
# ### 🧠 Code Cell (PySpark)


# MARKDOWN ********************

# ## 📅 Step 1 — Create the DW Schema + Date Dimension (`dw.DimDate`)
# 
# ### What we’re doing
# Before writing any dimensional tables, we need a dedicated schema (database) called **`dw`**.  
# Then we’ll build a **Date dimension** from the transactional dates in `SalesOrderHeader`.
# 
# A dedicated `DimDate` lets us:
# - Filter/group by time efficiently
# - Join facts to dates using a compact integer key (`DateKey`)
# - Add rich calendar attributes once (instead of repeating logic in every report)
# 
# ### Inputs
# From the Parquet-loaded DataFrame **`soh`** (created in Step 0.5), we use:
# - `OrderDate`
# - `DueDate`
# - `ShipDate`
# 
# ### Output
# - A schema/database: **`dw`**
# - A Delta table: **`dw.DimDate`**
# 
# 
# **Next:** Run the following **PySpark code cell** to create the schema and build/write `dw.DimDate`.


# CELL ********************

# 🧠 Step 1 Code Cell (PySpark) — Create dw schema + Build dw.DimDate
# Assumes Step 0.5 has already loaded Parquet into DataFrames and created `soh`.

from pyspark.sql import functions as F

try:
    # --- 0) Create the DW schema/database if it doesn't exist ---
    # Using Spark SQL inside PySpark so we can catch and handle errors cleanly.
    spark.sql("CREATE DATABASE IF NOT EXISTS dw")
    print("✅ Ensured schema/database exists: dw")

    # --- Safety check: ensure the source DataFrame exists ---
    if "soh" not in globals():
        raise ValueError("DataFrame 'soh' not found. Run Step 0.5 (load Parquet into DataFrames) first.")

    # --- 1) Collect all distinct dates used in the sales process ---
    # Union OrderDate, DueDate, ShipDate into one 'Date' column, remove nulls, then deduplicate.
    dates = (
        soh.select(F.to_date("OrderDate").alias("Date"))
           .unionByName(soh.select(F.to_date("DueDate").alias("Date")))
           .unionByName(soh.select(F.to_date("ShipDate").alias("Date")))
           .where(F.col("Date").isNotNull())
           .distinct()
    )

    # --- 2) Add commonly used calendar attributes ---
    # DateKey uses the conventional YYYYMMDD format (int), great for BI models and fast joins.
    dimDate = (
        dates
        .withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))
        .withColumn("Year", F.year("Date"))
        .withColumn("Quarter", F.quarter("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("MonthName", F.date_format("Date", "MMMM"))
        .withColumn("DayOfMonth", F.dayofmonth("Date"))
        .withColumn("DayOfWeek", F.date_format("Date", "E"))
        .withColumn("WeekOfYear", F.weekofyear("Date"))
        .select(
            "DateKey",
            "Date",
            "Year",
            "Quarter",
            "Month",
            "MonthName",
            "DayOfMonth",
            "DayOfWeek",
            "WeekOfYear"
        )
    )

    # --- 3) Persist as a Delta table in the dw schema ---
    # Overwrite mode is used for demo simplicity.
    dimDate.write.mode("overwrite").format("delta").saveAsTable("dw.DimDate")
    print("✅ Success: Created table dw.DimDate")

    # --- 4) Quick peek (demo-friendly) ---
    display(dimDate.orderBy("Date").limit(10))

except Exception as e:
    # Friendly error message for demos
    print("❌ Failed in Step 1 (create schema and/or build DimDate)")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 👤 Step 2 — Create the Customer Dimension (`dw.DimCustomer`)
# 
# ### What we’re doing
# In this step we create a **Customer dimension** that contains descriptive attributes about each customer.
# Dimensions provide the “who / what” context that makes facts easy to slice and filter.
# 
# We will:
# - Start from the Parquet-loaded DataFrame **`customer`** (from Step 0.5)
# - Create a deterministic surrogate key called **`CustomerKey`**
# - Select the most useful descriptive columns for analytics
# - Write the result as a Delta table: **`dw.DimCustomer`**
# 
# ---
# 
# ### Why a surrogate key?
# Even though the source has `CustomerID`, we add `CustomerKey` to:
# - decouple the star schema from source-system key choices
# - make future extensions easier (like Slowly Changing Dimensions / SCD)
# - keep joins stable in a lakehouse pipeline
# 
# For this demo, the surrogate key is created using a **SHA-256 hash** of the business key (`CustomerID`).
# 
# ---
# 
# ### Why Scala?
# This is a demo notebook, so we use **Scala** here to showcase:
# - Spark-native APIs in Fabric notebooks
# - another language working seamlessly alongside PySpark and SQL
# 
# ---
# 
# **Next:** Run the following **Scala code cell** to build and write `dw.DimCustomer`.
# 


# CELL ********************

# MAGIC %%spark
# MAGIC // 🧠 Step 2 Code Cell (Scala) — Build dw.DimCustomer
# MAGIC // Assumes Step 0.5 has already loaded Parquet into DataFrames and created `customer`.
# MAGIC // Assumes Step 1 created the `dw` schema/database.
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC try {
# MAGIC   // --- 0) Ensure the DW schema/database exists (safe to run repeatedly) ---
# MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS dw")
# MAGIC   println("✅ Ensured schema/database exists: dw")
# MAGIC 
# MAGIC   // --- 1) Safety check: ensure the source DataFrame exists ---
# MAGIC   // In Scala, we can reliably check whether a temp view exists (demo-friendly).
# MAGIC   // Step 0.5 created temp views like src_Customer; use that as the contract.
# MAGIC   val viewName = "src_Customer"
# MAGIC   if (!spark.catalog.tableExists(viewName)) {
# MAGIC     throw new IllegalStateException(
# MAGIC       s"Temp view '$viewName' not found. Run Step 0.5 (load Parquet + create temp views) first."
# MAGIC     )
# MAGIC   }
# MAGIC 
# MAGIC   val customerDF = spark.table(viewName)
# MAGIC 
# MAGIC   // --- 2) Build the dimension ---
# MAGIC   // CustomerKey is a deterministic surrogate key based on CustomerID.
# MAGIC   val dimCustomer =
# MAGIC     customerDF
# MAGIC       .withColumn(
# MAGIC         "CustomerKey",
# MAGIC         sha2(concat_ws("||", col("CustomerID").cast("string")), 256)
# MAGIC       )
# MAGIC       .select(
# MAGIC         col("CustomerKey"),
# MAGIC         col("CustomerID"),
# MAGIC         col("Title"),
# MAGIC         col("FirstName"),
# MAGIC         col("MiddleName"),
# MAGIC         col("LastName"),
# MAGIC         col("Suffix"),
# MAGIC         col("CompanyName"),
# MAGIC         col("SalesPerson"),
# MAGIC         col("EmailAddress"),
# MAGIC         col("Phone")
# MAGIC       )
# MAGIC 
# MAGIC   // --- 3) Persist as Delta table ---
# MAGIC   // Overwrite is used for demo simplicity.
# MAGIC   dimCustomer.write.mode("overwrite").format("delta").saveAsTable("dw.DimCustomer")
# MAGIC   println("✅ Success: Created table dw.DimCustomer")
# MAGIC 
# MAGIC   // --- 4) Quick peek (demo-friendly) ---
# MAGIC   display(dimCustomer.limit(10))
# MAGIC 
# MAGIC } catch {
# MAGIC   case e: Exception =>
# MAGIC     println("❌ Failed to create dw.DimCustomer")
# MAGIC     println(s"Error: ${e.getMessage}")
# MAGIC }


# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🛒 Step 3 — Create the Product Dimension (`dw.DimProduct`)
# 
# ### What we’re doing
# Products in AdventureWorksLT are spread across multiple normalized tables.  
# For analytics, we want a single, flat Product dimension containing:
# 
# - Product attributes (name, number, color, size, price)
# - Category name
# - Model name
# 
# So in this step we:
# 1. Start from the Parquet-loaded temp views:
#    - `src_Product`
#    - `src_ProductCategory`
#    - `src_ProductModel`
# 2. Flatten them into a single dimension
# 3. Create a deterministic surrogate key `ProductKey`
# 4. Write the result to **`dw.DimProduct`** (Delta)
# 
# ---
# 
# ### Why Spark SQL *and* try/catch?
# Spark SQL is perfect here because the transformation is mostly joins + projections.
# 
# However, Spark SQL cells don’t support try/catch.  
# So we execute SQL **from Python**, which lets us:
# - keep the transformation “SQL-shaped”
# - still catch errors cleanly for demo reliability
# 
# ---
# 
# **Next:** Run the following **PySpark code cell** to build and write `dw.DimProduct` using Spark SQL inside try/catch.


# CELL ********************

# 🧠 Step 3 Code Cell (PySpark) — Build dw.DimProduct using Spark SQL (with try/catch)
# Assumes Step 0.5 created temp views: src_Product, src_ProductCategory, src_ProductModel
# Assumes Step 1/2 created schema/database: dw

try:
    # --- 0) Ensure DW schema exists (safe to re-run) ---
    spark.sql("CREATE DATABASE IF NOT EXISTS dw")
    print("✅ Ensured schema/database exists: dw")

    # --- 1) Safety checks: required temp views must exist ---
    required_views = ["src_Product", "src_ProductCategory", "src_ProductModel"]
    missing = [v for v in required_views if not spark.catalog.tableExists(v)]
    if missing:
        raise ValueError(f"Missing temp view(s): {missing}. Run Step 0.5 (load Parquet + create temp views) first.")

    # --- 2) Create DimProduct (flattened) as a Delta table ---
    # We use CREATE OR REPLACE TABLE for demo simplicity.
    # ProductKey is a deterministic surrogate key (sha2 over ProductID).
    spark.sql("""
        CREATE OR REPLACE TABLE dw.DimProduct
        USING DELTA
        AS
        SELECT
          sha2(concat_ws('||', CAST(p.ProductID AS STRING)), 256) AS ProductKey,
          p.ProductID,
          p.Name AS ProductName,
          p.ProductNumber,
          p.Color,
          p.Size,
          p.Weight,
          p.StandardCost,
          p.ListPrice,
          c.Name AS CategoryName,
          m.Name AS ModelName
        FROM src_Product p
        LEFT JOIN src_ProductCategory c
          ON p.ProductCategoryID = c.ProductCategoryID
        LEFT JOIN src_ProductModel m
          ON p.ProductModelID = m.ProductModelID
    """)

    print("✅ Success: Created table dw.DimProduct")

    # --- 3) Quick peek (demo-friendly) ---
    dimProduct = spark.read.table("dw.DimProduct")
    display(dimProduct.limit(10))

except Exception as e:
    print("❌ Failed to create dw.DimProduct")
    print(f"Error: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🏠 Step 4 — Create the Address Dimension (`dw.DimAddress`) using Spark SQL
# 
# ### What we’re doing
# Orders reference addresses in two roles:
# - **Ship To**
# - **Bill To**
# 
# Instead of creating two separate address tables, we build **one** `DimAddress` and later “role-play” it in the model.
# 
# In this step we:
# 1. Read from the temp view `src_Address` (created in Step 0.5)
# 2. Generate a deterministic surrogate key `AddressKey` using `sha2(AddressID)`
# 3. Write a Delta table `dw.DimAddress`
# 
# ---
# 
# ### Important note about error handling
# A pure **Spark SQL** cell does **not** support `TRY/CATCH`.  
# So you have two options:
# 
# - ✅ **Option A (pure SQL cell)** — simplest for demos, errors show inline  
# - ✅ **Option B (SQL executed from Python/Scala)** — allows try/catch
# 
# Below is **Option A**, as requested: **Spark SQL**.
# 
# ---
# 
# **Next:** Run the following **Spark SQL code cell**.


# CELL ********************

# MAGIC %%sql
# MAGIC -- 🧠 Step 4 Code Cell (Spark SQL) — Build dw.DimAddress
# MAGIC 
# MAGIC -- 0) Ensure the DW schema exists
# MAGIC CREATE DATABASE IF NOT EXISTS dw;
# MAGIC 
# MAGIC -- 1) Create or replace the Address dimension as a Delta table
# MAGIC CREATE OR REPLACE TABLE dw.DimAddress
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   sha2(concat_ws('||', CAST(AddressID AS STRING)), 256) AS AddressKey,
# MAGIC   AddressID,
# MAGIC   AddressLine1,
# MAGIC   AddressLine2,
# MAGIC   City,
# MAGIC   StateProvince,
# MAGIC   CountryRegion,
# MAGIC   PostalCode
# MAGIC FROM src_Address;
# MAGIC 
# MAGIC -- 2) Quick peek (demo-friendly)
# MAGIC SELECT * FROM dw.DimAddress LIMIT 10;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Step 5 — Create the Sales Fact Table (`dw.FactSales`) using Scala (with try/catch)
# 
# ### What we’re doing
# Now we build the central **Fact table** for the star schema.
# 
# ✅ **Fact grain:** *one row per Sales Order Line* (from `SalesOrderDetail`)  
# This is the most flexible grain for analytics (you can always roll up to order level later).
# 
# We will:
# 1. Join `src_SalesOrderDetail` (line items) to `src_SalesOrderHeader` (header attributes)
# 2. Generate foreign keys to dimensions using deterministic hashes:
#    - `CustomerKey` (from `CustomerID`)
#    - `ProductKey`  (from `ProductID`)
#    - `ShipToAddressKey`, `BillToAddressKey` (from Address IDs)
# 3. Create date keys (`YYYYMMDD` ints) for joining to `dw.DimDate`
# 4. Compute measures:
#    - `ExtendedAmount`
#    - `DiscountAmount`
#    - `NetAmount`
# 5. Write the result to **Delta**: `dw.FactSales`
# 
# ---
# 
# ### ⚠️ Important modeling note (great for demos)
# Some amounts on `SalesOrderHeader` (like `SubTotal` / `TotalDue`) are **order-level** values.
# If you include them in a line-grain fact and then sum them, you will overcount.
# 
# For this demo we **include them**, but treat them carefully in reporting:
# - aggregate with `MAX` at `SalesOrderID`, or
# - create a separate `FactSalesOrder` later at order grain
# 
# ---
# 
# **Next:** Run the following **Scala code cell** to build and write `dw.FactSales`.


# CELL ********************

# MAGIC %%spark
# MAGIC // 🧠 Step 5 Code Cell (Scala) — Build dw.FactSales (line grain) with try/catch
# MAGIC // Assumes Step 0.5 created temp views: src_SalesOrderHeader, src_SalesOrderDetail
# MAGIC // Assumes dw schema exists (Step 1 created it; we also ensure it here safely)
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC try {
# MAGIC   // --- 0) Ensure DW schema exists (safe to run repeatedly) ---
# MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS dw")
# MAGIC   println("✅ Ensured schema/database exists: dw")
# MAGIC 
# MAGIC   // --- 1) Safety checks: ensure source temp views exist ---
# MAGIC   val requiredViews = Seq("src_SalesOrderHeader", "src_SalesOrderDetail")
# MAGIC   val missingViews = requiredViews.filter(v => !spark.catalog.tableExists(v))
# MAGIC   if (missingViews.nonEmpty) {
# MAGIC     throw new IllegalStateException(
# MAGIC       s"Missing temp view(s): ${missingViews.mkString(", ")}. Run Step 0.5 first."
# MAGIC     )
# MAGIC   }
# MAGIC 
# MAGIC   // --- 2) Load sources from temp views ---
# MAGIC   val h = spark.table("src_SalesOrderHeader").alias("h") // header
# MAGIC   val d = spark.table("src_SalesOrderDetail").alias("d") // detail (line)
# MAGIC 
# MAGIC   // --- 3) Join header + detail (inner join ensures only valid order lines remain) ---
# MAGIC   val joined = d.join(h, col("d.SalesOrderID") === col("h.SalesOrderID"), "inner")
# MAGIC 
# MAGIC   // --- 4) Build the fact table with keys + measures ---
# MAGIC   val factSales =
# MAGIC     joined
# MAGIC       // Dimension FK hashes (deterministic surrogate keys)
# MAGIC       .withColumn("CustomerKey", sha2(concat_ws("||", col("h.CustomerID").cast("string")), 256))
# MAGIC       .withColumn("ProductKey",  sha2(concat_ws("||", col("d.ProductID").cast("string")), 256))
# MAGIC       .withColumn("ShipToAddressKey", sha2(concat_ws("||", col("h.ShipToAddressID").cast("string")), 256))
# MAGIC       .withColumn("BillToAddressKey", sha2(concat_ws("||", col("h.BillToAddressID").cast("string")), 256))
# MAGIC 
# MAGIC       // Date keys (YYYYMMDD int) for joining to dw.DimDate
# MAGIC       .withColumn("OrderDateKey", date_format(to_date(col("h.OrderDate")), "yyyyMMdd").cast("int"))
# MAGIC       .withColumn("DueDateKey",   date_format(to_date(col("h.DueDate")),   "yyyyMMdd").cast("int"))
# MAGIC       .withColumn("ShipDateKey",  date_format(to_date(col("h.ShipDate")),  "yyyyMMdd").cast("int"))
# MAGIC 
# MAGIC       // Measures
# MAGIC       .withColumn("ExtendedAmount", col("d.OrderQty") * col("d.UnitPrice"))
# MAGIC       .withColumn("DiscountAmount", col("d.OrderQty") * col("d.UnitPrice") * col("d.UnitPriceDiscount"))
# MAGIC       .withColumn("NetAmount", col("ExtendedAmount") - col("DiscountAmount"))
# MAGIC 
# MAGIC       // Final projection (keep it tidy for BI)
# MAGIC       .select(
# MAGIC         // Degenerate identifiers (useful for drill-through)
# MAGIC         col("h.SalesOrderID").as("SalesOrderID"),
# MAGIC         col("d.SalesOrderDetailID").as("SalesOrderDetailID"),
# MAGIC 
# MAGIC         // Foreign keys to dimensions
# MAGIC         col("CustomerKey"),
# MAGIC         col("ProductKey"),
# MAGIC         col("OrderDateKey"),
# MAGIC         col("DueDateKey"),
# MAGIC         col("ShipDateKey"),
# MAGIC         col("ShipToAddressKey"),
# MAGIC         col("BillToAddressKey"),
# MAGIC 
# MAGIC         // Header attributes (optional)
# MAGIC         col("h.PurchaseOrderNumber").as("PurchaseOrderNumber"),
# MAGIC         col("h.ShipMethod").as("ShipMethod"),
# MAGIC         col("h.OnlineOrderFlag").as("OnlineOrderFlag"),
# MAGIC         col("h.Status").as("OrderStatus"),
# MAGIC 
# MAGIC         // Line attributes / measures
# MAGIC         col("d.OrderQty").as("OrderQty"),
# MAGIC         col("d.UnitPrice").as("UnitPrice"),
# MAGIC         col("d.UnitPriceDiscount").as("UnitPriceDiscount"),
# MAGIC         col("ExtendedAmount"),
# MAGIC         col("DiscountAmount"),
# MAGIC         col("NetAmount"),
# MAGIC 
# MAGIC         // Header-level amounts (⚠️ careful when summing at line grain)
# MAGIC         col("h.TaxAmt").as("TaxAmount"),
# MAGIC         col("h.Freight").as("FreightAmount"),
# MAGIC         col("h.SubTotal").as("OrderSubTotal"),
# MAGIC         col("h.TotalDue").as("OrderTotalDue")
# MAGIC       )
# MAGIC 
# MAGIC   // --- 5) Write out as Delta ---
# MAGIC   factSales.write.mode("overwrite").format("delta").saveAsTable("dw.FactSales")
# MAGIC   println("✅ Success: Created table dw.FactSales")
# MAGIC 
# MAGIC   // --- 6) Quick peek (demo-friendly) ---
# MAGIC   display(factSales.limit(10))
# MAGIC 
# MAGIC } catch {
# MAGIC   case e: Exception =>
# MAGIC     println("❌ Failed to create dw.FactSales")
# MAGIC     println(s"Error: ${e.getMessage}")
# MAGIC }


# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✅ Step 6 — Validate the Star Schema
# 
# ### What we’re doing
# Before calling it done, we’ll run two quick validations:
# 
# 1. **Foreign key coverage**  
#    Ensures the hashed keys in the fact table can actually find matching rows in dimensions.
# 
# 2. **A BI-style aggregation query**  
#    Example: Net Sales by Year and Product Category (this is the kind of query you’d do in Power BI).
# 
# These are fast, demo-friendly, and help catch issues early.


# CELL ********************

# MAGIC %%sql
# MAGIC -- 🧠 Step 6 Code Cell (Spark SQL) — FK coverage checks
# MAGIC 
# MAGIC -- Fact rows that fail to find a matching dimension row are a red flag
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN c.CustomerKey IS NULL THEN 1 ELSE 0 END) AS MissingCustomer,
# MAGIC   SUM(CASE WHEN p.ProductKey  IS NULL THEN 1 ELSE 0 END) AS MissingProduct,
# MAGIC   SUM(CASE WHEN a_ship.AddressKey IS NULL THEN 1 ELSE 0 END) AS MissingShipToAddress,
# MAGIC   SUM(CASE WHEN a_bill.AddressKey IS NULL THEN 1 ELSE 0 END) AS MissingBillToAddress
# MAGIC FROM dw.FactSales f
# MAGIC LEFT JOIN dw.DimCustomer c
# MAGIC   ON f.CustomerKey = c.CustomerKey
# MAGIC LEFT JOIN dw.DimProduct p
# MAGIC   ON f.ProductKey = p.ProductKey
# MAGIC LEFT JOIN dw.DimAddress a_ship
# MAGIC   ON f.ShipToAddressKey = a_ship.AddressKey
# MAGIC LEFT JOIN dw.DimAddress a_bill
# MAGIC   ON f.BillToAddressKey = a_bill.AddressKey;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
