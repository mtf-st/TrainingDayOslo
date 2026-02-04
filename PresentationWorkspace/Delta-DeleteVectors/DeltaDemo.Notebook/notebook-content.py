# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2a6331a9-231b-4456-9151-cb02be4a7655",
# META       "default_lakehouse_name": "DemoData",
# META       "default_lakehouse_workspace_id": "e1b5da95-2f32-4f5a-8f45-e9634cd2affb",
# META       "known_lakehouses": [
# META         {
# META           "id": "2a6331a9-231b-4456-9151-cb02be4a7655"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "2ab79a2e-762e-aea4-457f-9dc6fc4a0947",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_prefix='dummy2'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # requires an environment with faker, assumes you are using delta 4.0 etc
# # code is messy and requires additional work but it validates the core ICT capability

# CELL ********************

# important things
from pyspark.sql.functions import udf, monotonically_increasing_id, min, max, count, col, lit, sum, when, round as sql_round, input_file_name, lag, when
from pyspark.sql import DataFrame, Row
from pyspark.sql.window import Window
from pyspark.sql.types import *
from faker import Faker
import random
from delta.tables import DeltaTable # probs shouldn't do this.
import time
import notebookutils
import json, math, re, base64, uuid
from datetime import datetime
from typing import List, Dict, Optional,Tuple, Union
import builtins  # Add this to access Python's built-in round

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get("spark.fabric.resourceProfile")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_rows=10_000_000 #init number of rows
#num_rows = 1_000_000
modulo=10 #modulo number for % of rows to be updated and deleted if 20, then 1/20 = 5% etc
extra_rows=1_000_000 #how many additional rows to insert
#extra_rows= 100_000
#table_prefix='spark4' #just a prefix for the table name
#table_prefix='precooked'

non_ict_prefix = 'nict'
ict_prefix='ict'
generate_data = False
generate_changes = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nothing needs to change below this

#lh_source = [mount.source for mount in notebookutils.fs.mounts() if mount.localPath == "/lakehouse/default"][0]
# i hate that this returns the result

# lower case the variables to not break notebook utils
table_prefix=table_prefix.lower()
ict_prefix=ict_prefix.lower()
non_ict_prefix=non_ict_prefix.lower()

# create table names
#initial_table = f'{table_prefix}_{non_ict_prefix}_1_initialcopy'
initial_table = f'{table_prefix}_standard_table'
second_table = f'{table_prefix}_{non_ict_prefix}_2_secondcopy'

dv_table = f'{table_prefix}_dv_table'

initial_ict_table = f'{table_prefix}_{ict_prefix}_1_initialcopy'
second_ict_table = f'{table_prefix}_{ict_prefix}_2_secondcopy'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Faker
fake = Faker()

# Define UDFs for each field
@udf(StringType())
def gen_first_name():
    return fake.first_name()

@udf(StringType())
def gen_last_name():
    return fake.last_name()

@udf(StringType())
def gen_email():
    return fake.email()

@udf(StringType())
def gen_phone():
    return fake.phone_number()

@udf(StringType())
def gen_city():
    return fake.city()

@udf(StringType())
def gen_state():
    return fake.state()

@udf(StringType())
def gen_country():
    return fake.country()

@udf(StringType())
def gen_address():
    return fake.street_address()

@udf(StringType())
def gen_postal_code():
    return fake.postcode()

@udf(StringType())
def gen_company():
    return fake.company()

@udf(StringType())
def gen_job():
    return fake.job()

@udf(DoubleType())
def gen_balance():
    return round(random.uniform(-1000, 50000), 2)

@udf(IntegerType())
def gen_credit_score():
    return random.randint(300, 850)

@udf(StringType())
def gen_tier():
    return random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_delta_files_with_versions_enhanced(
    table_name: str,
    base_path: str = "Tables",
    max_versions: Optional[int] = None,
    verbose: Union[bool, int] = 1
) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Get Delta table files with their associated version information.
    
    Parameters:
    -----------
    table_name : str
        Name of the Delta table
    base_path : str
        Base path where tables are stored (default: "Tables")
    max_versions : Optional[int]
        Maximum number of versions to analyze (default: None - all versions)
    verbose : Union[bool, int]
        Verbosity level:
        - 0 or False: Silent mode - no output except errors
        - 1 or True: Basic progress messages (default)
        - 2: Detailed output including file samples and statistics
    
    Returns:
    --------
    Tuple of 4 DataFrames:
        1. detailed_df: All files per version with file type (data/deletion_vector)
        2. summary_df: Aggregated summary per version (all files)
        3. new_files_df: Only new files added in each version with file type
        4. new_files_summary_df: Aggregated summary of new files per version
    """
    
    # Convert boolean verbose to int for easier handling
    if isinstance(verbose, bool):
        verbose = 1 if verbose else 0
    
    table_path = f"{base_path}/dbo/{table_name}"
    
    # Get file listing including deletion vectors
    try:
        files = notebookutils.fs.ls(table_path)
    except Exception as e:
        print(f"Error reading directory {table_path}: {e}")
        empty_schema = "version INT, error STRING"
        empty_df = spark.createDataFrame([], empty_schema)
        return empty_df, empty_df, empty_df, empty_df
    
    # Convert files to DataFrame - INCLUDE BOTH .parquet AND .bin files
    data = []
    
    for f in files:
        if f.isFile:
            # Determine file type based on extension and name pattern
            if f.name.endswith('.bin') and f.name.startswith('deletion_vector_'):
                # Deletion vector .bin files
                file_type = "deletion_vector"
                data.append(Row(
                    name=f.name,
                    path=f.path,
                    size_kb=builtins.round(f.size / 1024.0, 2),
                    modifyTime=datetime.fromtimestamp(f.modifyTime / 1000.0),
                    file_type=file_type
                ))
            elif f.name.endswith('.parquet'):
                # Data parquet files
                if 'deletion_vector' in f.name.lower():
                    file_type = "deletion_vector"
                else:
                    file_type = "data"
                
                data.append(Row(
                    name=f.name,
                    path=f.path,
                    size_kb=builtins.round(f.size / 1024.0, 2),
                    modifyTime=datetime.fromtimestamp(f.modifyTime / 1000.0),
                    file_type=file_type
                ))
    
    if not data:
        if verbose >= 1:
            print(f"No data or deletion vector files found in {table_path}")
        empty_schema = "version INT, message STRING"
        empty_df = spark.createDataFrame([], empty_schema)
        return empty_df, empty_df, empty_df, empty_df
    
    files_df = spark.createDataFrame(data)
    
    if verbose >= 1:
        print(f"Found {len(data)} files total:")
        file_type_counts = files_df.groupBy("file_type").count().collect()
        for row in file_type_counts:
            print(f"  - {row.file_type}: {row['count']} files")
    
    # Get Delta history
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        history_df = delta_table.history()
        
        if max_versions:
            history_df = history_df.limit(max_versions)
        
    except Exception as e:
        print(f"Error reading Delta table {table_path}: {e}")
        empty_schema = "version INT, error STRING"
        empty_df = spark.createDataFrame([], empty_schema)
        return empty_df, empty_df, empty_df, empty_df
    
    history_rows = history_df.select(
        "version", "timestamp", "operation", "userName"
    ).orderBy(col("version").desc()).collect()
    
    versions_with_files = []
    
    if verbose >= 1:
        print(f"\nAnalyzing {len(history_rows)} versions for table '{table_name}'...")
    
    # First, get data files from reading the table at each version
    for idx, row in enumerate(history_rows):
        version = row.version
        
        try:
            version_df = spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(table_path)
            
            file_paths = version_df.select(
                input_file_name().alias("file_path")
            ).distinct().collect()
            
            for fp in file_paths:
                file_name = fp.file_path.split('/')[-1]
                
                versions_with_files.append(Row(
                    version=int(version),
                    timestamp=row.timestamp,
                    operation=str(row.operation) if row.operation else "UNKNOWN",
                    userName=str(row.userName) if row.userName else "UNKNOWN",
                    file_name=file_name
                ))
            
            if verbose >= 1 and (idx + 1) % 10 == 0:
                print(f"  Processed {idx + 1}/{len(history_rows)} versions...")
                
        except Exception as e:
            if verbose >= 2:
                print(f"  Warning: Could not read version {version}: {e}")
            continue
    
    # Now read transaction logs to map deletion vectors to specific versions
    if verbose >= 1:
        print(f"\nScanning Delta transaction logs for deletion vectors...")
    
    # Build a map of deletion vectors found in filesystem
    dv_files_in_fs = {row.name for row in files_df.filter(col("file_type") == "deletion_vector").collect()}
    
    if verbose >= 2:
        print(f"  Found {len(dv_files_in_fs)} deletion vector files in filesystem")
    
    # Parse transaction log files
    dv_count_by_version = {}
    
    try:
        delta_log_path = f"{table_path}/_delta_log"
        log_files = notebookutils.fs.ls(delta_log_path)
        
        # Build a map of version -> (timestamp, operation, userName) for versions we're analyzing
        version_metadata = {
            r.version: (r.timestamp, r.operation, r.userName) 
            for r in history_rows
        }
        
        for log_file in sorted(log_files, key=lambda x: x.name):
            if log_file.name.endswith('.json'):
                try:
                    # Extract version number from filename (e.g., 00000000000000000002.json -> 2)
                    version_num = int(log_file.name.replace('.json', '').lstrip('0') or '0')
                    
                    # Only process versions we're analyzing
                    if version_num not in version_metadata:
                        continue
                    
                    timestamp, operation, userName = version_metadata[version_num]
                    
                    # Read the log file content
                    try:
                        log_content = notebookutils.fs.head(log_file.path, 2000000)  # Increased size
                    except:
                        # If file is too large, read what we can
                        log_content = notebookutils.fs.head(log_file.path, 1000000)
                    
                    dv_files_this_version = set()
                    
                    for line in log_content.split('\n'):
                        if line.strip():
                            try:
                                log_entry = json.loads(line)
                                
                                # Look for add actions with deletion vectors
                                if 'add' in log_entry and log_entry['add']:
                                    add_entry = log_entry['add']
                                    
                                    # Check for deletion vector reference
                                    if 'deletionVector' in add_entry and add_entry['deletionVector']:
                                        dv_info = add_entry['deletionVector']
                                        
                                        # Try to extract the deletion vector filename
                                        # The pathOrInlineDv might be a base64-encoded UUID or a path
                                        if 'pathOrInlineDv' in dv_info:
                                            path_or_inline = dv_info['pathOrInlineDv']
                                            
                                            # Check if it looks like a UUID (with hyphens)
                                            if isinstance(path_or_inline, str):
                                                # Try to match with filesystem files
                                                # The path might be just the UUID or a relative path
                                                potential_filename = f"deletion_vector_{path_or_inline}.bin"
                                                
                                                if potential_filename in dv_files_in_fs:
                                                    dv_files_this_version.add(potential_filename)
                                                else:
                                                    # Try extracting UUID from path if it's a path
                                                    if '/' in path_or_inline:
                                                        uuid_part = path_or_inline.split('/')[-1]
                                                        potential_filename = f"deletion_vector_{uuid_part}.bin"
                                                        if potential_filename in dv_files_in_fs:
                                                            dv_files_this_version.add(potential_filename)
                                                    # Try as-is if it already looks like a filename
                                                    elif path_or_inline.startswith('deletion_vector_') and path_or_inline.endswith('.bin'):
                                                        if path_or_inline in dv_files_in_fs:
                                                            dv_files_this_version.add(path_or_inline)
                                        
                            except (json.JSONDecodeError, KeyError, TypeError) as e:
                                continue
                    
                    # Add the deletion vectors we found for this version
                    if dv_files_this_version:
                        dv_count_by_version[version_num] = len(dv_files_this_version)
                        for dv_file in dv_files_this_version:
                            versions_with_files.append(Row(
                                version=int(version_num),
                                timestamp=timestamp,
                                operation=str(operation) if operation else "UNKNOWN",
                                userName=str(userName) if userName else "UNKNOWN",
                                file_name=dv_file
                            ))
                    
                except (ValueError, AttributeError) as e:
                    if verbose >= 2:
                        print(f"  Could not parse log file {log_file.name}: {e}")
                    continue
        
        if verbose >= 1:
            total_dvs = sum(dv_count_by_version.values())
            print(f"  Found {total_dvs} deletion vectors from transaction logs")
            if verbose >= 2 and dv_count_by_version:
                print(f"  Deletion vectors per version: {dv_count_by_version}")
                    
    except Exception as e:
        if verbose >= 1:
            print(f"  Warning: Could not fully scan Delta log: {e}")
    
    # Fallback: Map any remaining unmatched deletion vectors by modification time
    mapped_dvs = {row.file_name for row in versions_with_files 
                  if row.file_name.startswith('deletion_vector_')}
    unmapped_dvs = dv_files_in_fs - mapped_dvs
    
    if unmapped_dvs and verbose >= 1:
        print(f"\n  {len(unmapped_dvs)} deletion vectors not found in logs, mapping by timestamp...")
    
    if unmapped_dvs:
        dv_files_list = files_df.filter(col("file_type") == "deletion_vector").collect()
        dv_by_name = {row.name: row for row in dv_files_list}
        
        for dv_name in unmapped_dvs:
            if dv_name in dv_by_name:
                dv_file_row = dv_by_name[dv_name]
                dv_modtime = dv_file_row.modifyTime
                
                # Find closest version by timestamp
                min_diff = float('inf')
                closest_version = None
                
                for hist_row in history_rows:
                    diff = abs((dv_modtime - hist_row.timestamp).total_seconds())
                    if diff < min_diff:
                        min_diff = diff
                        closest_version = hist_row
                
                if closest_version:
                    versions_with_files.append(Row(
                        version=int(closest_version.version),
                        timestamp=closest_version.timestamp,
                        operation=str(closest_version.operation) if closest_version.operation else "UNKNOWN",
                        userName=str(closest_version.userName) if closest_version.userName else "UNKNOWN",
                        file_name=dv_name
                    ))
                    if verbose >= 2:
                        print(f"    Mapped {dv_name} to version {closest_version.version} (time diff: {min_diff:.1f}s)")
    
    if verbose >= 1:
        print(f"\n✓ Complete! Found {len(versions_with_files)} file-version associations")
    
    if not versions_with_files:
        if verbose >= 1:
            print("No file associations found")
        empty_schema = "version INT, message STRING"
        empty_df = spark.createDataFrame([], empty_schema)
        return empty_df, empty_df, empty_df, empty_df
    
    version_files_df = spark.createDataFrame(versions_with_files)
    
    # Remove duplicates (same file might be referenced multiple times)
    version_files_df = version_files_df.distinct()
    
    # Join with file listing to get sizes and file types
    detailed_df = version_files_df.alias("v").join(
        files_df.alias("f"),
        col("v.file_name") == col("f.name"),
        "inner"
    ).select(
        col("v.version"),
        col("v.timestamp").alias("transaction_time"),
        col("v.operation"),
        col("v.userName"),
        col("v.file_name"),
        col("f.size_kb"),
        col("f.modifyTime").alias("file_modified_time"),
        col("f.file_type")
    ).orderBy(col("version").desc(), col("file_name"))
    
    # Show sample of files only in verbose mode 2
    if verbose >= 2:
        print(f"\nSample of matched files:")
        detailed_df.select("version", "file_name", "file_type", "size_kb").show(10, truncate=False)
        
        # Show file counts per version
        print(f"\nFiles per version:")
        detailed_df.groupBy("version", "file_type").count().orderBy("version", "file_type").show(50, truncate=False)
    
    # Create summary of all files per version
    summary_df = detailed_df.groupBy(
        "version", "transaction_time", "operation", "userName"
    ).agg(
        count("file_name").alias("num_files"),
        sql_round(sum("size_kb"), 2).alias("total_size_kb"),
        sql_round(sum("size_kb") / count("file_name"), 2).alias("avg_file_size_kb"),
        sql_round(min("size_kb"), 2).alias("min_file_size_kb"),
        sql_round(max("size_kb"), 2).alias("max_file_size_kb"),
        count(when(col("file_type") == "data", 1)).alias("num_data_files"),
        count(when(col("file_type") == "deletion_vector", 1)).alias("num_deletion_vectors"),
        sql_round(sum(when(col("file_type") == "data", col("size_kb")).otherwise(0)), 2).alias("data_size_kb"),
        sql_round(sum(when(col("file_type") == "deletion_vector", col("size_kb")).otherwise(0)), 2).alias("deletion_vector_size_kb")
    ).orderBy(col("version").desc())
    
    # Identify new files by comparing each version to its immediate predecessor
    all_versions = sorted([r.version for r in history_rows], reverse=True)
    
    if verbose >= 1:
        print(f"\nIdentifying new files added in each version...")
    
    # Collect file sets per version for comparison
    version_file_sets = {}
    for version in all_versions:
        files_in_version = set(
            [row.file_name for row in 
             detailed_df.filter(col("version") == version).select("file_name").distinct().collect()]
        )
        version_file_sets[version] = files_in_version
    
    # Build list of (version, file_name) tuples for NEW files only
    new_file_records = []
    
    for i, version in enumerate(all_versions):
        current_files = version_file_sets[version]
        
        if i < len(all_versions) - 1:
            # Compare to immediate previous version
            prev_version = all_versions[i + 1]
            prev_files = version_file_sets[prev_version]
            
            # NEW files = in current but NOT in previous
            new_files_set = current_files - prev_files
        else:
            # First version - all files are new
            new_files_set = current_files
        
        # Record each new file with its version
        for file_name in new_files_set:
            new_file_records.append(Row(version=version, file_name=file_name))
        
        if verbose >= 1 and (i + 1) % 10 == 0:
            print(f"  Processed {i + 1}/{len(all_versions)} versions...")
    
    if verbose >= 1:
        print(f"✓ New files identification complete!")
    
    # Create DataFrame of new files and join with detailed info
    if new_file_records:
        new_files_marker_df = spark.createDataFrame(new_file_records)
        
        # Join with detailed_df to get all the metadata
        new_files_df = new_files_marker_df.join(
            detailed_df,
            ["version", "file_name"],
            "inner"
        ).orderBy(col("version").desc(), col("file_name"))
    else:
        new_files_df = spark.createDataFrame([], detailed_df.schema)
    
    # Show sample of new files only in verbose mode 2
    if verbose >= 2:
        print(f"\nSample of new files per version:")
        new_files_df.select("version", "file_name", "file_type", "size_kb").show(10, truncate=False)
    
    # Create summary of only new files per version
    new_files_summary_df = new_files_df.groupBy(
        "version", "transaction_time", "operation", "userName"
    ).agg(
        count("file_name").alias("num_new_files"),
        sql_round(sum("size_kb"), 2).alias("total_new_size_kb"),
        sql_round(sum("size_kb") / count("file_name"), 2).alias("avg_new_file_size_kb"),
        sql_round(min("size_kb"), 2).alias("min_new_file_size_kb"),
        sql_round(max("size_kb"), 2).alias("max_new_file_size_kb"),
        count(when(col("file_type") == "data", 1)).alias("num_new_data_files"),
        count(when(col("file_type") == "deletion_vector", 1)).alias("num_new_deletion_vectors"),
        sql_round(sum(when(col("file_type") == "data", col("size_kb")).otherwise(0)), 2).alias("new_data_size_kb"),
        sql_round(sum(when(col("file_type") == "deletion_vector", col("size_kb")).otherwise(0)), 2).alias("new_deletion_vector_size_kb")
    ).orderBy(col("version").desc())
    
    return detailed_df, summary_df, new_files_df, new_files_summary_df


# Updated usage function with verbose parameter
def analyze_table_with_io(
    table_name: str, 
    base_path: str = "Tables", 
    max_versions: Optional[int] = None,
    verbose: Union[bool, int] = 1
):
    """
    Convenience function to analyze a table and display all 4 dataframes.
    """
    detailed_df, summary_df, new_files_df, new_files_summary_df = \
        get_delta_files_with_versions_enhanced(table_name, base_path, max_versions, verbose)
    
    # Only show tables if verbose > 0
    if verbose > 0:
        print("\n" + "="*80)
        print("1. DETAILED VIEW - All Files Per Version (with file type)")
        print("="*80)
        detailed_df.show(50, truncate=False)
        
        print("\n" + "="*80)
        print("2. SUMMARY - All Files Aggregated Per Version")
        print("="*80)
        summary_df.show(50, truncate=False)
        
        print("\n" + "="*80)
        print("3. NEW FILES - Only Files Added in Each Version")
        print("="*80)
        new_files_df.show(50, truncate=False)
        
        print("\n" + "="*80)
        print("4. NEW FILES SUMMARY - Written IO Per Version (ACTUAL writes, not cumulative)")
        print("="*80)
        new_files_summary_df.show(50, truncate=False)
    
    return detailed_df, summary_df, new_files_df, new_files_summary_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Generating Data')
# Create initial DataFrame with IDs
df = spark.range(num_rows).toDF("customer_id")
# Add all the fake data columns
df = df.withColumn("first_name", gen_first_name()) \
    .withColumn("last_name", gen_last_name()) \
    .withColumn("email", gen_email()) \
    .withColumn("phone", gen_phone()) \
    .withColumn("address", gen_address()) \
    .withColumn("city", gen_city()) \
    .withColumn("state", gen_state()) \
    .withColumn("country", gen_country()) \
    .withColumn("postal_code", gen_postal_code()) \
    .withColumn("company", gen_company()) \
    .withColumn("job_title", gen_job()) \
    .withColumn("account_balance", gen_balance()) \
    .withColumn("credit_score", gen_credit_score()) \
    .withColumn("membership_tier", gen_tier())

# Show results
print(f"Total records: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write \
.format("delta") \
.mode("overwrite") \
.option("delta.enableChangeDataFeed", "true") \
.saveAsTable(initial_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write \
.format("delta") \
.mode("overwrite") \
.option("delta.enableChangeDataFeed", "true") \
.option('delta.enableDeletionVectors','true') \
.saveAsTable(dv_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# should re-write below the insert statement gets re-evalued and thus increases volume on second merge


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Generating Changes')

source_table = spark.read.format("delta").table(initial_table)
current_data = source_table.filter(col("customer_id") % modulo == 0)

altered_data = current_data.select(
"customer_id",
"first_name",  # Keep original
"last_name",   # Keep original
"address",     # Keep original
"city",        # Keep original
"state",       # Keep original
"country",     # Keep original
"postal_code", # Keep original
"company",     # Keep original
"job_title"    # Keep original
) \
.withColumn("email", gen_email()) \
.withColumn("phone", gen_phone()) \
.withColumn("account_balance", gen_balance()) \
.withColumn("credit_score", gen_credit_score()) \
.withColumn("membership_tier", gen_tier())
print('generated updates')

# Add new rows
max_id = source_table.agg({"customer_id": "max"}).collect()[0][0]

new_rows = spark.range(max_id + 1, max_id + extra_rows + 1).toDF("customer_id") \
    .withColumn("first_name", gen_first_name()) \
    .withColumn("last_name", gen_last_name()) \
    .withColumn("email", gen_email()) \
    .withColumn("phone", gen_phone()) \
    .withColumn("address", gen_address()) \
    .withColumn("city", gen_city()) \
    .withColumn("state", gen_state()) \
    .withColumn("country", gen_country()) \
    .withColumn("postal_code", gen_postal_code()) \
    .withColumn("company", gen_company()) \
    .withColumn("job_title", gen_job()) \
    .withColumn("account_balance", gen_balance()) \
    .withColumn("credit_score", gen_credit_score()) \
    .withColumn("membership_tier", gen_tier())

# Combine updates and inserts
merge_data = altered_data.unionByName(new_rows, allowMissingColumns=True)

print(f'Total rows to merge: {merge_data.count()}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the DeltaTable object for the target table
deltaTable = DeltaTable.forName(spark, initial_table)

# Perform merge - CORRECTED
deltaTable.alias("target").merge(
    merge_data.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

print("First Merge completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"DELETE FROM {initial_table} WHERE customer_id % {modulo} = 1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the DeltaTable object for the target table
deltaTable = DeltaTable.forName(spark, dv_table)

# Perform merge - CORRECTED
deltaTable.alias("target").merge(
    merge_data.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

print("Second Merge completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"DELETE FROM {dv_table} WHERE customer_id % {modulo} = 1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

detailed, summary, new_files, new_summary = get_delta_files_with_versions_enhanced(
    initial_table, 
    max_versions=20,
    verbose=1
)

detailed_dv, summary_dv, new_files_dv, new_summary_dv = get_delta_files_with_versions_enhanced(
    dv_table, 
    max_versions=20,
    verbose=1
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(new_summary)
display(new_summary_dv)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
