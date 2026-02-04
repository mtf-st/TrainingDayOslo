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
# META     "mirrored_db": {
# META       "known_mirrored_dbs": [
# META         {
# META           "id": "b83a71b0-7434-4028-a380-abd328e5ed2a"
# META         },
# META         {
# META           "id": "5472382c-944a-44ab-b154-24a31ffbc79b"
# META         }
# META       ]
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

# CELL ********************

df = spark.sql("SELECT * FROM `FabFeb-Preso`.AdventureWorks.SalesLT.Product LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
