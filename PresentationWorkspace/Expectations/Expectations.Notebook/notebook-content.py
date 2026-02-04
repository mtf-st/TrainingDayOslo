# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "038fb5ea-a9f8-9c9a-4304-9c196bf1f15d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": [
# META         {
# META           "id": "b83a71b0-7434-4028-a380-abd328e5ed2a"
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

import great_expectations as gx
context = gx.get_context()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM `FabFeb-Preso`.`WideWorldImporters-Standard`.Sales.Invoices")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expectation_suite = "WWI_Validation"
context.suites.add(gx.ExpectationSuite(name=expectation_suite))
suite = context.suites.get(name=expectation_suite)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_source = context.data_sources.add_spark(name="test_data_source")
data_asset = data_source.add_dataframe_asset(name="test_data")

batch_definition = data_asset.add_batch_definition_whole_dataframe("test_batch_definition")

validation_definition = gx.ValidationDefinition(
    data = batch_definition,
    suite = suite,
    name = "test_validation_definition",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# https://greatexpectations.io/expectations/ has so many

# CELL ********************

unique_id = gx.expectations.ExpectColumnValuesToBeUnique(column="InvoiceID")
suite.add_expectation(unique_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = validation_definition.run(batch_parameters = {"dataframe": df})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(results)

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
