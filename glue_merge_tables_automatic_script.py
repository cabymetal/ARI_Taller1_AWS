import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "coviddb", table_name = "eco_oil", transformation_ctx = "eco_oil_table"]
## @return: eco_oil_table
## @inputs: []
eco_oil_table = glueContext.create_dynamic_frame.from_catalog(database="coviddb", table_name="eco_oil", transformation_ctx = "eco_oil_table")
## @type: DataSource
## @args: [database = "coviddb", table_name = "eco_sand_sandp", transformation_ctx = "eco_oil_table"]
## @return: eco_sand_table
## @inputs: []
eco_sand_table = glueContext.create_dynamic_frame.from_catalog(database="coviddb", table_name="eco_sand_sandp", transformation_ctx = "eco_oil_table")

## @type: ApplyMapping
## @args: [mapping = [("date", "string", "fecha", "string"), ("open", "double", "open_oil", "double"), ("high", "double", "high_oil", "double"), ("low", "double", "low_oil", "double"), ("close", "double", "close_oil", "double"), ("adj close", "double", "adj close_oil", "double"), ("volume", "long", "volume_oil", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = eco_oil_table]
applymapping1 = ApplyMapping.apply(frame = eco_oil_table, mappings = [("date", "string", "fecha", "string"), ("open", "double", "open_oil", "double"), ("high", "double", "high_oil", "double"), ("low", "double", "low_oil", "double"), ("close", "double", "close_oil", "double"), ("adj close", "double", "adj close_oil", "double"), ("volume", "long", "volume_oil", "long")], transformation_ctx = "applymapping1")

## @type: Join
## @args: [keys1 = ["fecha"], keys2 = ["date"]]
## @return: joined_frame
## @inputs: [frame1 = applymapping1, frame2 = eco_sand_table]
joined_frame = Join.apply(frame1 = applymapping1, frame2 = eco_sand_table, keys1 = ["fecha"], keys2 = ["date"], transformation_ctx = "joined_frame")


## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://cmurill5curated/Mundial/Economic_merge"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = joined_frame]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = joined_frame, connection_type = "s3", connection_options = {"path": "s3://cmurill5curated/Mundial/Economic_merge"}, format = "csv", transformation_ctx = "datasink2")
job.commit()