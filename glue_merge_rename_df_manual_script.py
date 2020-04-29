import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql.functions import col, asc
from awsglue.transforms import Join

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "coviddb", table_name = "curated_covcovid_datasets", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "coviddb", table_name = "curated_covcovid_datasets", transformation_ctx = "datasource0")

## @type: ApplyMapping
## @args: [mapping = [("col0", "string", "Provincia", "string"), ("col1", "string", "Estado", "string"), ("col2", "string", "Lat", "string"), ("col3", "string", "Longitud", "string"), ("col4", "string", "Valor", "string"), ("col5", "string", "Region", "string"), ("col6", "string", "TipoCaso", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col0", "string", "Provincia", "string"),
 ("col1", "string", "Lat", "string"), ("col2", "string", "Lon", "string"), ("col3", "string", "Fecha", "string"), 
 ("col4", "string", "Valor", "string"), ("col5", "string", "Region", "string"), ("col6", "string", "TipoCaso", "string")],
  transformation_ctx = "applymapping1")

# MY CODE
df = applymapping1.toDF().withColumn("id",  monotonically_increasing_id())
df = df.filter(col("id")<=2)

df2 = glueContext.create_dynamic_frame.from_catalog(database = "coviddb", table_name = "cur_economic_merge", transformation_ctx = "df2")
df2 = df2.toDF()

df = df.join(df2, df.Fecha == df2.date, 'inner')


applymapping1 = applymapping1.fromDF(df, glueContext, "datasource2")

# END MY CODE

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://cmurill5production/global/CovidVsEconomic"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", 
	connection_options = {"path": "s3://cmurill5production/global/CovidVsEconomic"}, format = "parquet", transformation_ctx = "datasink2")
job.commit()

#df = applymapping1.toDF().withColumn("id", applymapping1.toDF().id.cast(IntegerType()))