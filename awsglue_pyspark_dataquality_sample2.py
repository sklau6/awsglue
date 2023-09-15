import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_extract
import re

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

## Read data from Glue Data Catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "your_database", table_name = "your_table")
df = datasource0.toDF()

## Data Quality Checks

# 1. Age should be between 18 and 65
df_filtered_age = df.filter((col("age") >= 18) & (col("age") <= 65))

# 2. Email should have a correct format
email_pattern = '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
df_filtered = df_filtered_age.filter(regexp_extract(col("email"), email_pattern, 0) != "")

## Convert back to DynamicFrame
dynamic_frame_write = DynamicFrame.fromDF(df_filtered, glueContext, "dynamic_frame_write")

## Write back to another table or output
sink = glueContext.write_dynamic_frame.from_options(frame = dynamic_frame_write, connection_type = "s3", connection_options = {"path": "s3://your-output-bucket"}, format = "csv")

job.commit()
