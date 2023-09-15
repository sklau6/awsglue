import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize contexts and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue Data Catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_database",
    table_name="my_glue_table",
    transformation_ctx="datasource0")

# Perform transformations
# Here we rename a column from "col0" to "column0" and filter rows where "col1" > 0
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[("col0", "int", "column0", "int"), ("col1", "int", "col1", "int")],
    transformation_ctx="applymapping1")

filter1 = Filter.apply(
    frame=applymapping1,
    f=lambda x: x["col1"] > 0,
    transformation_ctx="filter1")

# Write the transformed data back to S3
datasink2 = glueContext.write_dynamic_frame.from_options(
    frame=filter1,
    connection_type="s3",
    connection_options={"path": "s3://my-destination-bucket/transformed-data/"},
    format="parquet",
    transformation_ctx="datasink2")

# Job commit
job.commit()
