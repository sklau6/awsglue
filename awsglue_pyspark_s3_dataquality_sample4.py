from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize the GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Create a Glue DynamicFrame using MongoDB as a source
mongo_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type = "mongodb", 
    connection_options = {
        "uri": "mongodb://username:password@host:port/database",
        "collection": "your_collection"
    }
)

# Write DynamicFrame to S3
glueContext.write_dynamic_frame.from_options(
    frame = mongo_dynamic_frame, 
    connection_type = "s3", 
    connection_options = {
        "path": "s3://your-bucket/your-path/"
    }
)

# Transform DynamicFrame as required
# For example, let's assume we are filtering rows where "age" is greater than 20
filtered_dynamic_frame = Filter.apply(frame = mongo_dynamic_frame, f = lambda x: x["age"] > 20)

# Write the transformed DynamicFrame to Snowflake
glueContext.write_dynamic_frame.from_options(
    frame = filtered_dynamic_frame, 
    connection_type = "snowflake", 
    connection_options = {
        "sfDatabase": "your_database",
        "sfWarehouse": "your_warehouse",
        "sfRole": "your_role",
        "sfSchema": "your_schema",
        "sfRole": "your_table",
        "sfURL": "your_snowflake_url"
    },
    redshift_tmp_dir = "s3://path/for/tempdata/"
)

