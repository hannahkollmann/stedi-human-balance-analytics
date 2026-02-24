import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    # Register each DynamicFrame as a temporary view, then run the SQL
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


# Glue boilerplate 
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read accelerometer landing data from S3 
accelerometer_landingS3source_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/landing/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landingS3source_node",
)

# Read customer_trusted data from S3 
customer_trustedS3source_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/trusted/customer/"],
        "recurse": True,
    },
    transformation_ctx="customer_trustedS3source_node",
)

# Join: keep only accelerometer records for trusted customers 
SqlQuery23 = """
SELECT  accel.*
FROM    accelerometer_landing AS accel
INNER JOIN customer_trusted    AS cust
        ON cust.email = accel.user
"""

accelerometerfiltertrusted_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery23,
    mapping={
        "customer_trusted": customer_trustedS3source_node,
        "accelerometer_landing": accelerometer_landingS3source_node,
    },
    transformation_ctx="accelerometerfiltertrusted_node",
)

# Write accelerometer_trusted to S3 and update Glue Catalog 
accelerometer_trustedS3target_node = glueContext.getSink(
    path="s3://stedi-hk-lakehouse/trusted/accelerometer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trustedS3target_node",
)

accelerometer_trustedS3target_node.setCatalogInfo(
    catalogDatabase="stedi",          # your Glue database
    catalogTableName="accelerometer_trusted",  # new trusted table
)

accelerometer_trustedS3target_node.setFormat("json")

accelerometer_trustedS3target_node.writeFrame(
    accelerometerfiltertrusted_node
)

job.commit()