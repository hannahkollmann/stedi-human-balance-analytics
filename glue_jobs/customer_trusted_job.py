import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame 

# Helper so I can run a SQL query over DynamicFrames
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    spark = glueContext.spark_session
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

# Read the customer landing data directly from S3 (JSON format)
customer_landing_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/landing/customer/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_dyf",
)

# Filter to customers who agreed to share data for research
sql_query = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
"""

customer_trusted_dyf = sparkSqlQuery(
    glueContext,
    query=sql_query,
    mapping={"customer_landing": customer_landing_dyf},
    transformation_ctx="customer_trusted_dyf",
)

# Write trusted customers to S3 and update the Glue catalog table
sink = glueContext.getSink(
    path="s3://stedi-hk-lakehouse/trusted/customer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE", 
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink",
)

sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customer_trusted",
)

sink.setFormat("json")
sink.writeFrame(customer_trusted_dyf)

job.commit()