import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    """
    Helper function to run a Spark SQL query over one or more DynamicFrames.
    """
    spark = glueContext.spark_session

    # Register each DynamicFrame as a temp view with the given alias
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)

    # Run SQL and convert result back to DynamicFrame
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


# Glue job bootstrap
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read accelerometer_trusted from S3 (Trusted Zone)
accelerometer_trustedS3source = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        # your trusted accelerometer path
        "paths": ["s3://stedi-hk-lakehouse/trusted/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trustedS3source",
)

# Read customer_trusted from S3 (Trusted Zone)
customer_trustedS3source = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        # your trusted customer path
        "paths": ["s3://stedi-hk-lakehouse/trusted/customer/"],
        "recurse": True,
    },
    transformation_ctx="customer_trustedS3source",
)

# SQL: keep only customers who have accelerometer data
SqlQuery_customers_curated = """
SELECT DISTINCT c.*
FROM customer_trusted c
INNER JOIN accelerometer_trusted a
    ON c.email = a.user
"""

customers_curated_dyf = sparkSqlQuery(
    glueContext,
    query=SqlQuery_customers_curated,
    mapping={
        # aliases used in the SQL above
        "customer_trusted": customer_trustedS3source,
        "accelerometer_trusted": accelerometer_trustedS3source,
    },
    transformation_ctx="customers_curated_dyf",
)


# Write customers_curated to Curated Zone and update Data Catalog
customers_curated_sink = glueContext.getSink(
    path="s3://stedi-hk-lakehouse/curated/customers/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customers_curated_sink",
)

# Glue database / table name
customers_curated_sink.setCatalogInfo(
    catalogDatabase="stedi",              
    catalogTableName="customers_curated",
)

customers_curated_sink.setFormat("json")

customers_curated_sink.writeFrame(customers_curated_dyf)

job.commit()