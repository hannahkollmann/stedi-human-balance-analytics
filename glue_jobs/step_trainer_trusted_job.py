import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Step Trainer landing S3 source  (Landing Zone)
step_trainer_landingS3source = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/landing/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landingS3source",
)

# Customers curated S3 source  (Curated Zone)
#    customers_curated = customers who:
#      - have accelerometer data
#      - agreed to share data
customers_curatedS3source = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/curated/customers/"],
        "recurse": True,
    },
    transformation_ctx="customers_curatedS3source",
)

# Filter Step Trainer records to only customers_curated
#    Join on serialnumber
SqlQuery_step_trainer_trusted = """
SELECT DISTINCT st.*
FROM step_trainer_landing st
INNER JOIN customers_curated c
    ON c.serialnumber = st.serialnumber;
"""

step_trainerfiltertrusted = sparkSqlQuery(
    glueContext,
    query=SqlQuery_step_trainer_trusted,
    mapping={
        "customers_curated": customers_curatedS3source,
        "step_trainer_landing": step_trainer_landingS3source,
    },
    transformation_ctx="step_trainerfiltertrusted",
)

# Write Step Trainer Trusted to S3 + Glue Catalog
step_trainer_trustedS3target = glueContext.getSink(
    path="s3://stedi-hk-lakehouse/trusted/step_trainer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trustedS3target",
)

step_trainer_trustedS3target.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trustedS3target.setFormat("json")

step_trainer_trustedS3target.writeFrame(step_trainerfiltertrusted)

job.commit()