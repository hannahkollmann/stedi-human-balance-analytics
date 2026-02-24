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

# step_trainer_trusted S3 source (Trusted Zone)
step_trainer_trustedS3source = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/trusted/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trustedS3source",
)

# accelerometer_trusted S3 source (Trusted Zone)
accelerometer_trustedS3source = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hk-lakehouse/trusted/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trustedS3source",
)

# machine_learning filter curated
#    Join step_trainer_trusted and accelerometer_trusted
#    on timestamp/sensorreadingtime
SqlQuery_machine_learning = """
SELECT DISTINCT *
FROM step_trainer_trusted
INNER JOIN accelerometer_trusted
    ON accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime;
"""

machine_learningfiltercurated = sparkSqlQuery(
    glueContext,
    query=SqlQuery_machine_learning,
    mapping={
        "accelerometer_trusted": accelerometer_trustedS3source,
        "step_trainer_trusted": step_trainer_trustedS3source,
    },
    transformation_ctx="machine_learningfiltercurated",
)

# machine_learning_curated S3 target (Curated Zone)
machine_learning_curatedS3target = glueContext.getSink(
    path="s3://stedi-hk-lakehouse/curated/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curatedS3target",
)

machine_learning_curatedS3target.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="machine_learning_curated",
)

machine_learning_curatedS3target.setFormat("json")
machine_learning_curatedS3target.writeFrame(
    machine_learningfiltercurated
)

job.commit()
