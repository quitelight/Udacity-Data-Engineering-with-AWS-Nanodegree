import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1689494412188 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1689494412188",
)

# Script generated for node Join
Join_node1689494479563 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1689494412188,
    keys1=["serialNumber"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1689494479563",
)

# Script generated for node Machine Learning Curtated
MachineLearningCurtated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1689494479563,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://krishnastedi/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurtated_node3",
)

job.commit()
