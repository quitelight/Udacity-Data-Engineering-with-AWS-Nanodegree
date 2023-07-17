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

# Script generated for node Accelerometer Data
AccelerometerData_node1689491512925 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerData_node1689491512925",
)

# Script generated for node Customer Data (trusted zone)
CustomerDatatrustedzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerDatatrustedzone_node1",
)

# Script generated for node Join
Join_node1689492338574 = Join.apply(
    frame1=CustomerDatatrustedzone_node1,
    frame2=AccelerometerData_node1689491512925,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689492338574",
)

# Script generated for node Drop Fields
DropFields_node1689492509099 = DropFields.apply(
    frame=Join_node1689492338574,
    paths=["timeStamp", "user", "x", "y", "z"],
    transformation_ctx="DropFields_node1689492509099",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689492509099,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://krishnastedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedZone_node3",
)

job.commit()
