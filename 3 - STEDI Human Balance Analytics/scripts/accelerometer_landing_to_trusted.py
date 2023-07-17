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

# Script generated for node Raw accelerometer data
Rawaccelerometerdata_node1689488592659 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Rawaccelerometerdata_node1689488592659",
)

# Script generated for node Trusted customer data
Trustedcustomerdata_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Trustedcustomerdata_node1",
)

# Script generated for node Join
Join_node1689488753230 = Join.apply(
    frame1=Trustedcustomerdata_node1,
    frame2=Rawaccelerometerdata_node1689488592659,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689488753230",
)

# Script generated for node Drop Fields
DropFields_node1689488947247 = DropFields.apply(
    frame=Join_node1689488753230,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1689488947247",
)

# Script generated for node Load to Trusted Zone
LoadtoTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689488947247,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://krishnastedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="LoadtoTrustedZone_node3",
)

job.commit()
