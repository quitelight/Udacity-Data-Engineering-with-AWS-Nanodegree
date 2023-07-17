import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Load Raw Data
LoadRawData_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="LoadRawData_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1689487576305 = Filter.apply(
    frame=LoadRawData_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1689487576305",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1689487576305,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://krishnastedi/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node3",
)

job.commit()
