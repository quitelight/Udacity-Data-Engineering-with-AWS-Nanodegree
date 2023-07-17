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

# Script generated for node Customers Curated
CustomersCurated_node1689493304245 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1689493304245",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://krishnastedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1689494901252 = ApplyMapping.apply(
    frame=CustomersCurated_node1689493304245,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "right_shareWithPublicAsOfDate",
            "bigint",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "right_shareWithResearchAsOfDate",
            "bigint",
        ),
        ("registrationDate", "bigint", "right_registrationDate", "bigint"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "right_lastUpdateDate", "bigint"),
        ("phone", "string", "right_phone", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "right_shareWithFriendsAsOfDate",
            "bigint",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1689494901252",
)

# Script generated for node Join
Join_node1689493336409 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforJoin_node1689494901252,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1689493336409",
)

# Script generated for node Drop Fields
DropFields_node1689493648170 = DropFields.apply(
    frame=Join_node1689493336409,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_shareWithPublicAsOfDate",
        "right_shareWithResearchAsOfDate",
        "right_registrationDate",
        "right_customerName",
        "right_email",
        "right_lastUpdateDate",
        "right_shareWithFriendsAsOfDate",
        "right_phone",
    ],
    transformation_ctx="DropFields_node1689493648170",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689493648170,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://krishnastedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
