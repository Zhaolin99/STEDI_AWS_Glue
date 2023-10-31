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

# Script generated for node customer_trusted
customer_trusted_node1698357566314 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1698357566314",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1698357420636 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1698357420636",
)

# Script generated for node PrivacyJoin
PrivacyJoin_node1698357815662 = Join.apply(
    frame1=customer_trusted_node1698357566314,
    frame2=accelerometer_landing_node1698357420636,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyJoin_node1698357815662",
)

# Script generated for node Drop Fields
DropFields_node1698357963748 = DropFields.apply(
    frame=PrivacyJoin_node1698357815662,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1698357963748",
)

# Script generated for node Amazon S3
AmazonS3_node1698358009330 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698357963748,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698358009330",
)

job.commit()
