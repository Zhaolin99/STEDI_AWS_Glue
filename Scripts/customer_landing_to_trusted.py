import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1698697495264 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1698697495264",
)

# Script generated for node Filter
Filter_node1698275941117 = Filter.apply(
    frame=customer_landing_node1698697495264,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1698275941117",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1698697291576 = DynamicFrame.fromDF(
    Filter_node1698275941117.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1698697291576",
)

# Script generated for node customer_trusted
customer_trusted_node1698275946604 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1698697291576,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_node1698275946604",
)

job.commit()
