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

# Script generated for node customer_trusted
customer_trusted_node1698698724046 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1698698724046",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1698698725216 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1698698725216",
)

# Script generated for node JOIN_Query
SqlQuery702 = """
select * from c
JOIN a ON c.email = a.user
"""
JOIN_Query_node1698698753128 = sparkSqlQuery(
    glueContext,
    query=SqlQuery702,
    mapping={
        "a": accelerometer_trusted_node1698698725216,
        "c": customer_trusted_node1698698724046,
    },
    transformation_ctx="JOIN_Query_node1698698753128",
)

# Script generated for node customer_curated
customer_curated_node1698698867899 = glueContext.write_dynamic_frame.from_options(
    frame=JOIN_Query_node1698698753128,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1698698867899",
)

job.commit()
