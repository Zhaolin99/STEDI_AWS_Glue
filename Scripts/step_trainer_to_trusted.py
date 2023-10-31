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

# Script generated for node step_trainer_landing
step_trainer_landing_node1698701884380 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-human-balance",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1698701884380",
)

# Script generated for node customer_curated_distinct
customer_curated_distinct_node1698701883843 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi-human-balance",
        table_name="customer_curated_distinct",
        transformation_ctx="customer_curated_distinct_node1698701883843",
    )
)

# Script generated for node SQL Query
SqlQuery900 = """
select * from s
JOIN c on c.serialnumber = s.serialnumber


"""
SQLQuery_node1698701946055 = sparkSqlQuery(
    glueContext,
    query=SqlQuery900,
    mapping={
        "c": customer_curated_distinct_node1698701883843,
        "s": step_trainer_landing_node1698701884380,
    },
    transformation_ctx="SQLQuery_node1698701946055",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1698702019272 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1698701946055,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1698702019272",
)

job.commit()
