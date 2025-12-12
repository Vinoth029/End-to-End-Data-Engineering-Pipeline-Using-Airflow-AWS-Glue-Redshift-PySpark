from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pydeequ.checks import *
from pydeequ.verification import *
import boto3, json, sys

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3_input = sys.argv[1]
run_id = sys.argv[2]

df = spark.read.parquet(s3_input)

check = Check(spark, CheckLevel.Error, "dq_checks") \
    .hasSize(lambda x: x > 0) \
    .isComplete("customer_id") \
    .isUnique("customer_id") \
    .isContainedIn("status", ["A", "I"])

result = VerificationSuite(spark).onData(df).addCheck(check).run()
passed = all([x.status == "Success" for x in result.checkResults.values()])

report = {"passed": passed, "details": str(result.checkResults)}

s3 = boto3.client('s3')
s3.put_object(
    Bucket='ingest-bucket',
    Key=f'quality_results/{run_id}/quality_report.json',
    Body=json.dumps(report)
)
