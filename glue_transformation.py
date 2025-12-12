from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3_input = sys.argv[1]
s3_output = sys.argv[2]

df = spark.read.parquet(s3_input)

df_clean = (
    df.withColumn('address', F.trim('address'))
      .withColumn('amount', F.col('amount').cast('double'))
      .withColumn('event_date', F.to_date('event_timestamp'))
)

w = Window.partitionBy("region").orderBy(F.col("event_timestamp").desc())
df_final = df_clean.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

df_final.write.partitionBy("event_date").mode("overwrite").parquet(s3_output)
