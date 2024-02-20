import sys
import boto3
import psycopg2
from awsglue.job import Job
from pyspark.sql.types import *
from awsglue.transforms import *
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def encrypt_with_kms(value):
    # Initialize AWS KMS client
    kms_client = boto3.client('kms', region_name='<KMS-Key-Region>')
    # Encrypt the value using the KMS key
    response = kms_client.encrypt(KeyId='<KMS-Key-ID>', Plaintext=value.encode())
    return response['CiphertextBlob']

df = spark.read.option("header",True).csv("s3://<S3-Bucket-Path>/SampleData.csv")

df.show()

# Register the UDF
encrypt_udf = udf(encrypt_with_kms, BinaryType())

encryted_df = df.withColumn("encrypted_PII_Data", encrypt_udf("PII_Data"))

encryted_df.show()

rds_jdbc_url = "<RDS-Postgres-JDBC-URL>"

properties = {
    "user": "<Username>",
    "password": "<Password>",
    "driver": "org.postgresql.Driver"
}

rds_table_name = "<RDS-Table-Name>"

encryted_df.select("data_a","data_b","encrypted_PII_Data").withColumnRenamed("encrypted_PII_Data","pii_data").write.jdbc(rds_jdbc_url, rds_table_name, mode="append", properties=properties)

job.commit()