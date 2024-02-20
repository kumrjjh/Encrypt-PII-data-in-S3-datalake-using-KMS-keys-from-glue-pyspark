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

def decrypt_data(encrypted_data):
	# Initialize AWS KMS client
    kms_client = boto3.client('kms', region_name='<KMS-Key-Region>')
    # Decrypt the value using the KMS key
    decrypted_data = kms_client.decrypt(CiphertextBlob=encrypted_data)
    return decrypted_data['Plaintext'].decode()

rds_jdbc_url = "<RDS-Postgres-JDBC-URL>"

properties = {
    "user": "<Username>",
    "password": "<Password>",
    "driver": "org.postgresql.Driver"
}

rds_table_name = "<RDS-Table-Name>"

rds_df = spark.read.jdbc(rds_jdbc_url, rds_table_name, properties=properties)

rds_df.show()

decrypt_udf = udf(decrypt_data, StringType())

decrypted_df = rds_df.withColumnRenamed("pii_data","encrypted_PII_Data").withColumn("decrypted_PII_Data", decrypt_udf("encrypted_PII_Data"))

try:
    decrypted_df.show()
except Exception as e:
    print(e)

job.commit()