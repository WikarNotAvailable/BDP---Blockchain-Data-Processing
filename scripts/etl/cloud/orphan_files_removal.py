import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = (
    SparkSession.builder.appName("OrphanFilesRemoval")    
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bdp-wallets-aggregations/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()
)

glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sql("CALL glue_catalog.system.remove_orphan_files(table => 'bdp.cleaned_transactions')")
spark.sql("CALL glue_catalog.system.remove_orphan_files(table => 'bdp.wallets_aggregations')")
spark.sql("CALL glue_catalog.system.remove_orphan_files(table => 'bdp.features')")

job.commit()