import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
LOGGER = glueContext.get_logger()
job = Job(glueContext)

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 's3_input_bucket', 's3_input_key', 's3_output_bucket', 's3_output_prefix']
)
job.init(args['JOB_NAME'], args)

input_path = f"s3://{args['s3_input_bucket']}/{args['s3_input_key']}"
LOGGER.info(f"Starting processing for file: {input_path}")

try:
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = args['s3_input_key'].split('/')[-1].replace('.csv', '')
    LOGGER.info(f'File name: {file_name}')
    
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [input_path],
        },
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx='datasource'
    )
    
    raw_df = datasource.toDF()
    
    LOGGER.info("Original schema:")
    raw_df.printSchema()
    LOGGER.info("Sample raw records:")
    raw_df.show(5, truncate=False)
    input_count = raw_df.count()
    LOGGER.info(f"Input record count: {input_count}")
    
    # -------------------- DATA CLEANING PHASE --------------------
    LOGGER.info("Starting data cleaning phase...")
    
    # 1. Clean up name field - trim whitespace and convert to proper case
    cleaned_df = raw_df.withColumn("name", F.initcap(F.trim(F.col("name"))))
    
    # 2. Validate and standardize email addresses
    cleaned_df = cleaned_df.withColumn("email_valid", 
                                      F.when(F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6}$"), True)
                                       .otherwise(False))
    
    # 3. Convert date strings to proper date type
    cleaned_df = cleaned_df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    
    # 4. Clean and convert amount to proper decimal/double
    cleaned_df = cleaned_df.withColumn("amount", 
                                      F.regexp_replace(F.col("amount"), "[^0-9.]", ""))  # Remove non-numeric chars except decimal
    cleaned_df = cleaned_df.withColumn("amount", F.col("amount").cast(DoubleType()))
    
    # 5. Standardize status values to lowercase
    cleaned_df = cleaned_df.withColumn("status", F.lower(F.trim(F.col("status"))))
    
    LOGGER.info("Sample data after cleaning:")
    cleaned_df.show(5, truncate=False)
    clean_count = cleaned_df.count()
    LOGGER.info(f"Records after cleaning: {clean_count}")
    if input_count != clean_count:
        LOGGER.info(f"Removed {input_count - clean_count} duplicate or invalid records during cleaning")
    
    # -------------------- TRANSFORMATION PHASE --------------------
    LOGGER.info("Starting data transformation phase...")
    
    # 1. Add processing metadata
    transformed_df = cleaned_df.withColumn("processing_date", F.current_timestamp()) \
                              .withColumn("source_file", F.lit(args['s3_input_key']))
    
    
    # 2. Calculate a running total of amount by date and status
    window_spec = Window.partitionBy("status").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    transformed_df = transformed_df.withColumn("running_total_by_status", 
                                              F.sum("amount").over(window_spec))
    
    # 3. Categorize amounts
    transformed_df = transformed_df.withColumn("amount_category", 
                                              F.when(F.col("amount") < 100, "low")
                                               .when((F.col("amount") >= 100) & (F.col("amount") < 200), "medium")
                                               .otherwise("high"))
    
    # 4. Flag email issues
    transformed_df = transformed_df.withColumn("data_quality_issues", 
                                              F.when(F.col("email_valid") == False, "Invalid email")
                                               .otherwise(None))
    
    LOGGER.info("Final transformed schema:")
    transformed_df.printSchema()
    LOGGER.info("Sample data after transformation:")
    transformed_df.show(5, truncate=False)
    output_count = transformed_df.count()
    LOGGER.info(f"Output record count: {output_count}")
    
    # -------------------- OUTPUT PHASE --------------------
    
    final_df = transformed_df.select(
        "id", "name", "email", "email_valid", "date", 
        "amount", "amount_category", "status", "running_total_by_status",
        "data_quality_issues", "processing_date", "source_file"
    )
    
    transformed_output_path = f"s3://{args['s3_output_bucket']}/{args['s3_output_prefix']}/{file_name}_{current_time}"
    final_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(transformed_output_path)
    LOGGER.info(f"Successfully written transformed data to: {transformed_output_path}")
    
    stats_df = spark.createDataFrame([
        (input_path, input_count, clean_count, output_count, 
         datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    ], ["source_file", "input_count", "cleaned_count", "output_count", "processing_timestamp"])
    
    stats_output_path = f"s3://{args['s3_output_bucket']}/{args['s3_output_prefix']}/stats/{file_name}_{current_time}"
    stats_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .json(stats_output_path)
    LOGGER.info(f"Successfully written processing statistics to: {stats_output_path}")
    
    job.commit()
    LOGGER.info("Glue job completed successfully.")
    
except Exception as e:
    LOGGER.error(f"Error occurred during Glue job: {str(e)}")
    import traceback
    LOGGER.error(traceback.format_exc())
    raise e