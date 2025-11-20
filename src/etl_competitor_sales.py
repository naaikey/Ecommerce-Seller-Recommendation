import sys
import yaml
import os  # Import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, when, lit, current_date, to_date, expr
)

def get_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    return spark

def get_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def write_hudi_table(df: DataFrame, table_name: str, path: str, primary_key: str, precombine_key: str):
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': primary_key,
        'hoodie.datasource.write.precombine.field': precombine_key,
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE'
    }
    (
        df.write
        .format("hudi")
        .options(**hudi_options)
        .mode("overwrite")
        .save(path)
    )
    print(f"Successfully wrote Hudi table to {path}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <config_file_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    config = get_config(config_path)

    # 1. Local File Path 
    project_root = os.getcwd()
    base_path = "file:///" + project_root.replace("\\", "/")

    # 2. Initialize Spark and Config
    spark = get_spark_session(app_name="ETL_Competitor_Sales")
    paths = config['competitor_sales']

    # 3. Build absolute paths from local
    INPUT_PATH = f"{base_path}/{paths['input_path']}"
    HUDI_OUTPUT_PATH = f"{base_path}/{paths['hudi_output_path']}"
    QUARANTINE_PATH = f"{base_path}/{paths['quarantine_path']}"

    print(f"Starting ETL for Competitor Sales...")
    print(f"Input Path: {INPUT_PATH}")

    # 4. Read Raw Data
    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(INPUT_PATH)

    # 5. Data Cleaning 
    cleaned_df = raw_df.withColumn("hudi_precombine_key",
                                   when(col("revenue").isNull(), 0).otherwise(col("revenue")))
    string_cols = ["item_id", "seller_id"]
    for c in string_cols:
        cleaned_df = cleaned_df.withColumn(c, trim(col(c)))
    cleaned_df = (
        cleaned_df
        .withColumn("units_sold", expr("try_cast(units_sold as int)"))
        .withColumn("revenue", expr("try_cast(revenue as double)"))
        .withColumn("marketplace_price", expr("try_cast(marketplace_price as double)"))
        .withColumn("sale_date", to_date(col("sale_date")))
    )
    cleaned_df = cleaned_df.fillna(0, subset=["units_sold", "revenue", "marketplace_price"])
    cleaned_df = cleaned_df.dropDuplicates(["seller_id", "item_id"])

    # 6. Data Quality (DQ) Checks
    dq_df = cleaned_df.withColumn(
        "dq_failure_reason",
        when(col("item_id").isNull(), "item_id IS NULL")
        .when(col("seller_id").isNull(), "seller_id IS NULL")
        .when(col("units_sold").isNull(), "units_sold is malformed or null")
        .when(col("revenue").isNull(), "revenue is malformed or null")
        .when(col("marketplace_price").isNull(), "marketplace_price is malformed or null")
        .when(col("units_sold") < 0, "units_sold < 0")
        .when(col("revenue") < 0, "revenue < 0")
        .when(col("marketplace_price") < 0, "marketplace_price < 0")
        .when(col("sale_date").isNull(), "sale_date IS NULL or malformed")
        .when(col("sale_date") > current_date(), "sale_date is in the future")
        .otherwise(lit(None))
    )

    # 7. Split Good and Bad Data 
    good_df = dq_df.filter(col("dq_failure_reason").isNull()).drop("dq_failure_reason")
    quarantine_df = dq_df.filter(col("dq_failure_reason").isNotNull())
    good_count = good_df.count()
    bad_count = quarantine_df.count()
    print(f"Processing complete. Found {good_count} good records and {bad_count} bad records.")

    # 8. Write Data to Hudi
    if good_count > 0:
        write_hudi_table(
            df=good_df,
            table_name="competitor_sales",
            path=HUDI_OUTPUT_PATH,
            primary_key="seller_id,item_id",
            precombine_key="hudi_precombine_key"
        )
    if bad_count > 0:
        (
            quarantine_df.write
            .mode("overwrite")
            .parquet(QUARANTINE_PATH)
        )
    spark.stop()

if __name__ == "__main__":
    main()