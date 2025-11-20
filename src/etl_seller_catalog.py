import sys
import yaml
import os  # Import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, initcap, lower, when, lit, sha2, expr
)
from pyspark.sql.types import DoubleType, IntegerType

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
    spark = get_spark_session(app_name="ETL_Seller_Catalog")
    paths = config['seller_catalog']

    # 3. Build absolute paths from local
    INPUT_PATH = f"{base_path}/{paths['input_path']}"
    HUDI_OUTPUT_PATH = f"{base_path}/{paths['hudi_output_path']}"
    QUARANTINE_PATH = f"{base_path}/{paths['quarantine_path']}"

    print(f"Starting ETL for Seller Catalog...")
    print(f"Input Path: {INPUT_PATH}")

    # 4. Read Raw Data
    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(INPUT_PATH)

    # 5. Data Cleaning  
    cleaned_df = raw_df.withColumn("hudi_precombine_key", sha2(raw_df.columns[0], 256))
    string_cols = ["seller_id", "item_id", "item_name", "category"]
    for c in string_cols:
        cleaned_df = cleaned_df.withColumn(c, trim(col(c)))
    cleaned_df = (
        cleaned_df
        .withColumn("item_name", initcap(col("item_name")))
        .withColumn("category", initcap(col("category")))
    )
    cleaned_df = (
        cleaned_df
        .withColumn("marketplace_price", expr("try_cast(marketplace_price as double)"))
        .withColumn("stock_qty", expr("try_cast(stock_qty as int)"))
    )
    cleaned_df = cleaned_df.fillna(0, subset=["stock_qty"])
    cleaned_df = cleaned_df.dropDuplicates(["seller_id", "item_id"])

    # 6. Data Quality (DQ) Checks
    dq_df = cleaned_df.withColumn(
        "dq_failure_reason",
        when(col("seller_id").isNull(), "seller_id IS NULL")
        .when(col("item_id").isNull(), "item_id IS NULL")
        .when(col("marketplace_price").isNull(), "marketplace_price is malformed or null")
        .when(col("stock_qty").isNull(), "stock_qty is malformed or null")
        .when(col("marketplace_price") < 0, "Price valid < 0")
        .when(col("stock_qty") < 0, "Stock valid < 0")
        .when(col("item_name").isNull(), "Item name present IS NULL")
        .when(col("category").isNull(), "Category present IS NULL")
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
            table_name="seller_catalog",
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