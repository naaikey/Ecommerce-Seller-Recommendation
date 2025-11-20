import sys
import yaml
import os 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum, count, avg, round, broadcast,
    when, lit, row_number, format_number
)
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def get_spark_session(app_name: str) -> SparkSession:
    # Initializes and returns a configured SparkSession.
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    # Set log level to WARN to suppress excessive INFO output
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_config(config_path: str) -> dict:
    # Loads configuration data from a YAML file.
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <config_file_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    config = get_config(config_path)

    # Determine the absolute project root path for local file paths
    project_root = os.getcwd()
    base_path = "file:///" + project_root.replace("\\", "/")

    # 1. Initialize Spark and Config
    spark = get_spark_session(app_name="Consumption_Recommendation")
    paths = config['recommendation']

    # Build absolute paths for Hudi tables (inputs) and CSV (output)
    SELLER_CATALOG_PATH = f"{base_path}/{paths['seller_catalog_hudi']}"
    COMPANY_SALES_PATH = f"{base_path}/{paths['company_sales_hudi']}"
    COMPETITOR_SALES_PATH = f"{base_path}/{paths['competitor_sales_hudi']}"
    OUTPUT_CSV_PATH = f"{base_path}/{paths['output_csv']}"

    print("Starting Consumption Layer...")

    # 2. Read Cleaned Hudi Tables
    seller_catalog_df = spark.read.format("hudi").load(SELLER_CATALOG_PATH)
    company_sales_df = spark.read.format("hudi").load(COMPANY_SALES_PATH)
    competitor_sales_df = spark.read.format("hudi").load(COMPETITOR_SALES_PATH)

    # --- Data Transformations ---

    # 3. Create Item-Category and Item-Name Mapping from seller catalog
    item_catalog_details = (
        seller_catalog_df
        .select("item_id", "category", "item_name")
        .dropDuplicates(["item_id"])
    )

    # 4. Aggregate Company Sales to find Top Sellers
    company_top_sellers = (
        company_sales_df.join(item_catalog_details.select("item_id", "category"), "item_id", "left")
        .groupBy("item_id", "category")
        .agg(sum("units_sold").alias("total_units_sold"))
    )

    # 5. Aggregate Competitor Sales to find Top Sellers
    competitor_top_sellers = (
        competitor_sales_df.join(item_catalog_details.select("item_id", "category"), "item_id", "left")
        .groupBy("item_id", "category")
        .agg(
            sum("units_sold").alias("total_units_sold"),
            avg("marketplace_price").alias("market_price"),
            count("seller_id").alias("num_sellers")
        )
    )

    # 6. Combine and Calculate All Market Top Items
    
    # Combine company and competitor sales data, aggregating total units sold
    all_top_items = (
        company_top_sellers.select("item_id", "category", "total_units_sold")
        .unionByName(competitor_top_sellers.select("item_id", "category", "total_units_sold"), allowMissingColumns=True)
        .groupBy("item_id", "category")
        .agg(sum("total_units_sold").alias("total_units_sold"))
        
        # Join back with competitor details and item name from original catalog details
        .join(broadcast(competitor_top_sellers.select("item_id", "market_price", "num_sellers")), "item_id", "left")
        .join(broadcast(item_catalog_details.select("item_id", "item_name")), "item_id", "left")
        .orderBy(col("total_units_sold").desc())
        
        # Filter out items where item_name and category is missing
        .filter(col("item_name").isNotNull() & col("category").isNotNull())
    )

    # 7. Identify Missing Items per Seller

    all_sellers = seller_catalog_df.select("seller_id").distinct()
    
    # Generate all possible seller-top_item pairs (Cross Join)
    all_seller_item_pairs = all_sellers.crossJoin(broadcast(all_top_items))

    # Identify items currently NOT sold by the seller (Left Anti Join)
    missing_items_df = all_seller_item_pairs.join(
        seller_catalog_df,
        (all_seller_item_pairs.seller_id == seller_catalog_df.seller_id) &
        (all_seller_item_pairs.item_id == seller_catalog_df.item_id),
        "left_anti"
    )

    # --- Recommendation Calculation ---

    # Calculate expected_units_sold and expected_revenue 
    recommendations_df = missing_items_df.withColumn(
        "expected_units_sold_float",
        when(col("num_sellers") > 0, round(col("total_units_sold") / col("num_sellers")))
        .otherwise(0)
    )
    recommendations_df = recommendations_df.withColumn(
        "expected_revenue_float", 
        round(col("expected_units_sold_float") * col("market_price"), 2)
    )

    # 8. Final Output Filtering
    
    # Logic : Partitioned ONLY by seller_id 
    seller_window = (
        Window
        .partitionBy("seller_id")
        .orderBy(col("expected_revenue_float").desc())
    )
    
    # Apply the ranking
    ranked_recs = recommendations_df.withColumn("rank", row_number().over(seller_window))

    # Select the top 10 ranked recommendations per seller
    final_output_df = (
        ranked_recs
        .filter(col("rank") <= 10)
        .drop("rank", "total_units_sold", "num_sellers") 
        # Final conversion to string decimal format and column order
        .select(
            "seller_id",
            "item_id",
            col("item_name"), 
            col("category"),
            format_number(col("market_price"), 2).alias("market_price"), 
            col("expected_units_sold_float").cast("integer").alias("expected_units_sold"),
            format_number(col("expected_revenue_float"), 2).alias("expected_revenue") 
        )
    )

    # 9. Output and Display
    
    final_count = final_output_df.count() 
    print(f"Successfully calculated and filtered for top {final_count} recommendations (Top 10 per seller).")

    # Display the final output table directly in the terminal
    print("\n--- Final Recommendations Output Table ---")
    final_output_df.show(n=final_count, truncate=False)
    print("------------------------------------------")

    # Write the output to CSV file
    (
        final_output_df.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(OUTPUT_CSV_PATH)
    )
    print(f"Successfully wrote recommendations to {OUTPUT_CSV_PATH}")

    spark.stop()

if __name__ == "__main__":
    main()