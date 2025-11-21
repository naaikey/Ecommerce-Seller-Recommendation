**Ecommerce Seller Recommendation**

**Project Overview**:
- **Purpose**: This project extracts, transforms, and prepares seller recommendation data for downstream consumption. It ingests company sales, competitor sales, and seller catalog data, runs ETL pipelines (Spark-based), and writes cleaned datasets and recommendation outputs to `output/`.
- **Intended Users**: Data engineers, data scientists, and reviewers running the ETL pipeline locally or on a cluster.

**Repository Structure**:
- `configs/` : YAML configuration files used by the pipelines (e.g., `ecomm_prod.yml`).
- `data/` : Source (raw) CSVs used by the ETL jobs.
- `scripts/` : Shell scripts and submit wrappers for `spark-submit` jobs.
- `src/` : Python Spark ETL scripts and recommendation logic (e.g., `etl_company_sales.py`, `consumption_recommendation.py`).
- `output/` : Pipeline outputs (Hudi tables, gold dataset, quarantine folder for bad data).

**Prerequisites**:
- `Python 3.12` installed and on `PATH` (for any Python helpers).
- `Java JDK 17.0.17` installed (required by Spark).
- `Apache Spark 3.5.7` installed with `spark-submit` available on `PATH`.
- `Hadoop 3.3.6` installed (if using HDFS/YARN).
- Optional: `virtualenv`/`venv` to isolate Python deps.

**Running Individual Jobs**
You can run the pipeline stages individually either with the provided wrapper scripts in `scripts/` or directly with `spark-submit`.

**Packaged spark-submit commands for Windows**

```bash

# ...etl_seller_catalog.py...
spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.driver.memory=4g src/etl_seller_catalog.py configs/ecomm_prod.yml

# ...etl_company_sales.py...
spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.driver.memory=4g src/etl_company_sales.py configs/ecomm_prod.yml

# ...etl_competitor_sales.py...
spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.driver.memory=4g src/etl_competitor_sales.py configs/ecomm_prod.yml

# ...consumption_recommendation.py...
spark-submit --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.driver.memory=4g src/consumption_recommendation.py configs/ecomm_prod.yml
```

**Linux / Bash (use the wrappers in `scripts/`)**

```bash

# Run seller catalog ETL via wrapper
bash scripts/etl_seller_catalog_spark_submit.sh

# Run company sales ETL via wrapper
bash scripts/etl_company_sales_spark_submit.sh

# Run competition sales ETL via wrapper
bash scripts/etl_competition_sales_spark_submit.sh

# Run recommendation stage
bash scripts/consumption_recommendation_spark_submit.sh
```

Notes:
- If you run on a Spark cluster, replace `--master local[*]` with your cluster master (e.g., `yarn` or `spark://...`) and adjust memory/executor settings.
- Many scripts accept a `--config` argument pointing to `configs/ecomm_prod.yml`; edit that file for environment-specific paths.

**Configuration**:
- Edit `configs/ecomm_prod.yml` to change input/output paths and any pipeline-specific parameters.

**Data Layout**:
- Input raw CSVs: `data/` (e.g., `company_sales_dirty.csv`, `competitor_sales_dirty.csv`, `seller_catalog_dirty.csv`).
- Outputs: `output/` (Hudi tables, gold datasets, `seller_recommend_data/`), and `quarantine/` folders for files that failed validation.

**Development & Testing**:
- Modify ETL logic in `src/` and validate by running the relevant `spark-submit` example against a small sample in `data/`.
- Add unit tests for transformation functions where possible (pure functions can be run without Spark).

**Contact / Author**:
- Created for the `ecommerce_seller_recommendation` assignment for course name: Data Stores and Pipelines, M.Sc. Data Science and AI @ BITS Pilani;
- Contact the repository owner for questions.

- Nikhilesh Kumar (nikhileshkumar97@gmail.com)

---
