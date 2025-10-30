import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = f"s3a://de-raw"
TARGET_PATH = f"s3a://de-project/ksenija-esepkina-bpk6977"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJobCustomers-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())


def generate_report(customer_df, nation_df, region_df):
    return (customer_df
            .join(nation_df, customer_df["C_NATIONKEY"] == nation_df["N_NATIONKEY"])
            .join(region_df, nation_df["N_REGIONKEY"] == region_df["R_REGIONKEY"])
            .groupBy([F.col("R_NAME"), F.col("N_NAME"), F.col("C_MKTSEGMENT")])
            .agg(F.count(F.col("C_CUSTKEY")).alias("unique_customers_count"),
                 F.avg(F.col("C_ACCTBAL")).alias("avg_acctbal"),
                 F.mean(F.col("C_ACCTBAL")).alias("mean_acctbal"),
                 F.min(F.col("C_ACCTBAL")).alias("min_acctbal"),
                 F.max(F.col("C_ACCTBAL")).alias("max_acctbal"),
                 )
            .orderBy([F.col("N_NAME"), F.col("C_MKTSEGMENT")])
            .select(F.col("R_NAME"),
                    F.col("N_NAME"),
                    F.col("C_MKTSEGMENT"),
                    F.col("unique_customers_count"),
                    F.col("avg_acctbal"),
                    F.col("mean_acctbal"),
                    F.col("min_acctbal"),
                    F.col("max_acctbal"),)
            )


def main():
    spark = _spark_session()
    customer_df = spark.read.parquet(f"{DATA_PATH}/customer", header=True, inferSchema=True)
    nation_df = spark.read.parquet(f"{DATA_PATH}/nation", header=True, inferSchema=True)
    region_df = spark.read.parquet(f"{DATA_PATH}/region", header=True, inferSchema=True)
    customers_report = generate_report(customer_df, nation_df, region_df)
    customers_report.show()
    customers_report.write.mode("overwrite").parquet(f"{TARGET_PATH}/customers_report")
    # ОБЯЗАТЕЛЬНО ДОБАВИТЬ ИНАЧЕ ПОД ОСТАНЕТСЯ ВИСЕТЬ
    spark.stop()


if __name__ == "__main__":
    main()