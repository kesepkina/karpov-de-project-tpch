import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = f"s3a://de-raw"
TARGET_PATH = f"s3a://de-project/ksenija-esepkina-bpk6977"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJobParts-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())


def generate_report(part_df, partsupp_df, supplier_df, nation_df):
    return (part_df
            .join(partsupp_df, F.col("P_PARTKEY") == F.col("PS_PARTKEY"))
            .join(supplier_df, F.col("PS_SUPPKEY") == F.col("S_SUPPKEY"))
            .join(nation_df, F.col("S_NATIONKEY") == F.col("N_NATIONKEY"))
            .groupBy([F.col("N_NAME"), F.col("P_TYPE"), F.col("P_CONTAINER")])
            .agg(F.count(F.col("P_PARTKEY")).alias("parts_count"),
                 F.avg(F.col("P_RETAILPRICE")).alias("avg_retailprice"),
                 F.sum(F.col("P_SIZE")).alias("size"),
                 F.mean(F.col("P_RETAILPRICE")).alias("mean_retailprice"),
                 F.min(F.col("P_RETAILPRICE")).alias("min_retailprice"),
                 F.max(F.col("P_RETAILPRICE")).alias("max_retailprice"),
                 F.avg(F.col("PS_SUPPLYCOST")).alias("avg_supplycost"),
                 F.mean(F.col("PS_SUPPLYCOST")).alias("mean_supplycost"),
                 F.min(F.col("PS_SUPPLYCOST")).alias("min_supplycost"),
                 F.max(F.col("PS_SUPPLYCOST")).alias("max_supplycost"),
                 )
            .orderBy([F.col("N_NAME"), F.col("P_TYPE"), F.col("P_CONTAINER")])
            .select(F.col("N_NAME"),
                    F.col("P_TYPE"),
                    F.col("P_CONTAINER"),
                    F.col("parts_count"),
                    F.col("avg_retailprice"),
                    F.col("size"),
                    F.col("mean_retailprice"),
                    F.col("min_retailprice"),
                    F.col("max_retailprice"),
                    F.col("avg_supplycost"),
                    F.col("mean_supplycost"),
                    F.col("min_supplycost"),
                    F.col("max_supplycost"),)
            )


def main():
    spark = _spark_session()
    part_df = spark.read.parquet(f"{DATA_PATH}/part", header=True, inferSchema=True)
    partsupp_df = spark.read.parquet(f"{DATA_PATH}/partsupp", header=True, inferSchema=True)
    supplier_df = spark.read.parquet(f"{DATA_PATH}/supplier", header=True, inferSchema=True)
    nation_df = spark.read.parquet(f"{DATA_PATH}/nation", header=True, inferSchema=True)
    parts_report = generate_report(part_df, partsupp_df, supplier_df, nation_df)
    parts_report.write.mode("overwrite").parquet(f"{TARGET_PATH}/parts_report")
    # ОБЯЗАТЕЛЬНО ДОБАВИТЬ ИНАЧЕ ПОД ОСТАНЕТСЯ ВИСЕТЬ
    spark.stop()


if __name__ == "__main__":
    main()