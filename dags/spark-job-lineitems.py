import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = f"s3a://de-raw"
TARGET_PATH = f"s3a://de-project/ksenija-esepkina-bpk6977"


def _spark_session():
    return (SparkSession.builder
            .appName("SparkJobLineItems-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())


def generate_report(lineitem_df):
    return (lineitem_df
            .groupBy(F.col("L_ORDERKEY"))
            .agg(F.count(F.col("L_PARTKEY")).alias("count"),
                 F.sum(F.col("L_EXTENDEDPRICE")).alias("sum_extendprice"),
                 F.mean(F.col("L_DISCOUNT")).alias("mean_discount"),  # median_discount
                 F.mean(F.col("L_TAX")).alias("mean_tax"),
                 F.mean(F.datediff(F.col("L_RECEIPTDATE"), F.col("L_SHIPDATE"))).alias("delivery_days"),
                 F.sum(F.when(F.col("L_RETURNFLAG") == 'A', 1).otherwise(0)).alias("A_return_flags"),
                 F.sum(F.when(F.col("L_RETURNFLAG") == 'R', 1).otherwise(0)).alias("R_return_flags"),
                 F.sum(F.when(F.col("L_RETURNFLAG") == 'N', 1).otherwise(0)).alias("N_return_flags"))
            .select(F.col("L_ORDERKEY"),
                    F.col("count"),
                    F.col("sum_extendprice"),
                    F.col("mean_discount"),
                    F.col("mean_tax"),
                    F.col("delivery_days"),
                    F.col("A_return_flags"),
                    F.col("R_return_flags"),
                    F.col("N_return_flags"))
            .orderBy(F.col("L_ORDERKEY"))
            )


def main():
    spark = _spark_session()
    lineitem_df = spark.read.parquet(f"{DATA_PATH}/lineitem", header=True, inferSchema=True)
    lineitems_report = generate_report(lineitem_df)
    lineitems_report.show()
    lineitems_report.write.mode("overwrite").parquet(f"{TARGET_PATH}/lineitems_report")
    # ОБЯЗАТЕЛЬНО ДОБАВИТЬ ИНАЧЕ ПОД ОСТАНЕТСЯ ВИСЕТЬ
    spark.stop()


if __name__ == "__main__":
    main()
