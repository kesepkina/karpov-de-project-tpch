import logging
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = f"s3a://de-raw"
TARGET_PATH = f"s3a://de-project/ksenija-esepkina-bpk6977"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJobOrders-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())


def generate_report(orders_df, customer_df, nation_df):
    orders_report = (orders_df
                     .withColumn("O_MONTH", F.date_format(orders_df["O_ORDERDATE"], 'yyyy-MM'))
                     .join(customer_df, F.col("O_CUSTKEY") == customer_df["C_CUSTKEY"], how="left")
                     .join(nation_df, F.col("C_NATIONKEY") == nation_df["N_NATIONKEY"], how="left")
                     .groupBy([F.col("O_MONTH"), F.col("N_NAME"), F.col("O_ORDERPRIORITY")])
                     .agg(F.count(F.col("O_ORDERKEY")).alias("orders_count"),
                          F.avg(F.col("O_TOTALPRICE")).alias("avg_order_price"),
                          F.sum(F.col("O_TOTALPRICE")).alias("sum_order_price"),
                          F.min(F.col("O_TOTALPRICE")).alias("min_order_price"),
                          F.max(F.col("O_TOTALPRICE")).alias("max_order_price"),
                          F.sum(F.when(F.col("O_ORDERSTATUS") == "F", 1).otherwise(0)).alias("f_order_status"),
                          F.sum(F.when(F.col("O_ORDERSTATUS") == "O", 1).otherwise(0)).alias("o_order_status"),
                          F.sum(F.when(F.col("O_ORDERSTATUS") == "P", 1).otherwise(0)).alias("p_order_status"))
                     .select(F.col("O_MONTH"),
                             F.col("N_NAME"),
                             F.col("O_ORDERPRIORITY"),
                             F.col("orders_count"),
                             F.col("avg_order_price"),
                             F.col("sum_order_price"),
                             F.col("min_order_price"),
                             F.col("max_order_price"),
                             F.col("f_order_status"),
                             F.col("o_order_status"),
                             F.col("p_order_status"))
                     .orderBy([F.col("N_NAME"), F.col("O_ORDERPRIORITY")])
                     )
    return orders_report


def main():
    spark = _spark_session()
    orders_df = spark.read.parquet(f"{DATA_PATH}/orders", header=True, inferSchema=True)
    customer_df = spark.read.parquet(f"{DATA_PATH}/customer", header=True, inferSchema=True)
    nation_df = spark.read.parquet(f"{DATA_PATH}/nation", header=True, inferSchema=True)
    orders_report = generate_report(orders_df, customer_df, nation_df)
    orders_report.show()
    logging.info(f"Report size = {orders_report.count()}")
    orders_report.write.mode("overwrite").parquet(f"{TARGET_PATH}/orders_report")
    # ОБЯЗАТЕЛЬНО ДОБАВИТЬ ИНАЧЕ ПОД ОСТАНЕТСЯ ВИСЕТЬ
    spark.stop()


if __name__ == "__main__":
    main()