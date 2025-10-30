DROP EXTERNAL TABLE IF EXISTS "ksenija-esepkina-bpk6977".customers;
CREATE EXTERNAL TABLE "ksenija-esepkina-bpk6977".customers(
    R_NAME TEXT,
    N_NAME TEXT,
    C_MKTSEGMENT TEXT,
    unique_customers_count BIGINT,
    avg_acctbal FLOAT8,
    mean_acctbal FLOAT8,
    min_acctbal FLOAT8,
    max_acctbal FLOAT8
)
LOCATION ('pxf://de-project/ksenija-esepkina-bpk6977/customers_report?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';