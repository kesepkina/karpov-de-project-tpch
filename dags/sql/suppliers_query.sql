DROP EXTERNAL TABLE IF EXISTS "ksenija-esepkina-bpk6977".suppliers;
CREATE EXTERNAL TABLE "ksenija-esepkina-bpk6977".suppliers(
    R_NAME TEXT,
    N_NAME TEXT,
    unique_supplers_count BIGINT,
    avg_acctbal FLOAT8,
    mean_acctbal FLOAT8,
    min_acctbal FLOAT8,
    max_acctbal FLOAT8
)
LOCATION ('pxf://de-project/ksenija-esepkina-bpk6977/suppliers_report?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';