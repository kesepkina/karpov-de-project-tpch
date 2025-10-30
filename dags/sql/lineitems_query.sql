DROP EXTERNAL TABLE IF EXISTS "ksenija-esepkina-bpk6977".lineitems;
CREATE EXTERNAL TABLE "ksenija-esepkina-bpk6977".lineitems(
    L_ORDERKEY BIGINT,
    count BIGINT,
    sum_extendprice FLOAT8,
    mean_discount FLOAT8,
    mean_tax FLOAT8,
    delivery_days FLOAT8,
    A_return_flags BIGINT,
    R_return_flags BIGINT,
    N_return_flags BIGINT
)
LOCATION ('pxf://de-project/ksenija-esepkina-bpk6977/lineitems_report?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';