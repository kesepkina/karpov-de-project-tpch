DROP EXTERNAL TABLE IF EXISTS "ksenija-esepkina-bpk6977".parts;
CREATE EXTERNAL TABLE "ksenija-esepkina-bpk6977".parts(
    N_NAME TEXT,
    P_TYPE TEXT,
    P_CONTAINER TEXT,
    parts_count BIGINT,
    avg_retailprice FLOAT8,
    size BIGINT,
    mean_retailprice FLOAT8,
    min_retailprice FLOAT8,
    max_retailprice FLOAT8,
    avg_supplycost FLOAT8,
    mean_supplycost FLOAT8,
    min_supplycost FLOAT8,
    max_supplycost FLOAT8
)
LOCATION ('pxf://de-project/ksenija-esepkina-bpk6977/parts_report?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';