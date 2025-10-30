"""
Проектный даг пользователя ksenija-esepkina-bpk6977, создающий отчёты на базе данных TPC-H
"""
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

ROOT = Path(__file__).parent

REPORTS = {
    key: open(ROOT / "sql" / f"{key}_query.sql").read()
    for key in ["lineitems", "orders", "customers", "suppliers", "parts"]
}


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )


with DAG(
    dag_id="de-project-ksenija-esepkina-bpk6977-dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    max_active_runs=1,
    tags=["ksenija-esepkina-bpk6977", "project"],
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)


    for report_name, sql_query in REPORTS.items():

        submit_task = _build_submit_operator(
            task_id=f'submit_{report_name}',
            application_file=f'spark_submit_{report_name}.yaml',
            link_dag=dag
        )

        sensor_task = _build_sensor(
            task_id=f'sensor_{report_name}',
            application_name=f"{{{{task_instance.xcom_pull(task_ids='submit_{report_name}')['metadata']['name']}}}}",
            link_dag=dag
        )

        datamart_task = SQLExecuteQueryOperator(
            task_id=f'{report_name}_datamart',
            sql=sql_query,
            conn_id=GREENPLUM_ID,
            dag=dag
        )

        start >> submit_task >> sensor_task >> datamart_task >> end
