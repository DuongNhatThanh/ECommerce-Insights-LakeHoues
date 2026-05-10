from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10)
}

dag = DAG(
    'Import_Marketing_data_to_Bronze_State_using_Spark_JDBC',
    default_args=default_args,
    description='Incremental Spark JDBC import for marketing tables into HDFS',
    schedule_interval='@daily',
    catchup=False
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)
ssh_hook_hdfs = SSHHook(ssh_conn_id='hdfs_server', cmd_timeout=None)

start_dag = DummyOperator(
    task_id='sstart_dag',
    dag=dag,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)


def create_incremental_spark_jdbc_task(table_name, target_dir):
    return SSHOperator(
        task_id=f'spark_jdbc_import_{table_name}',
        ssh_hook=ssh_hook_spark,
        command=f"""
          spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.cores.max=2 \
            --conf spark.executor.cores=2 \
            --jars /opt/spark/jars/postgresql.jar \
            /opt/spark-apps/jdbc/postgres_to_bronze.py \
            --schema marketing \
            --table {table_name} \
            --target-path {target_dir} \
            --incremental-column id
        """,
        dag=dag
    )


import_closed_deals = create_incremental_spark_jdbc_task(
    'closed_deals',
    '/data/bronze/marketing/closed_deals',
)

import_marketing_qualified_leads = create_incremental_spark_jdbc_task(
    'marketing_qualified_leads',
    '/data/bronze/marketing/marketing_qualified_leads',
)

check_create_hdfs_directory = SSHOperator(
    task_id='check_create_hdfs_directory',
    ssh_hook=ssh_hook_hdfs,
    command="""
    /usr/local/hadoop/bin/hdfs dfs -test -d /data/bronze/marketing || /usr/local/hadoop/bin/hdfs dfs -mkdir -p /data/bronze/marketing
    /usr/local/hadoop/bin/hdfs dfs -chmod -R 755 /data/bronze/marketing
    """,
    dag=dag
)

start_dag >> check_create_hdfs_directory
check_create_hdfs_directory >> [
    import_marketing_qualified_leads, import_closed_deals]

import_marketing_qualified_leads >> import_closed_deals >> end_dag
