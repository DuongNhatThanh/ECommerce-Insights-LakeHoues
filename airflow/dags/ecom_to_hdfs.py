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
    'Import_Ecom_data_to_Bronze_State_using_Spark_JDBC',
    default_args=default_args,
    description='Incremental Spark JDBC import for e-commerce tables into HDFS',
    schedule_interval='@daily',
    catchup=False
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)
ssh_hook_hdfs = SSHHook(ssh_conn_id='hdfs_server', cmd_timeout=None)


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
            --schema ecommerce \
            --table {table_name} \
            --target-path {target_dir} \
            --incremental-column id
        """,
        dag=dag
    )


import_customers = create_incremental_spark_jdbc_task(
    'customers',
    '/data/bronze/ecom/customers',
)

import_geolocation = create_incremental_spark_jdbc_task(
    'geolocation',
    '/data/bronze/ecom/geolocation',
)

import_order_items = create_incremental_spark_jdbc_task(
    'order_items',
    '/data/bronze/ecom/order_items',
)

import_order_payments = create_incremental_spark_jdbc_task(
    'order_payments',
    '/data/bronze/ecom/order_payments',
)

import_order_reviews = create_incremental_spark_jdbc_task(
    'order_reviews',
    '/data/bronze/ecom/order_reviews',
)

import_orders = create_incremental_spark_jdbc_task(
    'orders',
    '/data/bronze/ecom/orders',
)

import_products = create_incremental_spark_jdbc_task(
    'products',
    '/data/bronze/ecom/products',
)

import_sellers = create_incremental_spark_jdbc_task(
    'sellers',
    '/data/bronze/ecom/sellers',
)

import_product_category_name_translations = create_incremental_spark_jdbc_task(
    'product_category_name_translations',
    '/data/bronze/ecom/product_category_name_translations',
)

check_create_hdfs_directory = SSHOperator(
    task_id='check_create_hdfs_directory',
    ssh_hook=ssh_hook_hdfs,
    command="""
    /usr/local/hadoop/bin/hdfs dfs -test -d /data/bronze/ecom || /usr/local/hadoop/bin/hdfs dfs -mkdir -p /data/bronze/ecom
    /usr/local/hadoop/bin/hdfs dfs -chmod -R 755 /data/bronze/ecom
    """,
    dag=dag
)

start_dag = DummyOperator(
    task_id='sstart_dag',
    dag=dag,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)

start_dag >> check_create_hdfs_directory >> [
    import_geolocation, import_sellers, import_customers, import_product_category_name_translations, import_products, import_orders, import_order_items, import_order_payments, import_order_reviews]
import_geolocation >> import_sellers >> import_customers
import_customers >> [
    import_product_category_name_translations, import_products]
import_products >> import_orders >> [
    import_order_items, import_order_payments, import_order_reviews]

import_order_items >> import_order_payments >> import_order_reviews >> end_dag
